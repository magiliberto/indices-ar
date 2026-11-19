"""Modo remoto de indices-ar: caché, límite de uso y registros.

Sólo se activa con `server.py --http`. Por stdio el servidor no importa este módulo y se
comporta como siempre.

- Caché por URL pedida (sin el token, que viaja en una cabecera): INDEC 1 h, BCRA 15 min,
  catálogo del INDEC y listado de variables del BCRA 6 h.
- Límites: global por minuto y por día, por IP por minuto, y pedidos al INDEC por minuto
  después de la caché. Al superarlos la herramienta devuelve un error legible y el servicio
  sigue respondiendo.
- Backend intercambiable: memoria (un solo proceso) o DynamoDB (compartido entre los
  contenedores de Lambda). Se elige con INDICES_AR_CACHE=memoria|dynamodb.
- Registros: una línea JSON por llamada a una herramienta, a stderr. Nunca guarda el texto de
  la consulta, la IP ni el token. La IP sólo vive, resumida con un hash, en la clave del
  contador del minuto.
"""
from __future__ import annotations

import asyncio
import contextvars
import hashlib
import hmac
import json
import os
import sys
import time
import zlib
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, TextContent

MENSAJE_LIMITE = "Límite de uso alcanzado; probá de nuevo en un minuto"

# Segundos de vida en la caché, por tipo de pedido.
TTL = {"indec": 3600, "catalogo": 6 * 3600, "bcra": 15 * 60, "bcra_variables": 6 * 3600}
# Tipos de pedido que salen hacia el INDEC (cuentan para su límite por minuto).
HACIA_INDEC = ("indec", "catalogo")
# DynamoDB acepta ítems de hasta 400 KB; lo que no entra comprimido no se guarda.
MAX_BYTES_ITEM = 350_000


class LimiteAlcanzado(ToolError):
    """Error previsto: llega al modelo como resultado con error y el servicio sigue respondiendo."""


_CLAVE_IP_LOCAL = os.urandom(32)


def _clave_ip() -> bytes:
    """Clave del resumen de IP. En Lambda es el token del INDEC (compartido entre contenedores, nunca se escribe);
    sin token, una clave aleatoria del proceso."""
    token = os.environ.get("INDEC_API_TOKEN", "")
    return hashlib.sha256(b"indices-ar|ip|" + token.encode()).digest() if token else _CLAVE_IP_LOCAL


@dataclass
class Limites:
    global_por_minuto: int = 60
    global_por_dia: int = 3000
    ip_por_minuto: int = 30
    indec_por_minuto: int = 20

    @classmethod
    def desde_entorno(cls) -> Limites:
        def entero(nombre: str, defecto: int) -> int:
            valor = os.environ.get(nombre, "").strip()
            return int(valor) if valor.isdigit() else defecto

        return cls(
            global_por_minuto=entero("INDICES_AR_LIMITE_MINUTO", 60),
            global_por_dia=entero("INDICES_AR_LIMITE_DIA", 3000),
            ip_por_minuto=entero("INDICES_AR_LIMITE_IP_MINUTO", 30),
            indec_por_minuto=entero("INDICES_AR_LIMITE_INDEC_MINUTO", 20),
        )


# --------------------------------------------------------------------------- backends


class Memoria:
    """Caché y contadores en memoria. Sirve para correr el modo remoto en una sola máquina."""

    def __init__(self, reloj: Callable[[], float] = time.time) -> None:
        self.reloj = reloj
        self._datos: dict[str, tuple[float, Any]] = {}

    def _vivo(self, clave: str) -> Any:
        guardado = self._datos.get(clave)
        if guardado and guardado[0] > self.reloj():
            return guardado[1]
        self._datos.pop(clave, None)
        return None

    async def leer(self, clave: str) -> Any:
        return self._vivo(clave)

    async def guardar(self, clave: str, valor: Any, ttl: int) -> None:
        self._datos[clave] = (self.reloj() + ttl, valor)
        if len(self._datos) > 5000:  # limpieza simple de lo vencido
            for k in [k for k, (vence, _) in self._datos.items() if vence <= self.reloj()]:
                del self._datos[k]

    async def sumar(self, clave: str, ttl: int) -> int:
        actual = self._vivo(clave) or 0
        vence = self._datos[clave][0] if clave in self._datos else self.reloj() + ttl
        self._datos[clave] = (vence, actual + 1)
        return actual + 1


class Dynamo:
    """Caché y contadores en una tabla DynamoDB con clave de partición `pk` (texto) y TTL en `ttl`.

    El TTL nativo de DynamoDB borra con demora (puede tardar días), así que las lecturas
    descartan por su cuenta lo vencido.
    """

    def __init__(self, tabla: str, cliente: Any = None, reloj: Callable[[], float] = time.time) -> None:
        if cliente is None:
            import boto3  # sólo hace falta en este backend

            cliente = boto3.client("dynamodb")
        self.tabla = tabla
        self.cliente = cliente
        self.reloj = reloj

    async def leer(self, clave: str) -> Any:
        r = await asyncio.to_thread(self.cliente.get_item, TableName=self.tabla, Key={"pk": {"S": clave}}, ConsistentRead=False)
        item = r.get("Item")
        if not item or int(item["ttl"]["N"]) <= self.reloj():
            return None
        return json.loads(zlib.decompress(item["v"]["B"]))

    async def guardar(self, clave: str, valor: Any, ttl: int) -> None:
        cuerpo = zlib.compress(json.dumps(valor, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), 6)
        if len(cuerpo) > MAX_BYTES_ITEM:
            return
        await asyncio.to_thread(
            self.cliente.put_item,
            TableName=self.tabla,
            Item={"pk": {"S": clave}, "v": {"B": cuerpo}, "ttl": {"N": str(int(self.reloj()) + ttl)}},
        )

    async def sumar(self, clave: str, ttl: int) -> int:
        r = await asyncio.to_thread(
            self.cliente.update_item,
            TableName=self.tabla,
            Key={"pk": {"S": clave}},
            UpdateExpression="ADD n :uno SET #t = if_not_exists(#t, :vence)",
            ExpressionAttributeNames={"#t": "ttl"},
            ExpressionAttributeValues={":uno": {"N": "1"}, ":vence": {"N": str(int(self.reloj()) + ttl)}},
            ReturnValues="UPDATED_NEW",
        )
        return int(r["Attributes"]["n"]["N"])


# --------------------------------------------------------------------------- registro por llamada


@dataclass
class Registro:
    desde_cache: int = 0
    pedidos_afuera: int = 0
    tipos: set[str] = field(default_factory=set)


_registro: contextvars.ContextVar[Registro | None] = contextvars.ContextVar("indices_ar_registro", default=None)


def escribir_registro(linea: dict[str, Any]) -> None:
    print(json.dumps(linea, ensure_ascii=False, separators=(",", ":")), file=sys.stderr, flush=True)


# --------------------------------------------------------------------------- capa


class Capa:
    def __init__(
        self,
        backend: Any,
        limites: Limites | None = None,
        reloj: Callable[[], float] = time.time,
        registrar: Callable[[dict[str, Any]], None] = escribir_registro,
    ) -> None:
        self.backend = backend
        self.limites = limites or Limites()
        self.reloj = reloj
        self.registrar = registrar
        self._cabeceras_vistas: set[tuple[str, ...]] = set()
        # Superado el límite global, este proceso rechaza sin tocar el backend hasta que cambie
        # el minuto (o el día): un aluvión de pedidos no se convierte en un aluvión de escrituras.
        self._bloqueado_hasta = 0.0

    # ------------------------------------------------------------------ caché

    async def traer(self, tipo: str, url: str, buscar: Callable[[], Awaitable[Any]]) -> Any:
        """Devuelve lo que diga la caché para `url`; si no está, llama a `buscar()` y lo guarda.

        `buscar()` tiene que devolver algo serializable a JSON. Los errores no se guardan.
        """
        registro = _registro.get()
        clave = "c#" + hashlib.sha256(url.encode("utf-8")).hexdigest()
        try:
            guardado = await self.backend.leer(clave)
        except Exception as e:  # una caché caída no tumba el servicio
            self._error_backend("leer", e)
            guardado = None
        if guardado is not None:
            if registro:
                registro.desde_cache += 1
                registro.tipos.add(tipo)
            return guardado
        if tipo in HACIA_INDEC:
            minuto = int(self.reloj() // 60)
            if await self._sumar(f"l#indec#{minuto}", 120) > self.limites.indec_por_minuto:
                raise LimiteAlcanzado(MENSAJE_LIMITE)
        valor = await buscar()
        if registro:
            registro.pedidos_afuera += 1
            registro.tipos.add(tipo)
        try:
            await self.backend.guardar(clave, valor, TTL.get(tipo, 900))
        except Exception as e:
            self._error_backend("guardar", e)
        return valor

    # ------------------------------------------------------------------ límites

    async def _sumar(self, clave: str, ttl: int) -> int:
        try:
            return await self.backend.sumar(clave, ttl)
        except Exception as e:  # sin contador, el techo lo pone la concurrencia reservada de la función
            self._error_backend("sumar", e)
            return 0

    async def chequear_llamada(self, ip: str | None) -> None:
        ahora = self.reloj()
        if ahora < self._bloqueado_hasta:
            raise LimiteAlcanzado(MENSAJE_LIMITE)
        minuto, dia = int(ahora // 60), time.strftime("%Y%m%d", time.gmtime(ahora))
        excedido = False
        if await self._sumar(f"l#global#m#{minuto}", 120) > self.limites.global_por_minuto:
            excedido = True
            self._bloqueado_hasta = (minuto + 1) * 60
        if await self._sumar(f"l#global#d#{dia}", 2 * 86400) > self.limites.global_por_dia:
            excedido = True
            self._bloqueado_hasta = max(self._bloqueado_hasta, (int(ahora // 86400) + 1) * 86400)
        if ip:
            # HMAC con una clave que no se guarda: sin la clave, el resumen no se revierte por fuerza bruta.
            resumen = hmac.new(_clave_ip(), f"{minuto}|{ip}".encode(), hashlib.sha256).hexdigest()[:16]
            if await self._sumar(f"l#ip#{resumen}#{minuto}", 120) > self.limites.ip_por_minuto:
                excedido = True
        if excedido:
            raise LimiteAlcanzado(MENSAJE_LIMITE)

    # ------------------------------------------------------------------ middleware MCP

    async def __call__(self, ctx: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
        """Middleware del SDK: envuelve cada `tools/call` con el límite y el registro."""
        if getattr(ctx, "method", None) != "tools/call":
            return await call_next(ctx)
        params = ctx.params or {}
        argumentos = params.get("arguments") or {}
        linea: dict[str, Any] = {"evento": "llamada", "herramienta": str(params.get("name", ""))[:60]}
        if isinstance(argumentos.get("servicio"), str):
            linea["servicio"] = argumentos["servicio"][:80]
        if isinstance(argumentos.get("variable"), int):
            linea["variable"] = argumentos["variable"]
        cabeceras = getattr(getattr(ctx, "request", None), "headers", None)
        self._anotar_cabeceras(cabeceras)

        registro = Registro()
        token = _registro.set(registro)
        inicio = time.perf_counter()
        try:
            try:
                await self.chequear_llamada(ip_del_cliente(ctx))
            except LimiteAlcanzado as e:
                linea["estado"] = "limite"
                return CallToolResult(content=[TextContent(type="text", text=str(e))], is_error=True)
            resultado = await call_next(ctx)
            es_error = bool(getattr(resultado, "is_error", False) or (isinstance(resultado, dict) and resultado.get("isError")))
            linea["estado"] = "error" if es_error else "ok"
            if es_error and _es_limite(resultado):
                linea["estado"] = "limite_indec"
            return resultado
        except BaseException:
            linea["estado"] = "excepcion"
            raise
        finally:
            _registro.reset(token)
            linea["cache"] = (
                "-" if not (registro.desde_cache or registro.pedidos_afuera)
                else "hit" if not registro.pedidos_afuera
                else "miss" if not registro.desde_cache
                else "parcial"
            )
            linea["desde_cache"] = registro.desde_cache
            linea["pedidos_afuera"] = registro.pedidos_afuera
            linea["ms"] = round((time.perf_counter() - inicio) * 1000)
            self.registrar(linea)

    def _anotar_cabeceras(self, cabeceras: Any) -> None:
        """Registra qué cabeceras llegan (sólo los nombres, nunca los valores), una vez por combinación."""
        if not cabeceras:
            return
        nombres = tuple(sorted({str(k).lower() for k in cabeceras.keys()}))
        if nombres not in self._cabeceras_vistas and len(self._cabeceras_vistas) < 50:
            self._cabeceras_vistas.add(nombres)
            self.registrar({"evento": "cabeceras", "nombres": list(nombres)})

    def _error_backend(self, operacion: str, error: Exception) -> None:
        self.registrar({"evento": "error_cache", "operacion": operacion, "tipo": type(error).__name__})


def _es_limite(resultado: Any) -> bool:
    for c in getattr(resultado, "content", None) or []:
        if MENSAJE_LIMITE in (getattr(c, "text", "") or ""):
            return True
    return False


def ip_del_cliente(ctx: Any) -> str | None:
    """IP de quien llama: la primera de X-Forwarded-For (Lambda Function URL la completa), o la del socket."""
    pedido = getattr(ctx, "request", None)
    if pedido is None:
        return None
    reenviada = (pedido.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if reenviada:
        return reenviada
    cliente = getattr(pedido, "client", None)
    return getattr(cliente, "host", None)


# --------------------------------------------------------------------------- arranque


def cargar_token_desde_ssm() -> None:
    """Si INDICES_AR_TOKEN_SSM nombra un parámetro, lee el token de ahí y lo deja en el entorno del proceso."""
    nombre = os.environ.get("INDICES_AR_TOKEN_SSM", "").strip()
    if not nombre or os.environ.get("INDEC_API_TOKEN"):
        return
    import boto3

    try:
        r = boto3.client("ssm").get_parameter(Name=nombre, WithDecryption=True)
        os.environ["INDEC_API_TOKEN"] = r["Parameter"]["Value"]
    except Exception as e:  # sin token igual funcionan las herramientas del BCRA
        escribir_registro({"evento": "token_no_disponible", "tipo": type(e).__name__})


def crear_capa() -> Capa:
    backend_nombre = os.environ.get("INDICES_AR_CACHE", "memoria").strip().lower()
    if backend_nombre == "dynamodb":
        tabla = os.environ.get("INDICES_AR_TABLA", "").strip()
        if not tabla:
            raise SystemExit("INDICES_AR_CACHE=dynamodb necesita INDICES_AR_TABLA con el nombre de la tabla.")
        backend: Any = Dynamo(tabla)
    elif backend_nombre == "memoria":
        backend = Memoria()
    else:
        raise SystemExit(f"INDICES_AR_CACHE no reconocido: {backend_nombre}. Opciones: memoria, dynamodb.")
    return Capa(backend, Limites.desde_entorno())
