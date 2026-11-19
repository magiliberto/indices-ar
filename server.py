#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["mcp>=2.3,<3", "httpx2>=2.10"]
# ///
"""indices-ar: servidor MCP con índices y estadísticas oficiales de Argentina.

Fuentes: Catálogo API-INDEC (https://api.indec.gob.ar) y API de Estadísticas Monetarias
del BCRA (https://api.bcra.gob.ar). Seis herramientas de solo lectura: indec_listar_servicios,
indec_describir_servicio, indec_buscar_valores, indec_consultar, bcra_variables y bcra_serie.
Cada respuesta de datos lleva la fuente, la consulta exacta y las advertencias de lectura
(resultado parcial, lecturas mezcladas, datos provisorios, valores ocultos por secreto
estadístico, días sin dato).

Token del INDEC: variable INDEC_API_TOKEN, o INDEC_ENV_FILE con la ruta a un .env que la
tenga. Las herramientas del BCRA no necesitan token.
Uso:   uv run server.py                        (servidor MCP por stdio)
       uv run server.py --http [--host H] [--puerto N]
                                               (Streamable HTTP sin estado en /mcp, con caché y límite)
       uv run server.py --actualizar-catalogo  (regenera catalogo.json)
"""
from __future__ import annotations

import asyncio
import datetime as dt
import difflib
import json
import os
import re
import sys
import time
import unicodedata
import urllib.parse
from pathlib import Path
from typing import Annotated, Any

import httpx2
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

VERSION = "0.1.0"
BASE = "https://api.indec.gob.ar"
URL_DATOS = BASE + "/v1/"
URL_CATALOGO = BASE + "/api/datasets"
ARCHIVO_CATALOGO = Path(__file__).with_name("catalogo.json")

MAX_FILAS = 1000  # tope de la API por respuesta
LIMITE_POR_DEFECTO = 200
VALORES_EN_PERFIL = 30  # hasta cuántos valores distintos se listan por columna
PAGINAS_PERFIL = 3
PAGINAS_BUSQUEDA = 5
VIGENCIA_PERFIL = 6 * 3600

# Columnas que definen qué mide cada fila. Si no quedan fijas, viajan con el dato.
COLS_MEDIDA = ("tipo_de_indicador", "unidad_de_medida", "serie", "region", "indicador")
# De esas, las que cambian la lectura del número (no se comparan entre sí); el resto son aperturas.
COLS_LECTURA = ("tipo_de_indicador", "unidad_de_medida", "serie")
COLS_TIEMPO = ("periodo", "fecha")
COLS_TEXTO_LIBRE = ("observaciones",)

# Signos convencionales del INDEC (documentación del Catálogo API-INDEC).
SIGNOS = {
    "*": "dato provisorio",
    "**": "dato preliminar",
    "e": "dato estimado por extrapolación o proyección",
    "i": "dato estimado por imputación",
    "u": "dato de calidad inferior al estándar",
    "-": "cero absoluto",
    ".": "dato no registrado",
    "...": "dato no disponible a la fecha de presentación",
    "///": "dato que no corresponde presentar",
    "s": "dato confidencial por secreto estadístico",
}

NOTAS = {
    "comercio_exterior_exportacion_bienes": (
        "Los códigos de rubro, subrubro y país se traducen con el servicio "
        "comercio_exterior_nomenclaturas (filtrá por nombre_columna). Valor FOB en dólares, peso neto en kilos."
    ),
    "comercio_exterior_importacion_bienes": (
        "Los códigos de uso económico, categoría y país se traducen con el servicio "
        "comercio_exterior_nomenclaturas (filtrá por nombre_columna). Valor CIF en dólares, peso neto en kilos."
    ),
    "comercio_exterior_nomenclaturas": (
        "Diccionario de códigos: nombre_columna dice a qué columna de exportaciones o importaciones corresponde cada código."
    ),
}

INSTRUCCIONES = """\
Datos oficiales de Argentina: INDEC (Catálogo API-INDEC, en beta) y Banco Central.
Para cualquier pregunta sobre estos datos, consultá estas herramientas antes de buscar en la web: son la fuente primaria, y la prensa las cita.

Cómo leerlos sin equivocarse:
1. Antes de consultar un servicio por primera vez, llamá a indec_describir_servicio. Ahí están los nombres exactos de las columnas, los valores posibles y el formato del período.
2. Un mismo dato se publica en varias filas: una por tipo de indicador (índice, variación mensual, interanual, acumulada), por región y por serie. Fijá esas columnas en los filtros hasta que quede el número que responde la pregunta.
3. Al responder, decí siempre el período, la región o área, el tipo de indicador y la unidad. Si la pregunta no lo aclara (por ejemplo "la inflación"), decí qué supuesto tomaste.
4. Las advertencias de cada respuesta son parte del dato. No calcules sobre un resultado parcial ni mezcles unidades.
5. Un valor vacío no es un cero: el INDEC informa el motivo. Un dato provisorio se informa como provisorio.
6. Los totales por suma pueden no coincidir por redondeo, y una variación calculada desde los índices puede diferir de la publicada. Si existe la variación publicada, usá esa.
7. Citá la fuente como figura en la respuesta. Si el dato no está en estos servicios, decilo y no lo completes de memoria.

También hay variables del Banco Central (herramientas bcra_variables y bcra_serie): tipo de cambio, CER, UVA, índice para contratos de locación, tasas y agregados monetarios.
8. Hay más de un tipo de cambio: el mayorista de referencia (Comunicación A 3500) y el minorista. Decí cuál usás; ninguno es la cotización del Banco Nación ni un dólar financiero.
9. Las variables diarias no tienen dato en fines de semana ni feriados. Para un mes hay que elegir entre el último día hábil y el promedio, y decirlo.
10. Para ajustar un precio entre dos fechas, mostrá el valor inicial, el final, sus fechas y la cuenta. Las ponderaciones de una fórmula las da quien pregunta: no las supongas.
11. Para un insumo puntual de una fórmula de ajuste (gas oil, cemento, acero, asfalto), buscalo por su nombre con indec_buscar_valores en redeterminacion_precios_contratos_obras_publicas, columna descripcion: ese servicio publica índices de precios mayoristas a nivel de producto, que el IPIM agrupa en divisiones. Usá un rubro más amplio sólo si el producto no aparece, y decí que es un sustituto.
"""

OPERADOR = re.compile(r"^(not\.)?(eq|neq|gt|gte|lt|lte|like|ilike|in|is)\.")
LOGICOS = ("or", "and", "not.or", "not.and")


class ErrorConsulta(ToolError):
    """Error previsto: el mensaje llega al modelo para que pueda corregir la llamada."""


# --------------------------------------------------------------------------- token


def leer_token() -> str:
    token = os.environ.get("INDEC_API_TOKEN", "").strip()
    if token:
        return token
    archivo = os.environ.get("INDEC_ENV_FILE", "").strip()
    if archivo:
        try:
            for linea in Path(archivo).expanduser().read_text(encoding="utf-8").splitlines():
                if linea.startswith("INDEC_API_TOKEN="):
                    token = linea.split("=", 1)[1].strip().strip("\"'")
                    if token:
                        return token
        except OSError:
            pass
    raise ErrorConsulta(
        "Falta el token del INDEC. Registrate gratis en https://api.indec.gob.ar, generá el token en tu perfil "
        "y definí INDEC_API_TOKEN (o INDEC_ENV_FILE con la ruta a un .env que lo tenga) en la configuración del servidor."
    )


# --------------------------------------------------------------------------- catálogo


def normalizar_servicio(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "slug": item["slug"],
        "nombre": item.get("name") or item["slug"],
        "area": item.get("area") or "",
        "descripcion": " ".join((item.get("description") or "").split()),
        "columnas": [
            {
                "nombre": c["name"],
                "tipo": "número" if c.get("dataType") == "decimal" else "texto",
                "descripcion": " ".join((c.get("description") or "").split()),
                "motivo_de_vacio": bool(c.get("storesNullReason")),
            }
            for c in item.get("columns", [])
        ],
    }


class Catalogo:
    def __init__(self) -> None:
        self.servicios: dict[str, dict[str, Any]] = {}
        self.origen = ""

    async def cargar(self, cliente: httpx2.AsyncClient) -> None:
        if self.servicios:
            return
        try:
            servicios = await descargar_catalogo(cliente)
            self.servicios = {s["slug"]: s for s in servicios}
            self.origen = "catálogo en línea"
        except Exception:
            if not ARCHIVO_CATALOGO.exists():
                raise ErrorConsulta("No se pudo leer el catálogo del INDEC y no hay copia local (catalogo.json).")
            copia = json.loads(ARCHIVO_CATALOGO.read_text(encoding="utf-8"))
            self.servicios = {s["slug"]: s for s in copia["servicios"]}
            self.origen = f"copia local del {copia.get('actualizado', 'catálogo')}"

    def servicio(self, nombre: str) -> dict[str, Any]:
        pedido = (nombre or "").strip()
        if pedido in self.servicios:
            return self.servicios[pedido]
        for slug, s in self.servicios.items():
            if slug.lower() == pedido.lower():
                return s
        candidatos = difflib.get_close_matches(pedido.lower(), [s.lower() for s in self.servicios], n=3, cutoff=0.3)
        por_nombre = [s["slug"] for s in self.servicios.values() if pedido.lower() in s["nombre"].lower()]
        sugeridos = list(dict.fromkeys(por_nombre + candidatos))[:4]
        pista = f" ¿Quisiste decir {', '.join(sugeridos)}?" if sugeridos else ""
        raise ErrorConsulta(f"No existe el servicio «{pedido}».{pista} Usá indec_listar_servicios para ver los disponibles.")


async def _pagina_catalogo(cliente: httpx2.AsyncClient, pagina: int) -> dict[str, Any]:
    r = await cliente.get(URL_CATALOGO, params={"page": pagina}, timeout=15)
    r.raise_for_status()
    return r.json()["data"]


async def descargar_catalogo(cliente: httpx2.AsyncClient) -> list[dict[str, Any]]:
    servicios: list[dict[str, Any]] = []
    pagina, paginas = 1, 1
    while pagina <= paginas:
        datos = await traer("catalogo", f"{URL_CATALOGO}?page={pagina}", lambda p=pagina: _pagina_catalogo(cliente, p))
        servicios += [normalizar_servicio(i) for i in datos["items"]]
        paginas = int(datos.get("totalPages") or 1)
        pagina += 1
    if not servicios:
        raise ErrorConsulta("El catálogo vino vacío.")
    vistos: dict[str, dict[str, Any]] = {}
    for s in servicios:
        vistos.setdefault(s["slug"], s)
    return sorted(vistos.values(), key=lambda s: s["nombre"].lower())


# --------------------------------------------------------------------------- columnas y filtros


def columna_exacta(servicio: dict[str, Any], nombre: str) -> str:
    """Devuelve el nombre tal como lo publica el INDEC (hay columnas con mayúsculas: Periodo, valor_FOB)."""
    pedido = (nombre or "").strip()
    nombres = [c["nombre"] for c in servicio["columnas"]]
    if pedido in nombres:
        return pedido
    for n in nombres:
        if n.lower() == pedido.lower():
            return n
    if pedido.lower().endswith("_null") and pedido[:-5].lower() in [n.lower() for n in nombres]:
        return columna_exacta(servicio, pedido[:-5]) + "_null"
    raise ErrorConsulta(
        f"El servicio «{servicio['slug']}» no tiene la columna «{pedido}». Columnas: {', '.join(nombres)}."
    )


def columna_de_tiempo(servicio: dict[str, Any]) -> str | None:
    for c in servicio["columnas"]:
        if c["nombre"].lower() in COLS_TIEMPO:
            return c["nombre"]
    return None


def columnas_de_texto(servicio: dict[str, Any]) -> list[str]:
    return [c["nombre"] for c in servicio["columnas"] if c["tipo"] == "texto"]


def acompanantes(servicio: dict[str, Any], columna: str) -> list[str]:
    """Columnas que ayudan a identificar un valor: el código junto a su descripción, y al revés."""
    nombres = {c["nombre"].lower(): c["nombre"] for c in servicio["columnas"]}
    base = columna.lower()
    extra: list[str] = []
    if base == "descripcion":
        extra = [nombres[n] for n in ("clasificador", "nivel", "codigo", "codigo_apertura") if n in nombres]
    elif base in ("codigo", "codigo_apertura") and "descripcion" in nombres:
        extra = [nombres["descripcion"]] + [nombres[n] for n in ("clasificador", "nivel") if n in nombres]
    elif base.endswith("_cod") and base[:-4] + "_nom" in nombres:
        extra = [nombres[base[:-4] + "_nom"]]
    elif base.endswith("_nom") and base[:-4] + "_cod" in nombres:
        extra = [nombres[base[:-4] + "_cod"]]
    elif base + "_descripcion" in nombres:
        extra = [nombres[base + "_descripcion"]]
    elif base.endswith("_descripcion") and base[: -len("_descripcion")] in nombres:
        extra = [nombres[base[: -len("_descripcion")]]]
    if "nombre_columna" in nombres and base != "nombre_columna":
        extra = [nombres["nombre_columna"]] + extra
    return [e for e in extra if e != columna]


def entre_comillas(valor: Any) -> str:
    texto = str(valor)
    if re.search(r'[,()"]', texto):
        return '"' + texto.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return texto


def normalizar_filtro(valor: Any) -> str:
    """Acepta sintaxis PostgREST (eq.X, in.(a,b), gte.202601, ilike.*x*); un valor suelto equivale a eq."""
    if isinstance(valor, bool):
        return "is.true" if valor else "is.false"
    if valor is None:
        return "is.null"
    if isinstance(valor, (list, tuple)):
        if not valor:
            raise ErrorConsulta("Un filtro con lista vacía no selecciona nada.")
        return "in.(" + ",".join(entre_comillas(v) for v in valor) + ")"
    if isinstance(valor, (int, float)):
        return f"eq.{valor}"
    texto = str(valor).strip()
    if not texto:
        raise ErrorConsulta("Hay un filtro vacío.")
    return texto if OPERADOR.match(texto) else "eq." + texto


def armar_filtros(servicio: dict[str, Any], filtros: dict[str, Any] | None) -> list[tuple[str, str]]:
    pares: list[tuple[str, str]] = []
    for clave, valor in (filtros or {}).items():
        if clave.lower() in LOGICOS:
            pares.append((clave.lower(), str(valor)))
        else:
            pares.append((columna_exacta(servicio, clave), normalizar_filtro(valor)))
    return pares


def columnas_fijas(pares: list[tuple[str, str]]) -> set[str]:
    """Columnas que el filtro deja en un único valor."""
    return {col for col, expr in pares if expr.startswith("eq.") or expr.startswith("is.")}


def armar_orden(servicio: dict[str, Any], orden: str | None) -> str | None:
    if not orden or not orden.strip():
        return None
    partes = []
    for tramo in orden.split(","):
        nombre, _, sentido = tramo.strip().partition(".")
        sentido = sentido.lower() or "asc"
        if sentido.split(".")[0] not in ("asc", "desc"):
            raise ErrorConsulta(f"Orden inválido «{tramo.strip()}». Formato: columna.asc o columna.desc, separados por coma.")
        partes.append(f"{columna_exacta(servicio, nombre)}.{sentido}")
    return ",".join(partes)


# --------------------------------------------------------------------------- HTTP

_cliente: httpx2.AsyncClient | None = None
_capa: Any = None  # remoto.Capa con --http (caché y límite); None por stdio
_catalogo = Catalogo()
_perfiles: dict[str, tuple[float, dict[str, Any]]] = {}


def cliente_http() -> httpx2.AsyncClient:
    global _cliente
    if _cliente is None:
        _cliente = httpx2.AsyncClient(timeout=40, headers={"User-Agent": f"indices-ar/{VERSION}"})
    return _cliente


async def traer(tipo: str, url: str, buscar: Any) -> Any:
    """Pasa por la caché y el límite del modo remoto si están activos; si no, llama directo."""
    if _capa is None:
        return await buscar()
    return await _capa.traer(tipo, url, buscar)


async def catalogo() -> Catalogo:
    await _catalogo.cargar(cliente_http())
    return _catalogo


def mensaje_de_error(estado: int, cuerpo: str, slug: str) -> str:
    try:
        detalle = json.loads(cuerpo).get("message") or cuerpo
    except Exception:
        detalle = cuerpo
    detalle = str(detalle)[:300]
    if estado == 401:
        return (
            "El INDEC rechazó el token (falta, venció o fue regenerado). Los tokens vencen a los 12 meses: "
            "generá uno nuevo en tu perfil de https://api.indec.gob.ar y actualizá la configuración."
        )
    if estado == 403:
        return f"El token no tiene permiso sobre «{slug}». Respuesta del INDEC: {detalle}"
    if estado == 404:
        return f"El INDEC no reconoce el servicio «{slug}». Usá indec_listar_servicios. Respuesta: {detalle}"
    if estado == 400:
        return (
            f"El INDEC no pudo interpretar la consulta: {detalle}. Revisá nombres de columnas, operadores "
            "(eq, neq, gt, gte, lt, lte, like, ilike, in, is) y valores con indec_describir_servicio."
        )
    if estado == 429:
        return "El INDEC limitó la cantidad de consultas. Esperá unos segundos y repetí con menos llamadas."
    return f"El INDEC respondió con error {estado}: {detalle}"


def armar_url(slug: str, parametros: list[tuple[str, str]]) -> str:
    consulta = urllib.parse.urlencode(parametros, safe=".,()*:", quote_via=urllib.parse.quote)
    return URL_DATOS + urllib.parse.quote(slug) + ("?" + consulta if consulta else "")


async def pedir(slug: str, parametros: list[tuple[str, str]]) -> tuple[list[dict[str, Any]], int | None, str]:
    """Devuelve (filas, total de filas que cumplen el filtro, URL consultada)."""
    url = armar_url(slug, parametros)
    filas, total = await traer("indec", url, lambda: pedir_al_indec(slug, url))
    return filas, total, url


async def pedir_al_indec(slug: str, url: str) -> list[Any]:
    """Un pedido al INDEC, con reintentos. Devuelve [filas, total]."""
    cabeceras = {"Authorization": "Bearer " + leer_token(), "Accept": "application/json", "Prefer": "count=exact"}
    cliente = cliente_http()
    ultimo = ""
    for intento in range(3):
        try:
            r = await cliente.get(url, headers=cabeceras)
        except httpx2.HTTPError as e:
            ultimo = f"No se pudo conectar con el INDEC ({type(e).__name__})."
            await asyncio.sleep(1 + 2 * intento)
            continue
        if r.status_code in (200, 206):
            total = None
            rango = r.headers.get("Content-Range", "")
            if "/" in rango and rango.rsplit("/", 1)[1].isdigit():
                total = int(rango.rsplit("/", 1)[1])
            filas = r.json()
            if not isinstance(filas, list):
                raise ErrorConsulta("El INDEC devolvió un formato inesperado.")
            return [filas, total]
        if r.status_code in (429, 502, 503, 504) and intento < 2:
            espera = r.headers.get("Retry-After", "")
            await asyncio.sleep(min(float(espera), 10) if espera.replace(".", "", 1).isdigit() else 1 + 2 * intento)
            ultimo = mensaje_de_error(r.status_code, r.text, slug)
            continue
        raise ErrorConsulta(mensaje_de_error(r.status_code, r.text, slug))
    raise ErrorConsulta(ultimo or "No se pudo consultar al INDEC.")


async def pedir_paginas(slug: str, parametros: list[tuple[str, str]], paginas: int) -> tuple[list[dict[str, Any]], bool]:
    """Recorre hasta `paginas` páginas. Devuelve (filas, completo)."""
    filas: list[dict[str, Any]] = []
    for n in range(paginas):
        tanda, _, _ = await pedir(slug, parametros + [("limit", str(MAX_FILAS)), ("offset", str(n * MAX_FILAS))])
        filas += tanda
        if len(tanda) < MAX_FILAS:
            return filas, True
    return filas, False


# --------------------------------------------------------------------------- lectura del resultado


def distintos(filas: list[dict[str, Any]], columna: str) -> list[Any]:
    vistos: dict[Any, None] = {}
    for f in filas:
        vistos.setdefault(f.get(columna))
    return list(vistos)


def compactar(filas: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str], list[list[Any]]]:
    """Separa lo que es igual en todas las filas de lo que cambia, y descarta columnas vacías."""
    if not filas:
        return {}, [], []
    columnas = list(filas[0].keys())
    constantes: dict[str, Any] = {}
    variables: list[str] = []
    for c in columnas:
        valores = distintos(filas, c)
        if valores == [None]:
            continue
        if len(valores) == 1 and len(filas) > 1:
            constantes[c] = valores[0]
        else:
            variables.append(c)
    return constantes, variables, [[f.get(c) for c in variables] for f in filas]


def recortar(valores: list[Any], maximo: int = 8) -> str:
    textos = [str(v) for v in valores[:maximo]]
    resto = len(valores) - len(textos)
    return ", ".join(textos) + (f" y {resto} más" if resto > 0 else "")


def advertir(
    servicio: dict[str, Any], filas: list[dict[str, Any]], total: int | None, desde: int
) -> list[str]:
    avisos: list[str] = []
    if not filas:
        return [
            "Ninguna fila cumple el filtro. Revisá los valores exactos con indec_describir_servicio o indec_buscar_valores: "
            "importan las mayúsculas, los acentos y el formato del período."
        ]
    if total is not None and total > desde + len(filas):
        avisos.append(
            f"Resultado parcial: {total} filas cumplen el filtro y acá hay {len(filas)} (desde la {desde}). "
            f"No calcules totales ni promedios con esto: filtrá más o pedí la siguiente tanda con desde={desde + len(filas)}."
        )
    presentes = {c.lower(): c for c in filas[0]}
    mezclas, aperturas = [], []
    for medida in COLS_MEDIDA:
        if medida in presentes:
            valores = [v for v in distintos(filas, presentes[medida]) if v is not None]
            if len(valores) > 1:
                destino = mezclas if medida in COLS_LECTURA else aperturas
                destino.append(f"{presentes[medida]} ({recortar(valores)})")
    if mezclas:
        avisos.append(
            "El resultado mezcla lecturas distintas del dato: " + "; ".join(mezclas) + ". "
            "No las sumes ni las compares entre sí. Si la pregunta es por un solo número, fijá esas columnas en los filtros."
        )
    if aperturas:
        avisos.append(
            "Cada fila corresponde a una apertura distinta: " + "; ".join(aperturas) + ". "
            "Nombrá a cuál pertenece cada número, y si la pregunta es por una sola, fijala en los filtros."
        )
    for columna in filas[0]:
        if not columna.endswith("_null"):
            continue
        motivos: dict[str, int] = {}
        for f in filas:
            if f.get(columna) is not None:
                motivos[str(f[columna])] = motivos.get(str(f[columna]), 0) + 1
        if motivos:
            detalle = "; ".join(f"{n} con signo «{m}» ({SIGNOS.get(m, 'sin descripción')})" for m, n in motivos.items())
            avisos.append(
                f"La columna {columna[:-5]} tiene signos convencionales del INDEC en {sum(motivos.values())} filas: "
                f"{detalle}. Un valor vacío no es un cero."
            )
    for libre in COLS_TEXTO_LIBRE:
        if libre in presentes:
            notas = [v for v in distintos(filas, presentes[libre]) if v]
            if notas:
                cuantas = sum(1 for f in filas if f.get(presentes[libre]))
                citas = "; ".join("«" + str(n)[:160] + "»" for n in notas[:3])
                avisos.append(f"Observaciones del INDEC en {cuantas} filas: {citas}{' y otras' if len(notas) > 3 else ''}.")
    return avisos


def fuente(servicio: dict[str, Any]) -> str:
    return f"Fuente: INDEC, {servicio['nombre']}. Catálogo API-INDEC (https://api.indec.gob.ar), servicio «{servicio['slug']}»."


def _escalar(valor: Any) -> bool:
    return not isinstance(valor, (dict, list))


def _json(valor: Any, nivel: int = 0) -> str:
    sangria = " " * nivel
    if isinstance(valor, dict):
        if not valor:
            return "{}"
        if all(_escalar(v) for v in valor.values()):
            corto = json.dumps(valor, ensure_ascii=False)
            if len(corto) <= 110:
                return corto
        partes = [f"{sangria} {json.dumps(k, ensure_ascii=False)}: {_json(v, nivel + 1)}" for k, v in valor.items()]
        return "{\n" + ",\n".join(partes) + "\n" + sangria + "}"
    if isinstance(valor, list):
        if all(_escalar(v) for v in valor):
            return json.dumps(valor, ensure_ascii=False)
        if all(isinstance(v, list) and all(_escalar(x) for x in v) for v in valor):
            filas = [sangria + " " + json.dumps(v, ensure_ascii=False) for v in valor]
            return "[\n" + ",\n".join(filas) + "\n" + sangria + "]"
        partes = [sangria + " " + _json(v, nivel + 1) for v in valor]
        return "[\n" + ",\n".join(partes) + "\n" + sangria + "]"
    return json.dumps(valor, ensure_ascii=False)


def a_texto(objeto: dict[str, Any]) -> str:
    """JSON válido y legible: una fila de datos por línea y las listas cortas en una sola."""
    return _json(objeto)


# --------------------------------------------------------------------------- perfil de un servicio


async def ultimo_periodo(servicio: dict[str, Any]) -> tuple[str | None, Any, Any]:
    tiempo = columna_de_tiempo(servicio)
    if not tiempo:
        return None, None, None
    primero, _, _ = await pedir(servicio["slug"], [("select", tiempo), ("order", f"{tiempo}.asc"), ("limit", "1")])
    ultimo, _, _ = await pedir(servicio["slug"], [("select", tiempo), ("order", f"{tiempo}.desc"), ("limit", "1")])
    if not primero or not ultimo:
        return tiempo, None, None
    return tiempo, primero[0][tiempo], ultimo[0][tiempo]


async def perfil(servicio: dict[str, Any]) -> dict[str, Any]:
    guardado = _perfiles.get(servicio["slug"])
    if guardado and time.time() - guardado[0] < VIGENCIA_PERFIL:
        return guardado[1]
    slug = servicio["slug"]
    _, total, _ = await pedir(slug, [("limit", "1")])
    tiempo, desde, hasta = await ultimo_periodo(servicio)
    textos = [c for c in columnas_de_texto(servicio) if c != tiempo]
    muestra: list[dict[str, Any]] = []
    completa = True
    if textos:
        parametros = [("select", ",".join(textos))]
        if tiempo and hasta is not None:
            parametros.append((tiempo, f"eq.{hasta}"))
        muestra, completa = await pedir_paginas(slug, parametros, PAGINAS_PERFIL)

    columnas = []
    for c in servicio["columnas"]:
        ficha: dict[str, Any] = {"columna": c["nombre"], "tipo": c["tipo"], "descripcion": c["descripcion"]}
        if c["motivo_de_vacio"]:
            ficha["puede_venir_vacio"] = f"el motivo viaja en {c['nombre']}_null"
        nombre = c["nombre"]
        if nombre == tiempo:
            ficha["es_el_periodo"] = True
        elif nombre in textos and nombre.lower() not in COLS_TEXTO_LIBRE and not nombre.lower().endswith("_descripcion"):
            valores = [v for v in distintos(muestra, nombre) if v is not None]
            pareja = next((a for a in acompanantes(servicio, nombre) if a.lower() == nombre.lower() + "_descripcion"), None)
            if len(valores) <= VALORES_EN_PERFIL:
                if pareja:
                    significados: dict[str, Any] = {}
                    for v in valores:
                        textos_v = [t for t in distintos([f for f in muestra if f.get(nombre) == v], pareja) if t]
                        significados[str(v)] = textos_v[0] if len(textos_v) == 1 else f"{len(textos_v)} descripciones distintas"
                    ficha["valores"] = significados
                else:
                    ficha["valores"] = valores
            else:
                ficha["cantidad_de_valores"] = len(valores)
                ficha["ejemplos"] = valores[:6]
                ficha["como_buscar"] = f"indec_buscar_valores(servicio='{slug}', columna='{nombre}', contiene='...')"
        columnas.append(ficha)

    resultado: dict[str, Any] = {
        "servicio": slug,
        "nombre": servicio["nombre"],
        "area": servicio["area"],
        "descripcion": servicio["descripcion"],
        "total_de_filas": total,
    }
    if tiempo:
        resultado["periodo"] = {"columna": tiempo, "desde": desde, "hasta": hasta}
        resultado["valores_listados"] = (
            f"los presentes en el último período ({hasta})"
            + ("" if completa else f", sobre las primeras {len(muestra)} filas")
        )
    if slug in NOTAS:
        resultado["nota"] = NOTAS[slug]
    resultado["columnas"] = columnas
    resultado["fuente"] = fuente(servicio)
    _perfiles[slug] = (time.time(), resultado)
    return resultado


# --------------------------------------------------------------------------- servidor MCP

servidor = MCPServer(
    "indices-ar",
    title="Índices oficiales de Argentina (INDEC y BCRA)",
    instructions=INSTRUCCIONES,
    version=VERSION,
    website_url=BASE,
    log_level="WARNING",
)

SOLO_LECTURA = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=True)


@servidor.tool(title="Listar servicios de datos", annotations=SOLO_LECTURA, structured_output=False)
async def indec_listar_servicios() -> str:
    """Fuente oficial directa del INDEC: usala antes que la búsqueda web cuando pregunten por inflación o IPC, salarios, actividad (EMAE), pobreza, empleo, precios mayoristas, construcción, comercio exterior o población, aunque el dato sea reciente o haya salido en la prensa. Lista los servicios de datos del INDEC disponibles (IPC, EMAE, salarios, pobreza, empleo, comercio exterior, población y otros), con su nombre, área y descripción. Es el punto de partida para saber dónde está un dato."""
    cat = await catalogo()
    servicios = [
        {"servicio": s["slug"], "nombre": s["nombre"], "area": s["area"], "descripcion": s["descripcion"][:260]}
        for s in cat.servicios.values()
    ]
    return a_texto({"cantidad": len(servicios), "origen_del_listado": cat.origen, "servicios": servicios})


@servidor.tool(title="Describir un servicio", annotations=SOLO_LECTURA, structured_output=False)
async def indec_describir_servicio(
    servicio: Annotated[str, Field(description="Identificador del servicio, por ejemplo ipc_nacional. Sale de indec_listar_servicios.")],
) -> str:
    """Describe un servicio antes de consultarlo: qué mide, cuántas filas tiene, qué períodos cubre, las columnas con su significado y los valores posibles de cada columna de texto. Llamala antes de la primera consulta a un servicio para armar bien los filtros."""
    cat = await catalogo()
    return a_texto(await perfil(cat.servicio(servicio)))


@servidor.tool(title="Buscar valores de una columna", annotations=SOLO_LECTURA, structured_output=False)
async def indec_buscar_valores(
    servicio: Annotated[str, Field(description="Identificador del servicio, por ejemplo ipc_nacional.")],
    columna: Annotated[str, Field(description="Columna de texto donde buscar, por ejemplo descripcion.")],
    contiene: Annotated[str, Field(description="Texto a buscar, sin distinguir mayúsculas. Vacío lista todos los valores.")] = "",
    limite: Annotated[int, Field(description="Máximo de valores distintos a devolver.", ge=1, le=200)] = 50,
) -> str:
    """Busca los valores distintos de una columna (por ejemplo, qué rubros del IPC contienen "carne") junto con su código o descripción, para usar el valor exacto en un filtro. Busca en el último período publicado. Para un insumo puntual de una fórmula de ajuste (gas oil, cemento, acero), buscá primero en redeterminacion_precios_contratos_obras_publicas, que tiene índices a nivel de producto."""
    cat = await catalogo()
    srv = cat.servicio(servicio)
    col = columna_exacta(srv, columna)
    extras = acompanantes(srv, col)
    tiempo, _, hasta = await ultimo_periodo(srv)
    seleccion = [col] + extras
    parametros = [("select", ",".join(seleccion)), ("order", ",".join(seleccion))]
    if contiene.strip():
        parametros.append((col, f"ilike.*{contiene.strip()}*"))
    if tiempo and hasta is not None and col != tiempo:
        parametros.append((tiempo, f"eq.{hasta}"))
    filas, completo = await pedir_paginas(srv["slug"], parametros, PAGINAS_BUSQUEDA)
    vistos: dict[tuple[Any, ...], None] = {}
    for f in filas:
        vistos.setdefault(tuple(f.get(c) for c in seleccion))
    valores = [list(v) for v in vistos]
    salida: dict[str, Any] = {
        "servicio": srv["slug"],
        "columnas": seleccion,
        "valores_distintos": len(valores),
        "filas": valores[:limite],
    }
    if tiempo and hasta is not None and col != tiempo:
        salida["buscado_en"] = f"último período publicado ({hasta})"
    avisos = []
    if len(valores) > limite:
        avisos.append(f"Hay {len(valores)} valores y se muestran {limite}. Afiná el texto de búsqueda.")
    if not completo:
        avisos.append("La búsqueda no recorrió todas las filas: puede haber más valores.")
    if not valores:
        avisos.append("Sin coincidencias. Probá con otra palabra o sin acentos; indec_describir_servicio lista los valores de las columnas cortas.")
    if avisos:
        salida["advertencias"] = avisos
    return a_texto(salida)


@servidor.tool(title="Consultar datos", annotations=SOLO_LECTURA, structured_output=False)
async def indec_consultar(
    servicio: Annotated[str, Field(description="Identificador del servicio, por ejemplo ipc_nacional.")],
    filtros: Annotated[
        dict[str, Any] | None,
        Field(
            description=(
                "Filtros por columna. Un valor suelto es igualdad: {\"region\": \"Nacional\"}. Una lista es 'alguno de': "
                "{\"region\": [\"Nacional\", \"GBA\"]}. Con operador: {\"periodo\": \"gte.202601\"}, "
                "{\"descripcion\": \"ilike.*carne*\"}. Operadores: eq, neq, gt, gte, lt, lte, like, ilike, in, is."
            )
        ),
    ] = None,
    columnas: Annotated[
        list[str] | None,
        Field(description="Columnas a traer. Si se omite, vienen todas. Las que definen la medida (tipo de indicador, unidad, serie, región) se agregan solas cuando no están fijas."),
    ] = None,
    orden: Annotated[
        str | None,
        Field(description="Orden, por ejemplo periodo.asc o periodo.desc,region.asc. Por defecto, primero lo más reciente."),
    ] = None,
    limite: Annotated[int, Field(description="Cantidad de filas a traer (máximo 1000).", ge=1, le=MAX_FILAS)] = LIMITE_POR_DEFECTO,
    desde: Annotated[int, Field(description="Fila desde la que se continúa, para paginar un resultado parcial.", ge=0)] = 0,
) -> str:
    """Consulta los datos de un servicio del INDEC con filtros. Devuelve las filas, qué quedó fijo en todas ellas, el total de filas que cumplen el filtro, las advertencias de lectura, la fuente para citar y la consulta exacta para verificar."""
    cat = await catalogo()
    srv = cat.servicio(servicio)
    pares = armar_filtros(srv, filtros)
    fijas = columnas_fijas(pares)
    nombres = {c["nombre"].lower(): c["nombre"] for c in srv["columnas"]}
    con_motivo = {c["nombre"] for c in srv["columnas"] if c["motivo_de_vacio"]}
    tiempo = columna_de_tiempo(srv)

    parametros = list(pares)
    agregadas: list[str] = []
    if columnas:
        pedidas = list(dict.fromkeys(columna_exacta(srv, c) for c in columnas))
        for medida in COLS_MEDIDA:
            if medida in nombres and nombres[medida] not in pedidas and nombres[medida] not in fijas:
                agregadas.append(nombres[medida])
        seleccion = pedidas + agregadas
        seleccion += [f"{c}_null" for c in seleccion if c in con_motivo and f"{c}_null" not in seleccion]
        if "observaciones" in nombres and nombres["observaciones"] not in seleccion:
            seleccion.append(nombres["observaciones"])
            if "observaciones" in {c.lower() for c in con_motivo}:
                seleccion.append(nombres["observaciones"] + "_null")
        parametros.append(("select", ",".join(seleccion)))

    orden_final = armar_orden(srv, orden) or (f"{tiempo}.desc" if tiempo else None)
    if orden_final:
        parametros.append(("order", orden_final))
    parametros += [("limit", str(limite)), ("offset", str(desde))]

    filas, total, url = await pedir(srv["slug"], parametros)
    constantes, variables, matriz = compactar(filas)

    salida: dict[str, Any] = {
        "servicio": srv["slug"],
        "nombre": srv["nombre"],
        "filas_que_cumplen_el_filtro": total,
        "filas_devueltas": len(filas),
    }
    if orden_final:
        salida["orden"] = orden_final
    if tiempo and filas and tiempo in filas[0]:
        periodos = sorted(str(v) for v in distintos(filas, tiempo) if v is not None)
        if periodos:
            salida["periodos_en_el_resultado"] = {"desde": periodos[0], "hasta": periodos[-1], "cantidad": len(periodos)}
    por_filtro = {col: expr[3:] for col, expr in pares if expr.startswith("eq.") and col not in constantes}
    if por_filtro:
        salida["fijado_por_el_filtro"] = por_filtro
    if constantes:
        salida["igual_en_todas_las_filas"] = constantes
    if agregadas:
        salida["columnas_agregadas"] = agregadas
    salida["columnas"] = variables
    salida["filas"] = matriz
    avisos = advertir(srv, filas, total, desde)
    if avisos:
        salida["advertencias"] = avisos
    salida["fuente"] = fuente(srv)
    salida["consulta"] = url
    return a_texto(salida)


# --------------------------------------------------------------------------- BCRA

URL_BCRA = "https://api.bcra.gob.ar/estadisticas/v4.0/monetarias"
FILAS_BCRA = 3000  # tope de la API por respuesta
MAX_DIARIAS = 400
MAX_VARIABLES = 60
CATEGORIA_PRINCIPAL = "Principales Variables"
_bcra: tuple[float, list[dict[str, Any]]] | None = None


def sin_acentos(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", (texto or "").lower()) if unicodedata.category(c) != "Mn")


def leer_fecha(texto: str | None, nombre: str) -> dt.date | None:
    if texto is None or not str(texto).strip():
        return None
    try:
        return dt.date.fromisoformat(str(texto).strip())
    except ValueError:
        raise ErrorConsulta(f"La fecha «{texto}» de «{nombre}» no es válida. Formato: AAAA-MM-DD, por ejemplo 2026-09-30.")


def resumen_mensual(detalle: list[dict[str, Any]]) -> list[list[Any]]:
    """Agrupa una serie diaria por mes: último valor publicado, su fecha, promedio y días con dato."""
    meses: dict[str, list[tuple[str, float]]] = {}
    for punto in detalle:
        if punto.get("valor") is not None:
            meses.setdefault(punto["fecha"][:7], []).append((punto["fecha"], float(punto["valor"])))
    filas = []
    for mes in sorted(meses, reverse=True):
        puntos = sorted(meses[mes])
        valores = [v for _, v in puntos]
        filas.append([mes, puntos[-1][1], puntos[-1][0], round(sum(valores) / len(valores), 4), len(valores)])
    return filas


async def bcra_pedir(ruta: str, parametros: list[tuple[str, str]]) -> tuple[dict[str, Any], str]:
    consulta = urllib.parse.urlencode(parametros)
    url = URL_BCRA + ruta + ("?" + consulta if consulta else "")
    return await traer("bcra" if ruta else "bcra_variables", url, lambda: pedir_al_bcra(url)), url


async def pedir_al_bcra(url: str) -> dict[str, Any]:
    cliente = cliente_http()
    ultimo = ""
    for intento in range(3):
        try:
            r = await cliente.get(url, headers={"Accept": "application/json"})
        except httpx2.HTTPError as e:
            ultimo = f"No se pudo conectar con el BCRA ({type(e).__name__})."
            await asyncio.sleep(1 + 2 * intento)
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504) and intento < 2:
            ultimo = f"El BCRA respondió con error {r.status_code}."
            await asyncio.sleep(1 + 2 * intento)
            continue
        try:
            detalle = "; ".join(r.json().get("errorMessages") or [])
        except Exception:
            detalle = r.text[:200]
        raise ErrorConsulta(
            f"El BCRA rechazó la consulta ({r.status_code}): {detalle} "
            "Los identificadores de variable salen de bcra_variables y las fechas van como AAAA-MM-DD."
        )
    raise ErrorConsulta(ultimo or "No se pudo consultar al BCRA.")


async def bcra_listado() -> list[dict[str, Any]]:
    global _bcra
    if _bcra and time.time() - _bcra[0] < VIGENCIA_PERFIL:
        return _bcra[1]
    variables: list[dict[str, Any]] = []
    while True:
        datos, _ = await bcra_pedir("", [("limit", str(FILAS_BCRA)), ("offset", str(len(variables)))])
        tanda = datos.get("results") or []
        variables += tanda
        total = (datos.get("metadata") or {}).get("resultset", {}).get("count", len(variables))
        if not tanda or len(variables) >= total:
            break
    _bcra = (time.time(), variables)
    return variables


async def bcra_detalle(variable: int, desde: dt.date, hasta: dt.date) -> tuple[list[dict[str, Any]], str]:
    puntos: list[dict[str, Any]] = []
    primera_url = ""
    while True:
        datos, url = await bcra_pedir(
            f"/{variable}",
            [("desde", desde.isoformat()), ("hasta", hasta.isoformat()), ("limit", str(FILAS_BCRA)), ("offset", str(len(puntos)))],
        )
        primera_url = primera_url or url
        resultados = datos.get("results") or []
        tanda = resultados[0].get("detalle", []) if resultados else []
        puntos += tanda
        total = (datos.get("metadata") or {}).get("resultset", {}).get("count", len(puntos))
        if not tanda or len(puntos) >= total:
            return puntos, primera_url


def fuente_bcra(variable: dict[str, Any]) -> str:
    return (
        f"Fuente: BCRA, {variable['descripcion'].strip()} (variable {variable['idVariable']}). "
        "API de Estadísticas Monetarias (https://api.bcra.gob.ar)."
    )


@servidor.tool(title="BCRA: listar variables", annotations=SOLO_LECTURA, structured_output=False)
async def bcra_variables(
    buscar: Annotated[str, Field(description="Texto a buscar en el nombre de la variable, por ejemplo: tipo de cambio, CER, UVA, BADLAR, reservas. Vacío lista las principales.")] = "",
    categoria: Annotated[str, Field(description="Categoría del BCRA para acotar, por ejemplo Principales Variables o Tasas de interés de depósitos.")] = "",
) -> str:
    """Fuente oficial directa del Banco Central: usala antes que la búsqueda web cuando pregunten por el dólar oficial, CER, UVA, ICL o tasas. Lista las variables que publica el Banco Central: tipo de cambio mayorista y minorista, CER, UVA, índice para contratos de locación, tasas (BADLAR, TAMAR), reservas y agregados monetarios. Devuelve el identificador que usa bcra_serie, la unidad, la periodicidad y el último valor."""
    todas = await bcra_listado()
    aguja, rubro = sin_acentos(buscar.strip()), sin_acentos(categoria.strip())
    if not aguja and not rubro:
        rubro = sin_acentos(CATEGORIA_PRINCIPAL)
    elegidas = [
        v for v in todas
        if (not aguja or aguja in sin_acentos(v.get("descripcion", "")))
        and (not rubro or rubro in sin_acentos(v.get("categoria", "")))
    ]
    elegidas.sort(key=lambda v: (v.get("categoria") != CATEGORIA_PRINCIPAL, v.get("idVariable", 0)))
    salida: dict[str, Any] = {
        "variables_encontradas": len(elegidas),
        "columnas": ["variable", "descripcion", "unidad", "periodicidad", "categoria", "desde", "ultima_fecha", "ultimo_valor"],
        "filas": [
            [
                v["idVariable"], v["descripcion"].strip(), v.get("unidadExpresion"),
                {"D": "diaria", "M": "mensual"}.get(v.get("periodicidad"), v.get("periodicidad")),
                v.get("categoria"), v.get("primerFechaInformada"), v.get("ultFechaInformada"), v.get("ultValorInformado"),
            ]
            for v in elegidas[:MAX_VARIABLES]
        ],
    }
    avisos = []
    if len(elegidas) > MAX_VARIABLES:
        avisos.append(f"Hay {len(elegidas)} variables y se muestran {MAX_VARIABLES}. Afiná la búsqueda o la categoría.")
    if not elegidas:
        categorias = sorted({v.get("categoria") or "" for v in todas})
        avisos.append("Sin coincidencias. Categorías disponibles: " + "; ".join(c for c in categorias if c) + ".")
    if avisos:
        salida["advertencias"] = avisos
    salida["fuente"] = "Fuente: BCRA, API de Estadísticas Monetarias (https://api.bcra.gob.ar)."
    return a_texto(salida)


@servidor.tool(title="BCRA: serie de una variable", annotations=SOLO_LECTURA, structured_output=False)
async def bcra_serie(
    variable: Annotated[int, Field(description="Identificador de la variable, de bcra_variables. Por ejemplo 5 es el tipo de cambio mayorista de referencia y 30 el CER.")],
    desde: Annotated[str | None, Field(description="Fecha inicial, AAAA-MM-DD. Si se omite: un mes atrás en frecuencia diaria, trece meses en mensual.")] = None,
    hasta: Annotated[str | None, Field(description="Fecha final, AAAA-MM-DD. Si se omite, hoy.")] = None,
    frecuencia: Annotated[str, Field(description="diaria (cada valor publicado) o mensual (por mes: último valor, su fecha, promedio y días con dato).", pattern="^(diaria|mensual)$")] = "diaria",
) -> str:
    """Trae la serie de una variable del Banco Central entre dos fechas. Para un día puntual, usar la misma fecha en desde y hasta: si ese día no hubo dato (fin de semana o feriado), devuelve el último publicado antes y lo avisa."""
    todas = await bcra_listado()
    ficha = next((v for v in todas if v.get("idVariable") == variable), None)
    if not ficha:
        raise ErrorConsulta(f"El BCRA no tiene la variable {variable}. Usá bcra_variables para buscar el identificador.")
    fin = leer_fecha(hasta, "hasta") or dt.date.today()
    inicio = leer_fecha(desde, "desde") or fin - dt.timedelta(days=31 if frecuencia == "diaria" else 400)
    if inicio > fin:
        raise ErrorConsulta(f"La fecha desde ({inicio}) es posterior a hasta ({fin}).")

    puntos, url = await bcra_detalle(variable, inicio, fin)
    avisos: list[str] = []
    if not puntos:
        previos, url = await bcra_detalle(variable, inicio - dt.timedelta(days=45 if ficha.get("periodicidad") == "M" else 10), inicio)
        if previos:
            puntos = [max(previos, key=lambda p: p["fecha"])]
            avisos.append(
                f"No hay dato publicado entre {inicio} y {fin} (fin de semana, feriado o todavía sin publicar). "
                f"Se devuelve el último anterior, del {puntos[0]['fecha']}. Decí la fecha real del dato."
            )
        else:
            avisos.append(f"No hay datos entre {inicio} y {fin}. La variable tiene datos desde {ficha.get('primerFechaInformada')} hasta {ficha.get('ultFechaInformada')}.")
    puntos.sort(key=lambda p: p["fecha"], reverse=True)

    diaria = ficha.get("periodicidad") == "D"
    salida: dict[str, Any] = {
        "variable": variable,
        "descripcion": ficha["descripcion"].strip(),
        "unidad": ficha.get("unidadExpresion"),
        "periodicidad_de_la_variable": "diaria" if diaria else "mensual",
        "pedido": {"desde": inicio.isoformat(), "hasta": fin.isoformat(), "frecuencia": frecuencia},
        "valores_publicados_en_el_rango": len(puntos),
    }
    if frecuencia == "mensual" and diaria and puntos:
        salida["columnas"] = ["mes", "ultimo_valor", "fecha_del_ultimo", "promedio", "dias_con_dato"]
        salida["filas"] = resumen_mensual(puntos)
        avisos.append(
            "Para una variable diaria, «el valor del mes» puede ser el del último día hábil o el promedio, y no dan lo mismo: "
            "decí cuál usás. El primer y el último mes pueden estar incompletos según las fechas pedidas."
        )
    else:
        salida["columnas"] = ["fecha", "valor"]
        salida["filas"] = [[p["fecha"], p["valor"]] for p in puntos[:MAX_DIARIAS]]
        salida["orden"] = "primero lo más reciente"
        if len(puntos) > MAX_DIARIAS:
            avisos.append(
                f"Resultado parcial: hay {len(puntos)} valores y se muestran los {MAX_DIARIAS} más recientes. "
                "Acotá las fechas o pedí frecuencia mensual."
            )
    if avisos:
        salida["advertencias"] = avisos
    salida["fuente"] = fuente_bcra(ficha)
    salida["consulta"] = url
    return a_texto(salida)


# --------------------------------------------------------------------------- entrada


async def actualizar_catalogo() -> None:
    async with httpx2.AsyncClient(headers={"User-Agent": f"indices-ar/{VERSION}"}) as cliente:
        servicios = await descargar_catalogo(cliente)
    copia = {"actualizado": dt.date.today().isoformat(), "origen": URL_CATALOGO, "servicios": servicios}
    ARCHIVO_CATALOGO.write_text(json.dumps(copia, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"catalogo.json: {len(servicios)} servicios al {copia['actualizado']}")


def opcion(nombre: str, defecto: str) -> str:
    if nombre in sys.argv:
        i = sys.argv.index(nombre)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return defecto


def servir_http() -> None:
    """Streamable HTTP sin estado, con respuesta JSON, en /mcp. Activa la caché y el límite de remoto.py.

    Con --host 127.0.0.1 (por defecto) el SDK valida el Host contra localhost (protección contra
    DNS rebinding). Detrás de un proxy o de Lambda se usa --host 0.0.0.0: ahí el Host es el del
    dominio público y esa validación no aplica.
    """
    global _capa
    import remoto

    remoto.cargar_token_desde_ssm()
    _capa = remoto.crear_capa()
    servidor.middleware.append(_capa)
    servidor.run(
        transport="streamable-http",
        host=opcion("--host", os.environ.get("HOST", "127.0.0.1")),
        port=int(opcion("--puerto", os.environ.get("PORT", "8000"))),
        streamable_http_path="/mcp",
        stateless_http=True,
        json_response=True,
    )


def main() -> None:
    if "--version" in sys.argv:
        print(VERSION)
    elif "--actualizar-catalogo" in sys.argv:
        asyncio.run(actualizar_catalogo())
    elif "--http" in sys.argv:
        servir_http()
    else:
        servidor.run()


if __name__ == "__main__":
    main()
