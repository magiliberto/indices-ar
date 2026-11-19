"""Pruebas sin red del modo remoto: caché, límite de uso y registros (remoto.py)."""
import asyncio
import json
import sys
import zlib
from pathlib import Path

import pytest
from mcp import Client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import remoto  # noqa: E402
import server  # noqa: E402


class Reloj:
    def __init__(self) -> None:
        self.t = 1_800_000_000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def capa(monkeypatch):
    """Activa la capa en memoria con un reloj controlable y deja el servidor como estaba al terminar."""
    reloj = Reloj()
    lineas: list[dict] = []
    c = remoto.Capa(remoto.Memoria(reloj), remoto.Limites(), reloj=reloj, registrar=lineas.append)
    c.lineas = lineas
    c.reloj_fake = reloj
    monkeypatch.setattr(server, "_capa", c)
    monkeypatch.setattr(server, "_bcra", None)  # memo propio del proceso (anterior a la capa)
    server.servidor.middleware.append(c)
    yield c
    server.servidor.middleware.remove(c)


def contar_llamadas(monkeypatch, nombre, respuesta):
    llamadas = []

    async def falso(*args):
        llamadas.append(args)
        return respuesta

    monkeypatch.setattr(server, nombre, falso)
    return llamadas


# --------------------------------------------------------------------------- caché


def test_diez_pedidos_iguales_un_solo_pedido_al_indec(capa, monkeypatch):
    llamadas = contar_llamadas(monkeypatch, "pedir_al_indec", [[{"periodo": "202610", "valor": 2.3}], 1])

    async def correr():
        return [await server.pedir("ipc_nacional", [("region", "eq.Nacional")]) for _ in range(10)]

    resultados = asyncio.run(correr())
    assert len(llamadas) == 1
    assert all(r == resultados[0] for r in resultados)
    assert resultados[0][2].startswith("https://api.indec.gob.ar/v1/ipc_nacional")


def test_la_cache_del_indec_vence_a_la_hora_y_la_del_bcra_a_los_15_minutos(capa, monkeypatch):
    indec = contar_llamadas(monkeypatch, "pedir_al_indec", [[], 0])
    bcra = contar_llamadas(monkeypatch, "pedir_al_bcra", {"results": []})

    async def una_vuelta():
        await server.pedir("ipc_nacional", [])
        await server.bcra_pedir("/5", [("desde", "2026-09-01")])

    asyncio.run(una_vuelta())
    capa.reloj_fake.t += 15 * 60 + 1
    asyncio.run(una_vuelta())
    assert (len(indec), len(bcra)) == (1, 2)
    capa.reloj_fake.t += 3600
    asyncio.run(una_vuelta())
    assert (len(indec), len(bcra)) == (2, 3)


def test_sin_capa_no_hay_cache(monkeypatch):
    monkeypatch.setattr(server, "_capa", None)
    llamadas = contar_llamadas(monkeypatch, "pedir_al_indec", [[], 0])

    async def correr():
        for _ in range(3):
            await server.pedir("ipc_nacional", [])

    asyncio.run(correr())
    assert len(llamadas) == 3


def test_los_errores_no_se_guardan(capa, monkeypatch):
    intentos = []

    async def falla(*args):
        intentos.append(args)
        raise server.ErrorConsulta("El INDEC respondió con error 500")

    monkeypatch.setattr(server, "pedir_al_indec", falla)

    async def correr():
        for _ in range(2):
            with pytest.raises(server.ErrorConsulta):
                await server.pedir("ipc_nacional", [])

    asyncio.run(correr())
    assert len(intentos) == 2


def test_cache_caida_no_tumba_el_servicio(monkeypatch):
    class Roto:
        async def leer(self, clave):
            raise RuntimeError("sin red")

        async def guardar(self, clave, valor, ttl):
            raise RuntimeError("sin red")

        async def sumar(self, clave, ttl):
            raise RuntimeError("sin red")

    lineas = []
    monkeypatch.setattr(server, "_capa", remoto.Capa(Roto(), registrar=lineas.append))
    contar_llamadas(monkeypatch, "pedir_al_indec", [[{"valor": 1}], 1])
    filas, total, _ = asyncio.run(server.pedir("ipc_nacional", []))
    assert filas == [{"valor": 1}] and total == 1
    assert {l["operacion"] for l in lineas if l["evento"] == "error_cache"} == {"leer", "sumar", "guardar"}


# --------------------------------------------------------------------------- límites


def test_limite_global_error_legible_y_el_servicio_sigue(capa, monkeypatch):
    capa.limites = remoto.Limites(global_por_minuto=3)
    contar_llamadas(monkeypatch, "pedir_al_bcra", {"results": [], "metadata": {"resultset": {"count": 0}}})

    async def correr():
        async with Client(server.servidor) as c:
            resultados = [await c.call_tool("bcra_variables", {}) for _ in range(4)]
            herramientas = await c.list_tools()  # el servidor sigue respondiendo
            capa.reloj_fake.t += 61
            despues = await c.call_tool("bcra_variables", {})
            return resultados, herramientas, despues

    resultados, herramientas, despues = asyncio.run(correr())
    assert [r.is_error for r in resultados] == [False, False, False, True]
    assert resultados[3].content[0].text == "Límite de uso alcanzado; probá de nuevo en un minuto"
    assert len(herramientas.tools) == 6
    assert not despues.is_error
    assert [l["estado"] for l in capa.lineas if l["evento"] == "llamada"] == ["ok", "ok", "ok", "limite", "ok"]


def test_superado_el_limite_no_escribe_mas_en_el_backend(capa):
    capa.limites = remoto.Limites(global_por_minuto=2)
    sumas = []
    original = capa.backend.sumar

    async def contar(clave, ttl):
        sumas.append(clave)
        return await original(clave, ttl)

    capa.backend.sumar = contar

    async def correr():
        for _ in range(2):
            await capa.chequear_llamada(None)
        for _ in range(20):
            with pytest.raises(remoto.LimiteAlcanzado):
                await capa.chequear_llamada(None)

    capa.reloj_fake.t = 60 * 30_000_000  # principio de un minuto
    asyncio.run(correr())
    assert len(sumas) == 6  # 2 llamadas buenas + la que supera, dos contadores cada una
    capa.reloj_fake.t += 60
    asyncio.run(capa.chequear_llamada(None))  # minuto nuevo: vuelve a atender


def test_limite_diario(capa):
    capa.limites = remoto.Limites(global_por_minuto=1000, global_por_dia=5)

    async def correr():
        errores = 0
        for i in range(8):
            capa.reloj_fake.t += 61  # cada llamada en un minuto distinto
            try:
                await capa.chequear_llamada(None)
            except remoto.LimiteAlcanzado:
                errores += 1
        return errores

    assert asyncio.run(correr()) == 3


def test_limite_por_ip_separa_direcciones(capa):
    capa.limites = remoto.Limites(ip_por_minuto=2)

    async def correr():
        for _ in range(2):
            await capa.chequear_llamada("203.0.113.1")
        with pytest.raises(remoto.LimiteAlcanzado):
            await capa.chequear_llamada("203.0.113.1")
        await capa.chequear_llamada("203.0.113.2")

    asyncio.run(correr())
    claves = [k for k in capa.backend._datos if k.startswith("l#ip#")]
    assert claves and not any("203.0.113" in k for k in claves)  # la IP no queda en la clave


def test_limite_al_indec_cuenta_despues_de_la_cache(capa, monkeypatch):
    capa.limites = remoto.Limites(indec_por_minuto=2)
    llamadas = contar_llamadas(monkeypatch, "pedir_al_indec", [[], 0])

    async def correr():
        await server.pedir("ipc_nacional", [("periodo", "eq.1")])
        await server.pedir("ipc_nacional", [("periodo", "eq.2")])
        for _ in range(5):  # desde la caché: no cuentan
            await server.pedir("ipc_nacional", [("periodo", "eq.1")])
        with pytest.raises(server.ToolError) as e:
            await server.pedir("ipc_nacional", [("periodo", "eq.3")])
        return str(e.value)

    assert asyncio.run(correr()) == remoto.MENSAJE_LIMITE
    assert len(llamadas) == 2


# --------------------------------------------------------------------------- registros


def test_registro_sin_texto_de_consulta(capa, monkeypatch):
    contar_llamadas(monkeypatch, "pedir_al_bcra", {"results": [], "metadata": {"resultset": {"count": 0}}})

    async def correr():
        async with Client(server.servidor) as c:
            await c.call_tool("bcra_variables", {"buscar": "consulta-privada-xyz"})
            server._bcra = None  # como un contenedor nuevo: sólo queda la caché compartida
            await c.call_tool("bcra_variables", {"buscar": "consulta-privada-xyz"})

    asyncio.run(correr())
    llamadas = [l for l in capa.lineas if l["evento"] == "llamada"]
    assert "consulta-privada-xyz" not in json.dumps(capa.lineas)
    assert llamadas[0]["herramienta"] == "bcra_variables" and llamadas[0]["cache"] == "miss"
    assert llamadas[1]["cache"] == "hit"
    assert set(llamadas[0]) == {"evento", "herramienta", "estado", "cache", "desde_cache", "pedidos_afuera", "ms"}


# --------------------------------------------------------------------------- DynamoDB


class DynamoFalso:
    def __init__(self):
        self.items = {}

    def get_item(self, TableName, Key, ConsistentRead):
        item = self.items.get(Key["pk"]["S"])
        return {"Item": item} if item else {}

    def put_item(self, TableName, Item):
        self.items[Item["pk"]["S"]] = Item

    def update_item(self, TableName, Key, ExpressionAttributeValues, ReturnValues, **_):
        item = self.items.setdefault(Key["pk"]["S"], {"pk": Key["pk"], "n": {"N": "0"}, "ttl": ExpressionAttributeValues[":vence"]})
        item["n"] = {"N": str(int(item["n"]["N"]) + 1)}
        return {"Attributes": {"n": item["n"], "ttl": item["ttl"]}}


def test_backend_dynamodb_ida_y_vuelta():
    reloj = Reloj()
    cliente = DynamoFalso()
    d = remoto.Dynamo("tabla", cliente=cliente, reloj=reloj)

    async def correr():
        await d.guardar("c#x", [[{"región": "Nacional", "valor": 1.5}], 10], 60)
        leido = await d.leer("c#x")
        cuentas = [await d.sumar("l#a", 120) for _ in range(3)]
        reloj.t += 61
        vencido = await d.leer("c#x")
        return leido, cuentas, vencido

    leido, cuentas, vencido = asyncio.run(correr())
    assert leido == [[{"región": "Nacional", "valor": 1.5}], 10]
    assert cuentas == [1, 2, 3]
    assert vencido is None  # el TTL de DynamoDB borra tarde: la lectura descarta lo vencido
    assert isinstance(cliente.items["c#x"]["v"]["B"], bytes)
    assert json.loads(zlib.decompress(cliente.items["c#x"]["v"]["B"]))[1] == 10


def test_backend_dynamodb_no_guarda_lo_que_no_entra():
    cliente = DynamoFalso()
    d = remoto.Dynamo("tabla", cliente=cliente)
    import os

    enorme = [os.urandom(200).hex() for _ in range(3000)]  # ~1,2 MB, no comprime
    asyncio.run(d.guardar("c#grande", enorme, 60))
    assert "c#grande" not in cliente.items
