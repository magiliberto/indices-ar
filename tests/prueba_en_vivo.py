"""Prueba de punta a punta contra la API real: levanta el servidor por stdio, como lo hace Claude,
y recorre los servicios. Necesita INDEC_API_TOKEN o INDEC_ENV_FILE en el entorno.

    uv run --with 'mcp>=2.3,<3' --with httpx2 python tests/prueba_en_vivo.py

Contra un servidor remoto (modo --http), sin token local: el token lo tiene el servidor.

    uv run --with 'mcp>=2.3,<3' --with httpx2 python tests/prueba_en_vivo.py --url https://.../mcp

Con --solo-bcra se saltean las herramientas del INDEC (sirve para probar sin token).
"""
import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import Client, StdioServerParameters

RAIZ = Path(__file__).resolve().parent.parent
fallas: list[str] = []


def es_error(r) -> bool:
    return bool(getattr(r, "is_error", getattr(r, "isError", False)))


def texto(r) -> str:
    return r.content[0].text if r.content else ""


def chequear(condicion: bool, etiqueta: str, detalle: str = "") -> None:
    if not condicion:
        fallas.append(f"{etiqueta} {detalle}".strip())
    print(("  ok   " if condicion else "  FALLA ") + etiqueta + (f" — {detalle}" if detalle and not condicion else ""))


async def llamar(c: Client, herramienta: str, **argumentos):
    await asyncio.sleep(0.15)
    return await c.call_tool(herramienta, argumentos)


async def main() -> None:
    args = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    args.add_argument("--url", help="URL del servidor remoto, terminada en /mcp. Sin esto, levanta server.py por stdio.")
    args.add_argument("--solo-bcra", action="store_true", help="Saltea las herramientas del INDEC.")
    opciones = args.parse_args()
    if opciones.url:
        destino = opciones.url
    else:
        entorno = {k: os.environ[k] for k in ("INDEC_API_TOKEN", "INDEC_ENV_FILE") if os.environ.get(k)}
        if not entorno and not opciones.solo_bcra:
            sys.exit("Definí INDEC_API_TOKEN o INDEC_ENV_FILE antes de correr la prueba (o usá --solo-bcra).")
        destino = StdioServerParameters(command="uv", args=["run", "--quiet", str(RAIZ / "server.py")], env=entorno)
    async with Client(destino) as c:
        herramientas = (await c.list_tools()).tools
        nombres = sorted(t.name for t in herramientas)
        chequear(nombres == ["bcra_serie", "bcra_variables", "indec_buscar_valores", "indec_consultar", "indec_describir_servicio", "indec_listar_servicios"], "seis herramientas", str(nombres))
        chequear("indec_describir_servicio" in (c.instructions or ""), "instrucciones de lectura publicadas")
        if not opciones.solo_bcra:
            await recorrer_indec(c)
        await recorrer_bcra(c)

    print(f"\n{'TODO BIEN' if not fallas else str(len(fallas)) + ' FALLAS'}")
    for f in fallas:
        print(" -", f)
    sys.exit(1 if fallas else 0)


async def recorrer_indec(c: Client) -> None:
    r = await llamar(c, "indec_listar_servicios")
    listado = json.loads(texto(r))
    servicios = [s["servicio"] for s in listado["servicios"]]
    chequear(len(servicios) >= 19, f"listado con {len(servicios)} servicios ({listado['origen_del_listado']})")

    for slug in servicios:
        r = await llamar(c, "indec_describir_servicio", servicio=slug)
        if es_error(r):
            chequear(False, f"describir {slug}", texto(r)[:200])
            continue
        ficha = json.loads(texto(r))
        columnas = [col["columna"] for col in ficha["columnas"]]
        r1 = await llamar(c, "indec_consultar", servicio=slug, limite=5)
        r2 = await llamar(c, "indec_consultar", servicio=slug, columnas=columnas, limite=5)
        bien = not es_error(r1) and not es_error(r2)
        detalle = "" if bien else (texto(r1) if es_error(r1) else texto(r2))[:220]
        if bien:
            d1 = json.loads(texto(r1))
            bien = d1["filas_devueltas"] == 5 or d1["filas_que_cumplen_el_filtro"] < 5
            bien = bien and d1["fuente"].startswith("Fuente: INDEC") and d1["consulta"].startswith("https://api.indec.gob.ar/v1/")
        periodo = ficha.get("periodo", {})
        chequear(bien, f"{slug}: {ficha['total_de_filas']} filas, {periodo.get('desde', '-')} a {periodo.get('hasta', '-')}", detalle)

    print("casos de lectura")
    r = await llamar(c, "indec_consultar", servicio="ipc_nacional",
                     filtros={"nivel": "Nivel General", "region": "Nacional", "tipo_de_indicador": "v_m"}, limite=1)
    d = json.loads(texto(r))
    chequear(d["filas_devueltas"] == 1 and "advertencias" in d and "Resultado parcial" in d["advertencias"][0],
             "inflación mensual nacional: una fila y aviso de que hay más períodos", texto(r)[:300])
    print("       ", dict(zip(d["columnas"], d["filas"][0])))

    r = await llamar(c, "indec_consultar", servicio="IPC_NACIONAL", filtros={"NIVEL": "Nivel General", "periodo": "202610"}, columnas=["region", "valor"])
    d = json.loads(texto(r))
    chequear(any("mezcla lecturas" in a for a in d.get("advertencias", [])) and "tipo_de_indicador" in d["columnas"],
             "sin fijar el tipo de indicador: lo agrega y avisa la mezcla", texto(r)[:300])

    r = await llamar(c, "indec_consultar", servicio="comercio_exterior_exportacion_bienes", filtros={"periodo": "202609"}, limite=300)
    d = json.loads(texto(r))
    chequear(any("secreto estadístico" in a for a in d.get("advertencias", [])), "exportaciones: avisa valores ocultos por secreto estadístico",
             str(d.get("advertencias"))[:300])

    r = await llamar(c, "indec_consultar", servicio="mercado_trabajo_eph", limite=2)
    d = json.loads(texto(r))
    chequear(any("provisorio" in a.lower() for a in d.get("advertencias", [])), "empleo: avisa dato provisorio", str(d.get("advertencias"))[:300])

    r = await llamar(c, "indec_buscar_valores", servicio="ipc_nacional", columna="descripcion", contiene="carne")
    d = json.loads(texto(r))
    chequear(d["valores_distintos"] >= 1 and "codigo" in d["columnas"], f"buscar «carne» en el IPC: {d['valores_distintos']} valores con su código")

    r = await llamar(c, "indec_buscar_valores", servicio="comercio_exterior_nomenclaturas", columna="codigo_descripcion", contiene="brasil")
    d = json.loads(texto(r))
    chequear(d["valores_distintos"] >= 1, f"buscar «brasil» en nomenclaturas: {d['filas'][:2]}")

    r = await llamar(c, "indec_consultar", servicio="ipc", filtros={"region": "Nacional"})
    chequear(es_error(r) and "ipc_nacional" in texto(r), "servicio mal escrito: error con sugerencia", texto(r)[:200])
    r = await llamar(c, "indec_consultar", servicio="ipc_nacional", filtros={"provincia": "Mendoza"})
    chequear(es_error(r) and "region" in texto(r), "columna inexistente: error que lista las columnas", texto(r)[:200])
    r = await llamar(c, "indec_consultar", servicio="ipc_nacional", filtros={"region": "Marte"})
    d = json.loads(texto(r))
    chequear(d["filas_devueltas"] == 0 and "Ninguna fila" in d["advertencias"][0], "filtro sin resultados: lo dice en vez de devolver vacío")
    r = await llamar(c, "indec_consultar", servicio="ipc_nacional", filtros={"periodo": "gte.abc.def"}, limite=1)
    chequear(not es_error(r) or "INDEC" in texto(r), "operador raro: no rompe el servidor")


async def recorrer_bcra(c: Client) -> None:
    print("BCRA (sin token)")
    d = json.loads(texto(await llamar(c, "bcra_variables")))
    ids = [f[0] for f in d["filas"]]
    chequear(5 in ids and 30 in ids and 31 in ids, f"principales variables: {d['variables_encontradas']}, con dólar mayorista, CER y UVA")
    d = json.loads(texto(await llamar(c, "bcra_variables", buscar="locacion")))
    chequear(any(f[0] == 40 for f in d["filas"]), "búsqueda sin acentos encuentra el índice de locación")
    d = json.loads(texto(await llamar(c, "bcra_serie", variable=5, desde="2026-09-01", hasta="2026-09-30")))
    chequear(d["valores_publicados_en_el_rango"] >= 15 and d["filas"][0][0] == "2026-09-30" and d["fuente"].startswith("Fuente: BCRA"),
             f"dólar mayorista de septiembre: {d['valores_publicados_en_el_rango']} días, cierre {d['filas'][0][1]}")
    d = json.loads(texto(await llamar(c, "bcra_serie", variable=5, desde="2026-10-04", hasta="2026-10-04")))
    chequear(d["filas"][0][0] == "2026-10-02" and "último anterior" in d["advertencias"][0], "día sin dato (domingo): devuelve el viernes y lo avisa")
    d = json.loads(texto(await llamar(c, "bcra_serie", variable=5, desde="2026-03-01", hasta="2026-09-30", frecuencia="mensual")))
    chequear(len(d["filas"]) == 7 and d["columnas"][1] == "ultimo_valor" and "promedio" in d["advertencias"][0], "resumen mensual: siete meses, con último día y promedio")
    d = json.loads(texto(await llamar(c, "bcra_serie", variable=5, desde="2015-01-01", hasta="2026-09-30")))
    chequear(len(d["filas"]) == 400 and "Resultado parcial" in d["advertencias"][0], f"serie larga ({d['valores_publicados_en_el_rango']} días): recorta y avisa")
    r = await llamar(c, "bcra_serie", variable=999999)
    chequear(es_error(r) and "bcra_variables" in texto(r), "variable inexistente: error con indicación", texto(r)[:200])
    r = await llamar(c, "bcra_serie", variable=5, desde="30/09/2026")
    chequear(es_error(r) and "AAAA-MM-DD" in texto(r), "fecha mal escrita: error con el formato", texto(r)[:200])


asyncio.run(main())
