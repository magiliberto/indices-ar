"""Pruebas sin red: filtros, columnas, compactado y advertencias."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402

SRV = {
    "slug": "ipc_nacional",
    "nombre": "Índice de Precios al Consumidor (IPC)",
    "area": "",
    "descripcion": "",
    "columnas": [
        {"nombre": n, "tipo": t, "descripcion": "", "motivo_de_vacio": m}
        for n, t, m in [
            ("nivel", "texto", False), ("codigo", "texto", False), ("descripcion", "texto", False),
            ("periodo", "texto", False), ("anio", "número", False), ("tipo_de_indicador", "texto", False),
            ("tipo_de_indicador_descripcion", "texto", False), ("region", "texto", False),
            ("unidad_de_medida", "texto", False), ("valor", "número", True), ("observaciones", "texto", False),
        ]
    ],
}
EXPO = {
    "slug": "comercio_exterior_exportacion_bienes", "nombre": "Exportaciones", "area": "", "descripcion": "",
    "columnas": [{"nombre": n, "tipo": "texto", "descripcion": "", "motivo_de_vacio": n == "valor_FOB"}
                 for n in ("Anio", "Periodo", "pais_cod", "valor_FOB")],
}


def test_filtro_valor_suelto_es_igualdad():
    assert server.normalizar_filtro("Nacional") == "eq.Nacional"
    assert server.normalizar_filtro("v_m") == "eq.v_m"
    assert server.normalizar_filtro(202610) == "eq.202610"


def test_filtro_respeta_operadores():
    for expr in ("gte.202601", "ilike.*carne*", "in.(Nacional,GBA)", "not.eq.GBA", "is.null"):
        assert server.normalizar_filtro(expr) == expr


def test_filtro_lista_y_comillas():
    assert server.normalizar_filtro(["Nacional", "GBA"]) == "in.(Nacional,GBA)"
    assert server.normalizar_filtro(["a,b", "c"]) == 'in.("a,b",c)'
    with pytest.raises(server.ErrorConsulta):
        server.normalizar_filtro([])


def test_columnas_sin_distinguir_mayusculas():
    assert server.columna_exacta(EXPO, "periodo") == "Periodo"
    assert server.columna_exacta(EXPO, "VALOR_fob") == "valor_FOB"
    assert server.columna_exacta(EXPO, "valor_fob_null") == "valor_FOB_null"
    with pytest.raises(server.ErrorConsulta) as e:
        server.columna_exacta(EXPO, "pais")
    assert "pais_cod" in str(e.value)


def test_armar_filtros_y_fijas():
    pares = server.armar_filtros(SRV, {"Region": "Nacional", "periodo": "gte.202601", "or": "(nivel.eq.A,nivel.eq.B)"})
    assert pares == [("region", "eq.Nacional"), ("periodo", "gte.202601"), ("or", "(nivel.eq.A,nivel.eq.B)")]
    assert server.columnas_fijas(pares) == {"region"}


def test_orden():
    assert server.armar_orden(EXPO, "periodo.desc, pais_cod") == "Periodo.desc,pais_cod.asc"
    assert server.armar_orden(SRV, None) is None
    with pytest.raises(server.ErrorConsulta):
        server.armar_orden(SRV, "periodo.abajo")


def test_tiempo_y_acompanantes():
    assert server.columna_de_tiempo(SRV) == "periodo"
    assert server.columna_de_tiempo(EXPO) == "Periodo"
    assert server.acompanantes(SRV, "descripcion") == ["nivel", "codigo"]
    assert server.acompanantes(SRV, "tipo_de_indicador") == ["tipo_de_indicador_descripcion"]
    assert server.acompanantes(SRV, "region") == []
    nomenclaturas = {"slug": "n", "columnas": [{"nombre": n} for n in ("nombre_columna", "codigo", "codigo_descripcion")]}
    assert server.acompanantes(nomenclaturas, "codigo_descripcion") == ["nombre_columna", "codigo"]


def test_url_legible():
    url = server.armar_url("ipc_nacional", [("region", "in.(Nacional,GBA)"), ("nivel", "eq.Nivel General"), ("select", "periodo,valor")])
    assert url == "https://api.indec.gob.ar/v1/ipc_nacional?region=in.(Nacional,GBA)&nivel=eq.Nivel%20General&select=periodo,valor"


def test_compactar_separa_fijo_de_variable():
    filas = [
        {"periodo": "202610", "region": "Nacional", "valor": 2.2, "valor_null": None},
        {"periodo": "202609", "region": "Nacional", "valor": 1.9, "valor_null": None},
    ]
    fijo, columnas, matriz = server.compactar(filas)
    assert fijo == {"region": "Nacional"}
    assert columnas == ["periodo", "valor"]
    assert matriz == [["202610", 2.2], ["202609", 1.9]]


def test_compactar_una_fila_no_esconde_nada():
    fijo, columnas, matriz = server.compactar([{"periodo": "202610", "valor": 2.2, "valor_null": None}])
    assert fijo == {} and columnas == ["periodo", "valor"] and matriz == [["202610", 2.2]]


def test_advertencia_resultado_parcial():
    filas = [{"periodo": "202610", "valor": 1}] * 200
    avisos = server.advertir(SRV, filas, 1485, 0)
    assert any("Resultado parcial: 1485" in a and "desde=200" in a for a in avisos)
    assert server.advertir(SRV, filas, 200, 0) == []


def test_advertencia_mezcla_de_lecturas():
    filas = [
        {"periodo": "202610", "tipo_de_indicador": "v_m", "unidad_de_medida": "Porcentaje", "valor": 2.2},
        {"periodo": "202610", "tipo_de_indicador": "Indice", "unidad_de_medida": "Unidad", "valor": 12785.9},
    ]
    avisos = server.advertir(SRV, filas, 2, 0)
    assert len(avisos) == 1 and "tipo_de_indicador (v_m, Indice)" in avisos[0] and "unidad_de_medida" in avisos[0]


def test_advertencia_signos_y_observaciones():
    filas = [
        {"Periodo": "202609", "valor_FOB": None, "valor_FOB_null": "s", "observaciones": None},
        {"Periodo": "202609", "valor_FOB": 10.0, "valor_FOB_null": None, "observaciones": "Dato provisorio"},
    ]
    avisos = server.advertir(EXPO, filas, 2, 0)
    assert any("secreto estadístico" in a and "no es un cero" in a for a in avisos)
    assert any("«Dato provisorio»" in a for a in avisos)


def test_advertencia_sin_filas():
    assert "Ninguna fila" in server.advertir(SRV, [], 0, 0)[0]


def test_salida_es_json_valido_con_una_fila_por_linea():
    obj = {"servicio": "x", "columnas": ["a", "b"], "filas": [["1", 2.5], ["dos «comillas»", None]], "fuente": "INDEC"}
    texto = server.a_texto(obj)
    assert json.loads(texto) == obj
    assert '["1", 2.5],' in [linea.strip() for linea in texto.splitlines()]
    assert '"columnas": ["a", "b"]' in texto
    assert json.loads(server.a_texto({"filas": []})) == {"filas": []}


def test_servicio_inexistente_sugiere():
    cat = server.Catalogo()
    cat.servicios = {SRV["slug"]: SRV, EXPO["slug"]: EXPO}
    assert cat.servicio("IPC_NACIONAL")["slug"] == "ipc_nacional"
    with pytest.raises(server.ErrorConsulta) as e:
        cat.servicio("ipc")
    assert "ipc_nacional" in str(e.value)


def test_token_por_archivo(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text('OTRA=1\nINDEC_API_TOKEN="abc.def"\n')
    monkeypatch.delenv("INDEC_API_TOKEN", raising=False)
    monkeypatch.setenv("INDEC_ENV_FILE", str(env))
    assert server.leer_token() == "abc.def"
    monkeypatch.setenv("INDEC_ENV_FILE", str(tmp_path / "no-existe"))
    with pytest.raises(server.ErrorConsulta):
        server.leer_token()


def test_advertencia_distingue_aperturas_de_lecturas():
    filas = [
        {"region": "Nacional", "tipo_de_indicador": "v_m", "valor": 2.2},
        {"region": "Patagonia", "tipo_de_indicador": "v_m", "valor": 2.5},
    ]
    avisos = server.advertir(SRV, filas, 2, 0)
    assert len(avisos) == 1 and "apertura distinta: region (Nacional, Patagonia)" in avisos[0]
    assert "compares" not in avisos[0]


def test_bcra_resumen_mensual():
    detalle = [
        {"fecha": "2026-10-01", "valor": 1523.0}, {"fecha": "2026-09-30", "valor": 1517.0},
        {"fecha": "2026-09-29", "valor": 1523.0}, {"fecha": "2026-09-01", "valor": 1500.0},
    ]
    assert server.resumen_mensual(detalle) == [
        ["2026-10", 1523.0, "2026-10-01", 1523.0, 1],
        ["2026-09", 1517.0, "2026-09-30", 1513.3333, 3],
    ]


def test_bcra_busqueda_sin_acentos_y_fechas():
    assert server.sin_acentos("Índice para Contratos de Locación") == "indice para contratos de locacion"
    assert str(server.leer_fecha("2026-09-30", "desde")) == "2026-09-30"
    assert server.leer_fecha("", "desde") is None
    with pytest.raises(server.ErrorConsulta) as e:
        server.leer_fecha("30/09/2026", "desde")
    assert "AAAA-MM-DD" in str(e.value)
