# indices-ar

Servidor MCP para consultar desde Claude las estadísticas oficiales del INDEC, usando el [Catálogo API-INDEC](https://api.indec.gob.ar), y las variables del Banco Central, usando su API de Estadísticas Monetarias (`api.bcra.gob.ar`).

No es un producto oficial del INDEC ni del BCRA. El catálogo del INDEC está en fase beta y puede cambiar.

## Qué resuelve

La API del INDEC publica cada dato en varias filas: una por tipo de indicador (índice, variación mensual, interanual, acumulada), por región y por serie. Un modelo que elige mal la fila contesta un número equivocado con la misma seguridad que uno correcto.

Este servidor le entrega al modelo el dato junto con lo necesario para leerlo bien:

- **Qué quedó fijo y qué cambia:** cada respuesta separa lo que es igual en todas las filas de lo que varía.
- **La unidad viaja con el número:** si la consulta no fija el tipo de indicador, la unidad, la serie o la región, esas columnas se agregan solas.
- **Advertencias de lectura:** resultado parcial, lecturas mezcladas, datos provisorios y valores ocultos por secreto estadístico.
- **Fuente y consulta exacta:** cada respuesta trae la cita y la URL consultada, para verificar el número a mano.

## Herramientas

| Herramienta | Para qué |
|---|---|
| `indec_listar_servicios` | Ver los servicios disponibles: IPC, EMAE, salarios, pobreza, empleo, comercio exterior, población y otros. |
| `indec_describir_servicio` | Conocer un servicio antes de consultarlo: columnas, períodos que cubre y valores posibles. |
| `indec_buscar_valores` | Encontrar el valor o el código exacto para un filtro, por ejemplo qué rubros del IPC contienen "carne". |
| `indec_consultar` | Traer los datos con filtros, orden y paginado. |
| `bcra_variables` | Ver las variables del Banco Central: tipo de cambio mayorista y minorista, CER, UVA, índice para contratos de locación, tasas, reservas y agregados monetarios. |
| `bcra_serie` | Traer la serie de una variable entre dos fechas, día por día o resumida por mes. |

Todas son de solo lectura. Las cuatro primeras son del INDEC y las dos últimas del BCRA.

## Usarlo sin instalar nada

Lo agregás a Claude y listo: no hace falta registrarse ni instalar nada.

**En Claude (web o escritorio):**

1. En un chat, tocá **+** → **Plugins** → **Administrar plugins** → **Agregar** → **Agregar conector personalizado**.
2. Nombre: `Indices AR`. Dirección: `https://indices.mauriciogiliberto.com/mcp`. Sin inicio de sesión.
3. En cada chat donde lo quieras usar, prendelo en **+** → **Plugins**.

El plan gratuito de Claude permite un conector personalizado; los pagos, varios.

**En Claude Code:**

```bash
claude mcp add --transport http indices-ar https://indices.mauriciogiliberto.com/mcp
```

**Qué conviene saber:**

- Si la búsqueda web está activa, para datos «de noticia» como la inflación del mes Claude a veces responde con la prensa. Si querés el dato oficial con su fuente, pedile que use Indices AR.
- Es un servicio compartido con límite de uso. Si se pasa, la respuesta dice «Límite de uso alcanzado; probá de nuevo en un minuto». Un dato recién publicado puede tardar hasta una hora en aparecer.
- El servidor no guarda tus preguntas.
- Puede dejar de estar disponible sin aviso. Para uso intensivo, instalalo con tu propio token (abajo).

## Requisitos

- [uv](https://docs.astral.sh/uv/) instalado. Las dependencias se resuelven solas al primer arranque.
- Un token del INDEC. Es gratuito y personal: se obtiene registrándose en <https://api.indec.gob.ar> y vence a los 12 meses. Las herramientas del BCRA funcionan sin token.

El token se pasa de una de dos formas:

- `INDEC_API_TOKEN`: el token.
- `INDEC_ENV_FILE`: la ruta a un archivo `.env` que tenga una línea `INDEC_API_TOKEN=...`. Sirve para no escribir el token en la configuración de Claude.

## Instalación

### Claude Code

```bash
claude mcp add indices-ar --scope user \
  -e INDEC_ENV_FILE=/ruta/a/tu/.env \
  -- uv run --quiet /ruta/a/indices-ar/server.py
```

### Claude Desktop

En `claude_desktop_config.json` (Configuración → Desarrollador → Editar configuración):

```json
{
  "mcpServers": {
    "indices-ar": {
      "command": "/ruta/a/uv",
      "args": ["run", "--quiet", "/ruta/a/indices-ar/server.py"],
      "env": { "INDEC_ENV_FILE": "/ruta/a/tu/.env" }
    }
  }
}
```

Claude Desktop necesita la ruta completa a `uv` (se obtiene con `which uv`). Después de guardar hay que reiniciar la aplicación.

### Modo remoto (HTTP)

El mismo servidor, con las mismas herramientas, puede atender por HTTP para usarlo como conector remoto:

```bash
INDEC_ENV_FILE=/ruta/a/tu/.env uv run server.py --http --puerto 8000
# queda en http://127.0.0.1:8000/mcp
```

Usa Streamable HTTP sin estado y responde en JSON. En este modo se activan dos cosas que por stdio no corren:

- **Caché** por consulta: una hora para los datos del INDEC, 15 minutos para el BCRA y seis horas para el catálogo y la lista de variables. Por defecto vive en memoria; con `INDICES_AR_CACHE=dynamodb` e `INDICES_AR_TABLA=<tabla>` se comparte en una tabla de DynamoDB (necesita `boto3`).
- **Límite de uso:** 60 llamadas por minuto y 3000 por día en total, 30 por minuto por IP y 20 pedidos por minuto al INDEC después de la caché. Se ajustan con `INDICES_AR_LIMITE_MINUTO`, `INDICES_AR_LIMITE_DIA`, `INDICES_AR_LIMITE_IP_MINUTO` e `INDICES_AR_LIMITE_INDEC_MINUTO`. Al superarlo, la herramienta contesta «Límite de uso alcanzado; probá de nuevo en un minuto» y el servidor sigue atendiendo.

Cada llamada deja una línea JSON en la salida de errores con la herramienta, el servicio o la variable, si salió de la caché, la duración y el estado. No guarda el texto de la consulta, la IP ni el token.

Con `--host 127.0.0.1` (el valor por defecto) el servidor sólo acepta pedidos dirigidos a `localhost`. Detrás de un proxy o de un servicio en la nube hay que usar `--host 0.0.0.0`.

## Preguntas para probar

- ¿Cuánto fue la inflación de octubre de 2026 a nivel nacional y en la Patagonia?
- ¿Cómo evolucionó el índice de salarios del sector privado registrado frente al IPC en los últimos doce meses?
- ¿Cuáles fueron los cinco principales destinos de las exportaciones argentinas en septiembre de 2026?
- ¿Cuánta población se proyecta para Mendoza en 2030?
- ¿Cuánto varió el tipo de cambio mayorista entre el 31 de marzo y el 30 de septiembre de 2026, y cuánto el CER?
- Tengo una tarifa que se ajusta 50% por salarios del sector privado registrado, 30% por precios mayoristas y 20% por gas oil. ¿Cuánto corresponde ajustar entre marzo y septiembre de 2026?

Conviene repetir alguna sin dar la región ni el tipo de variación, para ver qué supuesto toma el modelo y si lo declara.

## Límites

- **Filas por consulta:** la API devuelve hasta 1.000 filas por pedido. El servidor informa cuántas cumplen el filtro y cómo seguir.
- **Uso:** el INDEC aplica límites de uso que no publica. El servidor reintenta ante un rechazo temporal, pero no está pensado para descargas masivas.
- **Tipo de cambio:** el BCRA publica el mayorista de referencia y el minorista. No es la cotización del Banco Nación ni un dólar financiero. Las series diarias no tienen dato en fines de semana ni feriados; el servidor devuelve el último día hábil anterior y lo avisa.
- **Cobertura:** solo responde con lo que está en el catálogo. La lista de servicios se lee en línea; si el portal no responde, usa la copia de `catalogo.json`.
- **Redondeo:** según el INDEC, los totales por suma pueden no coincidir por redondeo, y una variación calculada desde los índices puede diferir de la publicada en los informes técnicos.

## Desarrollo

```bash
# Pruebas sin red
uv run --with 'mcp>=2.3,<3' --with httpx2 --with pytest pytest -q tests/test_unit.py tests/test_remoto.py

# Prueba de punta a punta contra la API real (necesita el token)
INDEC_ENV_FILE=/ruta/a/tu/.env uv run --with 'mcp>=2.3,<3' --with httpx2 python tests/prueba_en_vivo.py

# La misma prueba contra un servidor remoto (el token lo tiene el servidor)
uv run --with 'mcp>=2.3,<3' --with httpx2 python tests/prueba_en_vivo.py --url https://tu-servidor/mcp

# Sólo las herramientas del BCRA, sin token
uv run --with 'mcp>=2.3,<3' --with httpx2 python tests/prueba_en_vivo.py --solo-bcra

# Regenerar la copia local del catálogo
uv run server.py --actualizar-catalogo
```

## Fuente de los datos

Instituto Nacional de Estadística y Censos (INDEC), Catálogo API-INDEC, y Banco Central de la República Argentina (BCRA), API de Estadísticas Monetarias. Al usar los datos hay que citar la fuente de manera visible; cada respuesta del servidor trae la cita armada.

## Autor y licencia

Hecho por Mauricio Giliberto ([mauriciogiliberto.com](https://mauriciogiliberto.com)). Licencia MIT.
