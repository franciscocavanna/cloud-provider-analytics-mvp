# Cloud Provider Analytics — MVP técnico (Segunda Entrega)

Pipeline `landing → Bronze → Silver → Gold → Serving (Cassandra/AstraDB)` con **PySpark + Structured Streaming** en Google Colab.

**Grupo:** Francisco Cavanna · Pedro Welsh · Tommy Wasserman

Demuestra un end-to-end mínimo funcionando: ingesta batch de maestros + streaming de eventos a Bronze, limpieza y reglas de calidad en Silver, un mart de FinOps en Gold, y serving en una tabla de AstraDB modelada query-first con 2 consultas resueltas.

---

## Estructura del repo

```
Cloud_Provider_Analytics_MVP.ipynb   # notebook principal (corre de punta a punta)
README.md                            # este archivo
DECISIONES.md                        # log de decisiones y trade-offs
queries.cql                          # DDL del keyspace/tabla + las 2 queries del MVP
architecture.svg                     # diagrama de arquitectura actualizado
```

Estructura de zonas que genera el pipeline (Parquet, en Drive):

```
datalake/
  landing/        # CSV + JSONL originales (read-only, no se tocan)
  bronze/         # tipificado + ingest_ts/source_file + dedup; eventos particionados por usage_date, billing por month
  silver/         # limpio + join orgs + features + flags; particionado por usage_date
  gold/           # org_daily_usage_by_service (org x service x day); particionado por usage_date
  quarantine/     # registros que no pasaron las reglas de calidad
  _checkpoints/   # checkpoint del stream (resiliencia de sesión Colab)
```

---

## Quickstart

### Lo que ya está configurado en el notebook (no hace falta tocar)
- `ASTRA_TOKEN` — token de la base (hardcodeado, es una DB free tier de un TP).
- `DB_ID` = `de83959c-7447-4099-bb0f-decf663f92b0` — ID de la base en AstraDB.
- `KEYSPACE` = `cloud_analytics`.
- El **Secure Connect Bundle se descarga solo** desde el notebook (celda que le pide el link a la API de Astra). No hay que bajarlo a mano del navegador.

### Pasos en Colab
1. Subí `cloud_provider_challenge_dataset_v1.zip` a tu Google Drive (en `MyDrive/`).
   - Si lo dejás en otra ruta, ajustá `ZIP_PATH` en la celda "Cargar el dataset a Landing".
2. Abrí `Cloud_Provider_Analytics_MVP.ipynb` en Colab.
3. **Runtime → Run all.**
   - Te va a pedir autorizar el montaje de Google Drive: aceptá.
   - Si los joins/streaming se quedan sin RAM: Runtime → Change runtime type → **High-RAM**, y volvé a correr.

### Qué imprime cada parte (esto es la evidencia para la entrega)
- **Streaming → Bronze:** `eventos RAW = 43200` y `eventos en Bronze = 43200` (sin pérdida) + conteo por schema_version.
- **Silver:** `clean / quarantine`, imputaciones de unit, spikes, y muestras de quarantine.
- **Gold:** filas del mart + check de consistencia de costo (Silver = Gold).
- **AstraDB:** `bundle descargado: ... bytes`, `conectado a AstraDB`, `filas upserteadas: 11050`, resultados de Query #1 y Top-N de Query #2.
- **Idempotencia:** conteos antes/después del re-run (deben coincidir → no duplica).

---

## Resumen de resultados (dataset provisto)

| Etapa | Resultado |
|---|---|
| Eventos raw → Bronze | 43.200 → 43.200 (sin pérdida) |
| Schema versions | v1: 10.800 · v2: 32.400 |
| Silver clean / quarantine | 42.989 / 211 |
| Imputaciones de `unit` | 2.038 |
| Spikes de costo (flag de anomalía) | 54 |
| Filas Gold (org×servicio×día) | 11.050 |
| Costo total (check Silver=Gold) | 148.351,58 USD |

Las decisiones de diseño, los trade-offs y un par de cosas a tener en cuenta para la entrega final están en **[DECISIONES.md](DECISIONES.md)**.
