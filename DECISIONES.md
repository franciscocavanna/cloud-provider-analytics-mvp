# Log de decisiones y trade-offs

Breve registro de las decisiones que tomamos en el MVP y por qué. Algunas se validaron recién al correr el pipeline contra los datos reales.

## Patrón: Lambda con serving desacoplado
Mantenemos Lambda (batch para maestros/facturación, streaming para eventos). Para el serving **desacoplamos**: el streaming aterriza los eventos crudos en Bronze, y un pase **batch** arma Silver → Gold → Cassandra.

*Por qué:* agregar dentro del Structured Streaming y escribir a Cassandra te mete en el lío de los output modes (append necesita ventana + watermark finalizado; update + foreachBatch para SUMs entre micro-batches es frágil). Desacoplando, la agregación de Gold es un batch determinístico y la idempotencia queda limpia. Sigue siendo Lambda: el speed layer ingesta rápido, el serving se computa en batch.

## Watermark ancho (esto fue un bug real)
Los 120 archivos JSONL están fragmentados en **orden aleatorio**: un mismo micro-lote trae eventos de julio y agosto mezclados. Con un watermark de 2h, apenas el stream veía un evento del 31/08 el watermark saltaba a esa fecha y descartaba como *late data* todos los de julio → **perdíamos ~2/3 de los eventos** (43.200 → 14.438).

*Fix:* el watermark tiene que ser más ancho que el desorden máximo, que acá es ~el span completo (~60 días). Un watermark asume que el tiempo-de-evento sigue al de llegada (stream ordenado); este feed no lo está. En un stream real ordenado, 2h alcanzaría y solo los stragglers muy tardíos se descartarían. Con el watermark de 60 días: 43.200 → 43.200, sin pérdida.

## `value` como string en Bronze, casteo en Silver
`value` a veces viene número y a veces string (1.309 strings, 877 nulls). Lo dejamos como **string en Bronze** (zona raw-estándar) y lo casteamos a double **con fallback** en Silver, marcando `value_cast_failed`. En esta data los strings son numéricos limpios ("120.0"), así que el flag da 0, pero la regla está implementada por si aparece basura.

## 3 reglas de calidad + por qué imputamos `unit`
- **R1** — `event_id` no nulo y único (el dedup lo hace Bronze por `dropDuplicates`).
- **R2** — `cost_usd_increment ≥ -0.01`: negativos por debajo de -0.01 → **quarantine** (211 filas, el mínimo es -154). Positivos > p99×3 (~50) → se quedan con flag `is_cost_spike` (54 filas).
- **R3** — `unit` no nulo cuando `value` existe (2.038 casos): **imputamos** la unidad desde el `metric` en vez de quarantinear.

*Por qué imputar y no descartar en R3:* esos 2.038 eventos tienen un **costo válido**. Si los mandábamos a quarantine, el mart de FinOps **subcontaba el costo total**. La consigna permite imputación, así que conservamos el evento y marcamos `unit_imputed`. (Validación: con la imputación, el costo total Silver = Gold = 148.351,58, no se pierde nada.)

## Particionado
- `usage_events` (Bronze/Silver/Gold) → particionado por **`usage_date`** (rangos de fecha eficientes, que es como se consulta).
- `billing_monthly` → por **`month`** (3 meses, tiene sentido).
- `customers_orgs` (80 filas) y `users` (800 filas) → **sin particionar** a propósito. Particionar tablas chicas genera muchos archivitos y solo agrega overhead. Particionar por particionar es mala práctica.

## Cassandra: driver Python, no conector
Cargamos Gold con `cassandra-driver` + secure-connect-bundle, no con el conector Spark-Cassandra.

*Por qué:* el conector es muy sensible a la versión y en Colab suele romper (ClassNotFound / mismatch de versiones). El driver es lo que documenta AstraDB y es confiable. Gold es chico (~11k filas) → lo traemos con `collect()` y hacemos upsert concurrente. Sigue siendo "carga desde Spark" (los datos salen del DataFrame).

## Modelado de la tabla (query-first)
`PRIMARY KEY ((org_id), usage_date, service)`. Partición = `org_id`: las 2 queries del MVP filtran por una sola org → pegan a una sola partición, sin `ALLOW FILTERING`. `usage_date` primero en el clustering permite rangos de fecha.

**Trade-off en la query #2 (top-N):** lo puro query-first sería una tabla pre-agregada `(org_id) → (service, costo_14d)` con clustering por costo. Para el MVP, que pide **una sola tabla**, resolvemos #2 sobre la misma tabla trayendo el slice de 14 días (una partición, pocas filas) y agregando del lado del cliente. La tabla dedicada queda para la entrega final.

## Idempotencia
Re-ejecutar no duplica porque (a) los `INSERT` de Cassandra son **upserts por primary key** (pisan la misma fila), y (b) el **checkpoint** del stream evita reprocesar archivos ya leídos. El notebook lo evidencia con conteos antes/después del re-run.

---

## Cosas a tener en cuenta para la entrega final
- **Trampa de moneda en billing:** hay USD/ARS/EUR, pero el `exchange_rate_to_usd` de las filas **USD no es 1.0** (viene 0.85–1.0, ruidoso). Para el mart de FinOps de ahora no afecta (los eventos ya traen `cost_usd_increment` en USD), pero para `revenue_by_org_month` no se puede multiplicar `subtotal × exchange_rate` a ciegas — para USD habría que forzar rate = 1.0.
- **"Colecciones" en Cassandra:** el enunciado final valora usar colecciones. En el MVP usamos columnas escalares porque las 2 queries piden métricas puntuales; para la final se puede evaluar un `map<text,double>` de métricas o un `set` de tags.
- **Marts restantes:** faltan `revenue_by_org_month`, `cost_anomaly_mart`, `tickets_by_org_date` y `genai_tokens_by_org_date` (este último es fácil: ya tenemos los tokens en Silver), más las queries #3, #4 y #5.
- **`schema_version=3`:** no aparece en los datos (solo v1/v2, perfectamente consistentes con el corte del 18/07), así que la defensa de "schema desconocido → quarantine" no se llega a disparar acá.
