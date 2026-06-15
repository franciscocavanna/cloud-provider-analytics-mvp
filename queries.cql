-- ============================================================
-- Cloud Provider Analytics — Serving en AstraDB (Cassandra)
-- Tabla modelada query-first para el mart FinOps del MVP.
-- ============================================================

-- El keyspace 'cloud_analytics' se crea en la consola de AstraDB
-- al crear la base Serverless (en serverless no se hace CREATE KEYSPACE por CQL).
-- USE cloud_analytics;

-- ------------------------------------------------------------
-- Tabla: org_daily_usage_by_service
-- PK: partition = org_id  |  clustering = usage_date, service
--   - partition por org_id  -> las 2 queries filtran por una org = una sola partición
--   - usage_date primero en el clustering -> rangos de fecha eficientes
--   - service desempata dentro del día
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id           text,
    usage_date       date,
    service          text,
    daily_cost_usd   double,
    requests         double,
    cpu_hours        double,
    storage_gb_hours double,
    genai_tokens     double,
    carbon_kg        double,
    event_count      int,
    has_anomaly      int,
    plan_tier        text,
    industry         text,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date ASC, service ASC);


-- ============================================================
-- QUERY #1 — Costos y requests diarios por org y servicio
--            en un rango de fechas.
-- Consulta nativa de una sola partición con rango sobre usage_date.
-- (Reemplazar los ? por valores reales, p.ej. org_xxxx y fechas.)
-- ============================================================
SELECT usage_date, service, daily_cost_usd, requests
FROM org_daily_usage_by_service
WHERE org_id = ?
  AND usage_date >= ?
  AND usage_date <= ?;


-- ============================================================
-- QUERY #2 — Top-N servicios por costo acumulado en los
--            últimos 14 días para una organización.
-- Cassandra trae el slice de 14 días de la partición (una org);
-- el sumado por servicio + orden + top-N se hace del lado del cliente
-- (ver DECISIONES.md: para el MVP usamos una sola tabla).
-- ============================================================
SELECT service, daily_cost_usd
FROM org_daily_usage_by_service
WHERE org_id = ?
  AND usage_date >= ?   -- max_date - 14 días
  AND usage_date <= ?;  -- max_date
-- luego, en la app: agrupar por service, sum(daily_cost_usd), ordenar desc, tomar N.
