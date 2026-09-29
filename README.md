# gr-billing-microservice

Servicio financiero en FastAPI con PostgreSQL, Alembic y GraphQL de solo lectura. Los importes se almacenan como `NUMERIC(15,2)` y se serializan como strings decimales. Las escrituras permanecen en REST para conservar idempotencia y contratos claros.

## Rutas iniciales

`/health`, `/health/ready`, `/api/v1/finanzas/parametros`, `/api/v1/finanzas/cobros/generar`, `/api/v1/finanzas/pagos`, `/api/v1/finanzas/cartera` y `/api/v1/finanzas/graphql`.
