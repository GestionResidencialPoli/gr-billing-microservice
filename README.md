# gr-billing-microservice

Servicio financiero en FastAPI con PostgreSQL, Alembic y GraphQL de solo lectura. Los importes se almacenan como `NUMERIC(15,2)` y se serializan como strings decimales. Las escrituras permanecen en REST para conservar idempotencia y contratos claros.

## Rutas

- Salud: `GET /health` y `GET /health/ready`.
- Parámetros y valores: `GET/PUT /api/v1/finanzas/parametros`, `GET/PUT /api/v1/finanzas/valores-apartamento/{id}`.
- Cobros: `POST /api/v1/finanzas/cobros/generar` encola la generación; consultar progreso con `GET /api/v1/finanzas/cobros/generaciones/{id}`.
- Pagos y cartera: `POST /api/v1/finanzas/pagos` requiere UUID `Idempotency-Key`; reversiones crean movimientos compensatorios.
- Estado de cuenta: administración consulta por apartamento; propietarios solo consultan su vivienda y deben tener rol `RESIDENTE` y tipo `PROPIETARIO` en Identidad.
- Comprobantes: JPG, PNG o PDF de hasta 5 MB; aprobación administrativa registra un pago. La revisión es simulada y no consulta bancos.
- Reportes CSV/PDF y `POST /api/v1/finanzas/graphql`; GraphQL es de solo lectura y su consulta `cartera(fechaCorte)` limita profundidad y tokens. En `APP_ENV=production` la introspección queda desactivada.

## Ejecución local

Configura las variables de `.env.example`, instala `requirements.txt` y ejecuta `alembic upgrade head` antes de `uvicorn app.main:app --port 4400`. La generación se encola en PostgreSQL y el worker procesa lotes de 100 apartamentos con hasta cuatro cálculos en paralelo por instancia. Los eventos `cartera.estado-actualizado` salen del outbox al exchange `gr.finance.events` con confirmación del broker.

Para pruebas, instala `requirements-dev.txt`, usa una base cuyo nombre contenga `_test_db` y configura un exchange de RabbitMQ terminado en `.test.events`; las pruebas se niegan a truncar cualquier otra base o exchange.
