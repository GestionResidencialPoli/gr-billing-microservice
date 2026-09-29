# gr-billing-microservice

- FastAPI, SQLAlchemy, Alembic y Strawberry; Alembic es el único dueño del esquema.
- Nunca usar `float` para dinero; conservar `Decimal` y `NUMERIC(15,2)`.
- La API pública usa `/api/v1/finanzas`, cookies/JWT/CSRF y errores con código estable.
- Las ramas salen de `develop` con `feature/GR-###-descripcion`; los commits usan `tipo(scope): GR-### descripcion breve`.
