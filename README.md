# MastersMon Backend

Backend Node/Express para MastersMon Online usando el schema `game` de PostgreSQL.

## Archivos importantes

```txt
package.json
src/server.js
.env.example
scripts/check-db.js
```

## Local

```bash
npm install
cp .env.example .env
npm run check:db
npm run dev
```

Auth usa JWT con modo hibrido temporal:

- Si el request trae `Authorization: Bearer TOKEN`, la API usa el usuario autenticado.
- Si no hay token, la API usa `CURRENT_USER_EMAIL` como fallback temporal.
- TODO: remover el fallback cuando auth este completamente conectado en produccion.

## Render

Build Command:

```bash
npm install
```

Start Command:

```bash
npm start
```

Environment Variables:

```txt
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/mastersmon?sslmode=require
NODE_ENV=production
CURRENT_USER_EMAIL=demo@mastersmon.com
JWT_SECRET=...
JWT_EXPIRES_IN=7d
CORS_ORIGIN=https://mastersmon.com
```

## Endpoints

```txt
GET  /api/health
POST /api/auth/register
POST /api/auth/login
GET  /api/auth/me
POST /api/auth/logout
GET  /api/me
GET  /api/me/inventory
GET  /api/me/team
GET  /api/me/collection
GET  /api/me/pokedex-summary
GET  /api/me/pokedex
GET  /api/maps
GET  /api/maps/:slug/spawns
POST /api/encounters
GET  /api/encounters/active
POST /api/captures
GET  /api/server/recent-captures
```

Los endpoints `/api/demo/*` se mantienen temporalmente por compatibilidad con clientes antiguos, pero el flujo principal debe usar los endpoints listados arriba.

### Auth

`POST /api/auth/register`

```json
{
  "email": "user@example.com",
  "password": "123456",
  "trainerName": "Nombre"
}
```

`POST /api/auth/login`

```json
{
  "email": "user@example.com",
  "password": "123456"
}
```

Ambos devuelven:

```json
{
  "ok": true,
  "token": "JWT",
  "user": { "id": "...", "email": "user@example.com" },
  "profile": {}
}
```

Para requests autenticados:

```txt
Authorization: Bearer TOKEN
```

`GET /api/auth/me` devuelve `{ ok, user, profile }`. `POST /api/auth/logout` devuelve `{ ok: true }`; el cierre real de sesion se hace eliminando el token en el frontend.

## Prueba rapida

Crear encuentro:

```bash
curl -X POST https://TU_API_RENDER.onrender.com/api/encounters \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TOKEN" \
  -d "{\"mapSlug\":\"bosque-verde\"}"
```

Capturar:

```bash
curl -X POST https://TU_API_RENDER.onrender.com/api/captures \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TOKEN" \
  -d "{\"encounterId\":\"ID_DEL_ENCUENTRO\",\"ballSlug\":\"poke-ball\"}"
```

## Migraciones

La migracion segura de auth esta en:

```txt
database/migrations/20260518_auth_users.sql
```

Agrega `password_hash` y `last_login_at` con `ADD COLUMN IF NOT EXISTS`, sin borrar datos ni cambiar IDs existentes.
