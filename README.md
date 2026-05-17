# MastersMon Backend Node

Backend Node/Express para consumir el schema `game` de PostgreSQL.

## Archivos importantes

```txt
package.json
src/server.js
.env.example
```

## Local

```bash
npm install
cp .env.example .env
npm run dev
```

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
DEMO_EMAIL=demo@mastersmon.com
CORS_ORIGIN=*
```

## Endpoints iniciales

```txt
GET  /api/health
GET  /api/demo/me
GET  /api/demo/inventory
GET  /api/demo/team
GET  /api/demo/collection
GET  /api/demo/pokedex-summary
GET  /api/demo/pokedex
GET  /api/maps
GET  /api/maps/:slug/spawns
POST /api/demo/encounters
GET  /api/demo/encounters/active
POST /api/demo/captures
POST /api/demo/captures/latest
GET  /api/server/recent-captures
```

## Prueba rapida

Crear encuentro:

```bash
curl -X POST https://TU_API_RENDER.onrender.com/api/demo/encounters \
  -H "Content-Type: application/json" \
  -d "{\"mapSlug\":\"bosque-verde\"}"
```

Capturar:

```bash
curl -X POST https://TU_API_RENDER.onrender.com/api/demo/captures/latest \
  -H "Content-Type: application/json" \
  -d "{\"ballSlug\":\"poke-ball\"}"
```
