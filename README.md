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
GET  /api/me/monsters/:playerMonsterId
GET  /api/me/monsters/:playerMonsterId/evolutions
GET  /api/me/collection
GET  /api/me/pokedex-summary
GET  /api/me/pokedex
GET  /api/shop/items
POST /api/shop/buy
POST /api/items/use
POST /api/evolutions/evolve
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

### Items

`POST /api/items/use`

Usa el usuario resuelto por JWT o, temporalmente, `CURRENT_USER_EMAIL` cuando no hay token.

```json
{
  "itemSlug": "potion",
  "playerMonsterId": "UUID_DEL_PLAYER_MONSTER",
  "quantity": 1
}
```

Ejemplo Rare Candy:

```json
{
  "itemSlug": "rare-candy",
  "playerMonsterId": "UUID_DEL_PLAYER_MONSTER",
  "quantity": 1
}
```

Items soportados en esta fase: `potion`, `super-potion`, `hyper-potion`, `revive` y `rare-candy`. Las piedras evolutivas se usan desde los endpoints de evoluciones.

Errores esperados: `ITEM_NOT_FOUND`, `ITEM_NOT_USABLE`, `INVALID_QUANTITY`, `INSUFFICIENT_ITEM`, `MONSTER_NOT_FOUND`, `MONSTER_NOT_OWNED`, `MONSTER_ALREADY_FULL_HP`, `MONSTER_NOT_FAINTED`, `MAX_LEVEL_REACHED` y `ITEM_USE_FAILED`.

### Evoluciones

`GET /api/me/monsters/:playerMonsterId/evolutions`

Devuelve las reglas de evolucion disponibles para una criatura del usuario actual, incluyendo requisito de nivel o item, cantidad poseida y si puede evolucionar.

`POST /api/evolutions/evolve`

Evolucion por nivel:

```json
{
  "playerMonsterId": "UUID_DEL_PLAYER_MONSTER",
  "ruleId": "UUID_DE_LA_REGLA"
}
```

Evolucion por piedra:

```json
{
  "playerMonsterId": "UUID_DEL_PLAYER_MONSTER",
  "ruleId": "UUID_DE_LA_REGLA"
}
```

El backend valida la regla contra `game.evolution_rules`. Si la regla usa `use-item`, descuenta una unidad del item requerido en la misma transaccion. La evolucion actualiza `game.player_monsters.species_id` sin crear un monstruo nuevo, por lo que el slot de equipo se mantiene.

Tambien actualiza `game.player_pokedex` para la especie destino con `seen=true` y `caught=true`, sin borrar la especie anterior.

Errores esperados: `MONSTER_NOT_FOUND`, `MONSTER_NOT_OWNED`, `EVOLUTION_NOT_FOUND`, `EVOLUTION_NOT_AVAILABLE`, `LEVEL_TOO_LOW`, `REQUIRED_ITEM_MISSING`, `INSUFFICIENT_ITEM`, `ALREADY_FINAL_EVOLUTION`, `INVALID_EVOLUTION_RULE` y `EVOLUTION_FAILED`.

### Misiones

`GET /api/me/quests`

Inicializa las misiones activas faltantes para el usuario actual, sincroniza progreso computable y devuelve:

```json
[
  {
    "quest_id": "UUID",
    "slug": "capture_3",
    "title": "Captura 3 criaturas",
    "quest_type": "capture",
    "progress": 1,
    "target_count": 3,
    "status": "active",
    "can_claim": false,
    "rewards": { "gold": 1000, "diamonds": 0, "itemSlug": "poke-ball", "itemQuantity": 5 }
  }
]
```

`POST /api/me/quests/:questId/claim`

Reclama una mision completada. La entrega de oro, diamantes e items es transaccional y no se entrega automaticamente al completar la mision.

Eventos de progreso integrados:

- `capture`: avanza capturas generales, por tipo y shiny si aplica.
- `buy_item`: avanza compras de tienda.
- `use_item`: avanza uso de items desde Mochila.
- `evolve`: avanza evoluciones.
- `team_update`: sincroniza el tamano del equipo activo.
- `pokedex_species`: sincroniza especies capturadas en Pokedex.

Errores esperados: `QUEST_NOT_FOUND`, `QUEST_NOT_COMPLETED`, `QUEST_ALREADY_CLAIMED`, `QUEST_REWARD_FAILED` y `QUEST_PROGRESS_FAILED`.

### Batallas PvE con skills

`GET /api/me/monsters/:playerMonsterId/skills`

Devuelve hasta 4 skills disponibles para una criatura del usuario actual.

`GET /api/gyms`

Lista gimnasios reales desde `game.gyms`, con region, tipo, medalla, poder recomendado y tamano de equipo si existe.

`POST /api/battles/start`

```json
{
  "battleType": "gym",
  "targetSlug": "kanto-boulder-badge"
}
```

Usa el equipo activo real del jugador. Si un gimnasio aun no tiene filas en `game.gym_trainer_team`, el backend genera un equipo PvE basico segun el tipo del gimnasio para mantener la fase jugable.

`GET /api/battles/:battleId`

Devuelve estado de batalla, equipos, HP, criatura activa, skills disponibles, log, ganador y recompensas.

`POST /api/battles/:battleId/turn`

```json
{
  "action": "skill",
  "skillSlug": "thunder-shock"
}
```

El frontend no calcula dano. El backend valida la skill del Pokemon activo, calcula precision, dano, STAB, efectividad simple, critico, respuesta enemiga y guarda `battle_state` mas filas en `game.battle_turns`.

Errores esperados: `BATTLE_NOT_FOUND`, `BATTLE_NOT_OWNED`, `BATTLE_ALREADY_FINISHED`, `TEAM_EMPTY`, `GYM_NOT_FOUND`, `NPC_NOT_FOUND`, `INVALID_BATTLE_TYPE`, `INVALID_ACTION`, `SKILL_NOT_FOUND`, `SKILL_NOT_AVAILABLE`, `ACTIVE_MONSTER_FAINTED`, `BATTLE_TURN_FAILED` y `BATTLE_REWARD_FAILED`.

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
database/migrations/20260519_rare_candy_item.sql
database/migrations/20260520_real_quests.sql
database/migrations/20260521_pve_battles_skills.sql
```

Agrega `password_hash` y `last_login_at` con `ADD COLUMN IF NOT EXISTS`, sin borrar datos ni cambiar IDs existentes.
La migracion de `rare-candy` agrega el item y su categoria de forma idempotente si faltan en la base.
La migracion de misiones agrega columnas compatibles a `game.quests` y `game.player_quests`, y siembra las misiones base con `ON CONFLICT`.
La migracion de batallas crea `game.skills`, `game.monster_species_skills`, asigna skills basicas por tipo y agrega columnas JSON/resultado a `battle_sessions` y `battle_turns`.
