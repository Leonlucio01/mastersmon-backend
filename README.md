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

## Cuenta QA controlada

Para crear o refrescar una cuenta de pruebas segura sin tocar otros usuarios:

```bash
QA_USER_PASSWORD="elige-una-password-segura" node scripts/seedQaUser.js
```

En PowerShell:

```powershell
$env:QA_USER_PASSWORD="elige-una-password-segura"; node scripts/seedQaUser.js
```

En Render Shell:

```bash
QA_USER_PASSWORD="elige-una-password-segura" node scripts/seedQaUser.js
```

Variables necesarias:

- `DATABASE_URL`
- `QA_USER_PASSWORD`

Antes de ejecutar, confirma que `DATABASE_URL` apunta a la base esperada. El script usa `DATABASE_URL` desde el entorno o `.env`, no imprime secretos y falla si `QA_USER_PASSWORD` no existe. La cuenta controlada es:

```txt
Email: qa@mastersmon.com
Trainer: QA Trainer
```

El seed es idempotente y solo crea/actualiza datos ligados a `qa@mastersmon.com`:

- Wallet: 75000 gold, 500 diamonds, 8 boss tickets.
- Inventario: Poke Ball, Great Ball, Ultra Ball, Master Ball, potions, revives, Rare Candy y piedras evolutivas si existen en `game.items`.
- Equipo 6/6: Pikachu, Bulbasaur, Charmander, Squirtle, Geodude y Gastly.
- Coleccion extra: Eevee, Vulpix, Ekans, Pidgey shiny, Caterpie y Abra locked.
- Pokedex: marca como vistas/capturadas las especies entregadas.
- Misiones: crea filas faltantes de `player_quests` para quests activas, sin reclamar recompensas ni borrar historial.

El script no borra usuarios, capturas, trades, market listings, subastas, historial ni `wallet_transactions`. Si detecta ofertas/listings/subastas abiertas del usuario QA, solo reporta warning y las deja intactas.

Checklist QA recomendada:

- Login con `qa@mastersmon.com` y verificar Hub, wallet e inventario.
- Coleccion: probar filtros por tipo, shiny, rareza, nivel y busqueda.
- Equipo: validar slots completos, mover criaturas y bloqueo de criaturas en equipo.
- Mochila: usar Potion, Revive y Rare Candy; probar piedras evolutivas con Pikachu/Eevee.
- Trade Center: crear oferta con una criatura fuera del equipo, validar error con criatura en equipo y locked.
- Mercado/Subastas: crear venta de item, crear venta/subasta de criatura fuera del equipo, validar errores de fondos/listing propio.
- Gimnasios/Arena: iniciar batalla con equipo real, usar skills, cambio de criatura e items.
- Formularios de creacion: revisar create trade, accept trade, create market listing, create auction y bid/buyout.

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
- `battle_win`: avanza victorias PvE.
- `gym_win`: avanza victorias de gimnasio.
- `badge_earned`: avanza medallas ganadas.
- `arena_win`: avanza victorias de Arena.
- `arena_streak`: sincroniza la mejor racha actual de Arena.
- `trade_list`: avanza ofertas publicadas.
- `trade_complete`: avanza intercambios completados.
- `team_update`: sincroniza el tamano del equipo activo.
- `pokedex_species`: sincroniza especies capturadas en Pokedex.

Errores esperados: `QUEST_NOT_FOUND`, `QUEST_NOT_COMPLETED`, `QUEST_ALREADY_CLAIMED`, `QUEST_REWARD_FAILED` y `QUEST_PROGRESS_FAILED`.

### Trade Center

`GET /api/trades`

Lista ofertas abiertas. Soporta `limit`, `offset`, `species`, `type`, `rarity`, `minLevel`, `search` y `mine=true`.

`GET /api/trades/mine`

Devuelve las ofertas abiertas del usuario, sus ofertas cerradas recientes y trades que acepto.

`POST /api/trades`

```json
{
  "offeredPlayerMonsterId": "UUID_DEL_PLAYER_MONSTER",
  "requestedType": "any",
  "requestedSpeciesId": null,
  "requestedTypeSlug": null,
  "requestedRarity": null,
  "requestedMinLevel": null,
  "requestedNotes": "Busco criatura de agua"
}
```

Crea una oferta abierta. El backend bloquea criaturas de otro usuario, bloqueadas, en equipo o ya publicadas.

`POST /api/trades/:tradeOfferId/accept`

```json
{
  "acceptedPlayerMonsterId": "UUID_DEL_PLAYER_MONSTER"
}
```

Acepta una oferta abierta en una transaccion: bloquea la oferta, valida ownership/requisitos, mueve `game.player_monsters.user_id` de ambas criaturas, marca la oferta como `accepted`, inserta `game.trade_history`, actualiza Pokedex de ambos usuarios y avanza misiones `trade_complete`.

`POST /api/trades/:tradeOfferId/cancel`

Cancela una oferta abierta propia sin mover criaturas.

`GET /api/trades/history`

Devuelve los ultimos intercambios del usuario con snapshots JSONB de criatura enviada y recibida.

La migracion `20260529_trade_center.sql` adapta `game.trade_offers`, crea `game.trade_history`, indices parciales para evitar doble oferta abierta y misiones `complete_1_trade`, `complete_3_trades`, `list_1_trade`.

### Mercado global y subastas

`GET /api/market/listings`

Lista ventas directas abiertas. Query params opcionales: `type`, `search`, `rarity`, `minLevel`, `maxPrice`, `currency`, `mine`, `limit` y `offset`.

`POST /api/market/listings`

Crear venta de criatura:

```json
{
  "listingType": "monster",
  "playerMonsterId": "uuid",
  "priceGold": 5000,
  "priceDiamonds": 0
}
```

Crear venta de item:

```json
{
  "listingType": "item",
  "itemSlug": "rare-candy",
  "quantity": 1,
  "priceGold": 3000,
  "priceDiamonds": 0
}
```

Los items se reservan descontandolos al publicar; si se cancela la venta se devuelven. Las criaturas no cambian de owner al publicar, pero quedan bloqueadas por validacion para trade/equipo/otra venta/subasta.

`POST /api/market/listings/:listingId/buy`

Compra una venta abierta. Usa transaccion, lock del listing y wallets, transfiere oro/diamantes, mueve `player_monsters.user_id` o suma inventario, actualiza Pokedex del comprador, registra `wallet_transactions`, `market_history` y misiones `market_buy` / `market_sell`.

`POST /api/market/listings/:listingId/cancel`

Cancela una venta propia abierta. Si era item, devuelve el stock reservado.

`GET /api/market/auctions`

Lista subastas abiertas o propias con `mine=true`. Devuelve item/criatura, vendedor, puja actual, buyout, mayor postor, vencimiento y flags `can_bid`, `can_buyout`, `can_cancel`.

`POST /api/market/auctions`

Crear subasta de criatura o item:

```json
{
  "auctionType": "monster",
  "playerMonsterId": "uuid",
  "startingPriceGold": 1000,
  "buyoutPriceGold": 10000,
  "durationHours": 24
}
```

`POST /api/market/auctions/:auctionId/bid`

```json
{
  "bidGold": 2000
}
```

La puja se reserva descontando oro al pujar. Si existe postor anterior, se le devuelve su puja en la misma transaccion.

`POST /api/market/auctions/:auctionId/buyout`

Compra directa de subasta. Devuelve la puja previa si existe, transfiere criatura/item, paga al vendedor y registra historial/misiones.

`POST /api/market/auctions/:auctionId/cancel`

Cancela una subasta propia abierta. Si habia puja, la devuelve; si era item, devuelve el stock reservado.

`POST /api/market/auctions/:auctionId/claim`

Finaliza subasta vencida de forma idempotente. Si hay mayor postor, entrega el lote y paga al vendedor; si no hay pujas, marca `expired` y devuelve el item reservado cuando aplica.

`GET /api/market/history`

Devuelve compras, ventas, pujas y subastas del usuario actual.

Errores esperados: `MARKET_INVALID_PRICE`, `MARKET_MONSTER_NOT_FOUND`, `MARKET_MONSTER_NOT_OWNED`, `MARKET_MONSTER_IN_TEAM`, `MARKET_MONSTER_LOCKED`, `MARKET_MONSTER_ALREADY_LISTED`, `MARKET_ITEM_NOT_FOUND`, `MARKET_ITEM_INSUFFICIENT`, `MARKET_LISTING_NOT_FOUND`, `MARKET_LISTING_NOT_OPEN`, `CANNOT_BUY_OWN_LISTING`, `INSUFFICIENT_FUNDS`, `AUCTION_NOT_FOUND`, `AUCTION_NOT_OPEN`, `CANNOT_BID_OWN_AUCTION`, `BID_TOO_LOW`, `AUCTION_BID_FAILED`, `AUCTION_BUYOUT_FAILED`, `AUCTION_CLAIM_FAILED` y `AUCTION_CANCEL_FAILED`.

La migracion `20260529_market_auctions.sql` adapta `game.market_listings`, `game.auction_listings`, `game.auction_bids`, crea `game.market_history`, indices parciales para evitar listings/subastas abiertas duplicadas por criatura y misiones `market_buy`, `market_sell`, `auction_bid`, `auction_win`.

Errores esperados: `MONSTER_NOT_FOUND`, `MONSTER_NOT_OWNED`, `MONSTER_LOCKED`, `MONSTER_IN_TEAM`, `MONSTER_ALREADY_LISTED`, `TRADE_NOT_FOUND`, `TRADE_NOT_OPEN`, `CANNOT_ACCEPT_OWN_TRADE`, `ACCEPT_MONSTER_NOT_FOUND`, `ACCEPT_MONSTER_NOT_OWNED`, `ACCEPT_MONSTER_LOCKED`, `ACCEPT_MONSTER_IN_TEAM`, `ACCEPT_MONSTER_ALREADY_LISTED`, `TRADE_REQUIREMENT_NOT_MET`, `TRADE_NOT_OWNED`, `TRADE_CREATE_FAILED`, `TRADE_ACCEPT_FAILED` y `TRADE_CANCEL_FAILED`.

### Batallas PvE con skills

`GET /api/me/monsters/:playerMonsterId/skills`

Devuelve hasta 4 skills disponibles para una criatura del usuario actual.

`GET /api/gyms`

Lista gimnasios reales desde `game.gyms`, con region, tipo, medalla, poder recomendado, recompensa y estado del jugador:

- `is_unlocked`
- `is_completed`
- `recommended_level`
- `difficulty_label`: `easy`, `normal` o `hard`
- `player_team_power`
- `player_average_level`
- `wins`
- `completed_at`
- `reward_gold`
- `reward_items`
- `required_previous_gym`

`POST /api/battles/start`

```json
{
  "battleType": "gym",
  "targetSlug": "kanto-boulder-badge"
}
```

Usa el equipo activo real del jugador. Si un gimnasio esta bloqueado responde `GYM_LOCKED`. Si un gimnasio aun no tiene filas en `game.gym_trainer_team`, el backend genera un equipo PvE basico segun el tipo del gimnasio para mantener la fase jugable. La migracion `20260527_gym_ai_balance.sql` siembra equipos iniciales para los primeros gimnasios de Kanto cuando estan vacios.

`GET /api/battles/:battleId`

Devuelve estado de batalla, equipos, HP, energia, criatura activa, skills disponibles, items disponibles en batalla, log, ganador y recompensas.

Cada skill del activo incluye:

- `energy_cost`
- `cooldown_turns`
- `effect_type`
- `effect_chance`
- `effect_value`
- `current_cooldown`
- `can_use`
- `disabled_reason`

`POST /api/battles/:battleId/turn`

```json
{
  "action": "skill",
  "skillSlug": "thunder-shock"
}
```

El frontend no calcula dano, energia ni cooldowns. El backend valida la skill del Pokemon activo, calcula precision, dano, STAB, efectividad simple, critico, respuesta enemiga y guarda `battle_state` mas filas en `game.battle_turns`.

Energia y cooldowns:

- Cada criatura en `battle_state` tiene `energy`, `maxEnergy` y `cooldowns`.
- Energia inicial: `100/100`.
- Al usar una skill se descuenta `energy_cost` y se aplica `cooldown_turns`.
- Al final de cada turno completo, los activos vivos regeneran 15 energia.
- Los cooldowns bajan solo para criaturas activas; la skill usada mantiene su cooldown inicial durante ese cierre de turno.
- Tackle queda como fallback de costo 0 y cooldown 0.

Errores de disponibilidad:

```json
{
  "ok": false,
  "code": "NOT_ENOUGH_ENERGY",
  "error": "Not enough energy to use this skill."
}
```

Efectos y estados basicos:

- La migracion `20260526_battle_status_effects.sql` agrega `effect_type`, `effect_chance` y `effect_value` a `game.skills`.
- `poison-sting` puede aplicar `poison`, `ember` puede aplicar `burn`, `thunder-shock` puede aplicar `paralysis` y `mud-slap` puede aplicar `accuracy_down`.
- Los estados viven dentro de `battle_state` por criatura en `statusEffects`; los modificadores temporales viven en `statStages`.
- `poison` causa 8% del HP maximo al cierre de turno durante 3 turnos.
- `burn` causa 5% del HP maximo al cierre de turno durante 3 turnos.
- `paralysis` dura 2 turnos y tiene 25% de probabilidad de bloquear la accion antes de consumir energia o cooldown.
- Cambiar criatura conserva sus estados en `battle_state`.
- Usar potion/revive en batalla cura HP, pero no limpia estados.

Ejemplo de metadata en `battle_turns.result`:

```json
{
  "effect_type": "poison",
  "effect_chance": 30,
  "effect_success": true,
  "effect_applied": "poison",
  "status_blocked": false
}
```

IA enemiga:

- El enemigo evalua solo skills disponibles: respeta energia, cooldown y HP.
- Prioriza una skill que pueda hacer KO.
- Luego prioriza ventaja de tipo.
- Luego prioriza efectos utiles si el objetivo aun no tiene ese estado.
- Si tiene HP bajo, prioriza dano.
- Si el activo enemigo esta en 25% HP o menos y otro rival vivo tiene mejor matchup, puede cambiar. El cambio consume su turno y se limita para evitar loops.
- En gimnasios, el rival tiene una `potion` por batalla; puede usarla si baja a 35% HP o menos.
- `battle_turns.result` guarda `enemy_decision_reason`, `expected_damage`, `was_best_move`, `enemy_switched` y `enemy_used_item` cuando aplica.

```json
{
  "ok": false,
  "code": "SKILL_ON_COOLDOWN",
  "error": "Skill is on cooldown."
}
```

Tambien soporta cambiar criatura. Cambiar consume turno y el enemigo responde:

```json
{
  "action": "switch",
  "targetPlayerMonsterId": "uuid"
}
```

Y usar items permitidos durante batalla. El item se descuenta del inventario real y el HP modificado vive en `battle_state`:

```json
{
  "action": "use_item",
  "itemSlug": "potion",
  "targetPlayerMonsterId": "uuid"
}
```

Items permitidos en batalla: `potion`, `super-potion`, `hyper-potion` y `revive`.

`GET /api/me/battles`

Devuelve las ultimas batallas del jugador actual. Query params opcionales:

- `limit` default 20, maximo 100
- `status`
- `battleType`

Cada fila incluye `battle_id`, `battle_type`, `target_slug`, `target_name`, `status`, `winner`, `rewards`, fechas, total de turnos y resumen compacto de equipos.

`GET /api/me/battles/:battleId`

Devuelve el detalle completo de una batalla propia: sesion, `battle_state` final, turnos ordenados, recompensas y resumen de dano causado/recibido, skills usadas, criaturas debilitadas, duracion y resultado. El historial solo lee recompensas ya persistidas; no recalcula premios.

Los turnos guardan datos de auditoria en `result` JSONB cuando estan disponibles: `event_type`, `actor`, `target`, `skill`, `item`, `damage`, `critical`, `type_multiplier`, `remaining_hp`, `energy_before`, `energy_after`, `cooldowns_after`, `skill_energy_cost`, `skill_cooldown_turns`, `effect_type`, `effect_chance`, `effect_applied`, `status_blocked`, `blocked_by`, `enemy_decision_reason`, `expected_damage`, `was_best_move`, `enemy_switched` y `enemy_used_item`.

Al ganar una batalla de tipo `gym`, la victoria guarda progreso en `game.player_gym_progress`, entrega medalla en `game.player_achievements`, entrega recompensa, registra `wallet_transactions` e incrementa misiones `battle_win`, `gym_win` y `badge_earned` cuando aplica. La recompensa completa se entrega solo en la primera victoria de cada gimnasio; repetirlo entrega una recompensa reducida.

Al ganar una batalla de tipo `arena`, la victoria actualiza `game.player_arena_progress`, suma puntos, victorias, racha, rango, entrega oro, registra `wallet_transactions` e incrementa misiones `battle_win`, `arena_win` y `arena_streak` cuando aplica. Al perder, suma derrotas y corta la racha.

Errores esperados: `BATTLE_NOT_FOUND`, `BATTLE_NOT_OWNED`, `BATTLE_ALREADY_FINISHED`, `TEAM_EMPTY`, `GYM_NOT_FOUND`, `GYM_LOCKED`, `NPC_NOT_FOUND`, `INVALID_BATTLE_TYPE`, `INVALID_ACTION`, `SKILL_NOT_FOUND`, `SKILL_NOT_AVAILABLE`, `NOT_ENOUGH_ENERGY`, `SKILL_ON_COOLDOWN`, `NO_AVAILABLE_SKILLS`, `ENERGY_STATE_INVALID`, `STATUS_STATE_INVALID`, `EFFECT_APPLY_FAILED`, `ACTIVE_MONSTER_FAINTED`, `MONSTER_NOT_IN_BATTLE`, `MONSTER_FAINTED`, `MONSTER_ALREADY_ACTIVE`, `ITEM_NOT_ALLOWED_IN_BATTLE`, `ITEM_NOT_FOUND`, `INSUFFICIENT_ITEM`, `MONSTER_ALREADY_FULL_HP`, `MONSTER_NOT_FAINTED`, `SWITCH_FAILED`, `ITEM_USE_FAILED`, `BATTLE_TURN_FAILED`, `GYM_PROGRESS_FAILED`, `BADGE_GRANT_FAILED`, `ARENA_RIVAL_NOT_FOUND`, `ARENA_TEAM_EMPTY`, `ARENA_BATTLE_START_FAILED`, `ARENA_PROGRESS_FAILED`, `ARENA_REWARD_FAILED` y `BATTLE_REWARD_FAILED`.

### Arena PvE

`GET /api/arena`

Devuelve progreso del jugador, rivales disponibles, ranking simple, preview de recompensas y rival recomendado:

- `progress`: puntos, victorias, derrotas, racha, mejor racha, rango actual y siguiente rango.
- `rivals`: `slug`, nombre, rango, nivel recomendado, dificultad, equipo preview, oro y puntos de recompensa.
- `ranking`: top Arena con jugadores reales y filas de sistema si todavia no hay suficientes usuarios.

`POST /api/arena/battles/start`

```json
{
  "targetSlug": "ragnar-bronce-ii"
}
```

Inicia una `battle_session` con `battle_type = "arena"` y reutiliza el mismo motor PvE: skills, energia, cooldowns, estados, cambio de criatura, items, IA enemiga, historial y rewards. Tambien se puede iniciar desde `POST /api/battles/start` enviando `battleType: "arena"`.

`GET /api/arena/ranking`

Devuelve top Arena ordenado por puntos, victorias y mejor racha. Incluye `player_position` cuando el jugador actual no aparece en el corte solicitado.

La migracion `20260527_arena_ladder.sql` crea `game.player_arena_progress`, `game.arena_npc_profiles`, `game.arena_npc_teams`, siembra rivales base y agrega misiones `arena_win` / `arena_streak` de forma idempotente.

### Progreso de gimnasios y medallas

`GET /api/me/gym-progress`

Devuelve:

- `total_gyms`
- `completed_gyms`
- `badges`
- `gyms`
- `next_gym`

`GET /api/me/badges`

Devuelve las medallas de gimnasio desbloqueadas desde `game.player_achievements`.

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
database/migrations/20260521_gym_progress_badges.sql
database/migrations/20260522_battle_switch_items.sql
database/migrations/20260524_battle_history.sql
database/migrations/20260526_battle_energy_cooldowns.sql
database/migrations/20260526_battle_status_effects.sql
database/migrations/20260529_trade_center.sql
database/migrations/20260529_market_auctions.sql
```

Agrega `password_hash` y `last_login_at` con `ADD COLUMN IF NOT EXISTS`, sin borrar datos ni cambiar IDs existentes.
La migracion de `rare-candy` agrega el item y su categoria de forma idempotente si faltan en la base.
La migracion de misiones agrega columnas compatibles a `game.quests` y `game.player_quests`, y siembra las misiones base con `ON CONFLICT`.
La migracion de batallas crea `game.skills`, `game.monster_species_skills`, asigna skills basicas por tipo y agrega columnas JSON/resultado a `battle_sessions` y `battle_turns`.
La migracion de progreso de gimnasios crea `game.player_gym_progress`, siembra achievements de medallas y agrega misiones de batalla/gimnasio de forma idempotente.
La migracion de acciones de batalla agrega columnas opcionales `item_slug` e `item_name` a `game.battle_turns` para registrar items usados durante combate.
La migracion de historial de batallas permite `status = completed` en `game.battle_sessions` y agrega indices para listar batallas y turnos recientes con menor costo.
La migracion de energia/cooldowns asegura `energy_cost` y `cooldown_turns` en `game.skills` y actualiza los costos base de las skills PvE.
La migracion de estados agrega efectos simples a `game.skills` para poison, burn, paralysis y accuracy_down.
La migracion de Trade Center crea ofertas/historial real de intercambio.
La migracion de Mercado global adapta ventas directas, subastas, pujas, historial e inserta misiones de mercado de forma idempotente.
