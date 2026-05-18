import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import pg from "pg";

dotenv.config();

const { Pool } = pg;

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL environment variable.");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes("sslmode=require")
    ? { rejectUnauthorized: false }
    : process.env.NODE_ENV === "production"
      ? { rejectUnauthorized: false }
      : undefined,
});

const app = express();

app.use(cors({
  origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(",") : "*",
  credentials: true,
}));

app.use(express.json());

async function query(sql, params = []) {
  const result = await pool.query(sql, params);
  return result.rows;
}

function getCurrentUserEmail() {
  const email = process.env.CURRENT_USER_EMAIL;

  if (!email) {
    const error = new Error("Missing CURRENT_USER_EMAIL environment variable.");
    error.status = 500;
    throw error;
  }

  return email;
}

async function getCurrentUserId() {
  const email = getCurrentUserEmail();
  const rows = await query(
    "SELECT id FROM game.users WHERE email = $1 LIMIT 1",
    [email]
  );

  if (!rows.length) {
    const error = new Error(`Current user not found for CURRENT_USER_EMAIL: ${email}`);
    error.status = 404;
    throw error;
  }

  return rows[0].id;
}

function getLimit(value, fallback, max) {
  const parsed = Number(value || fallback);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.floor(parsed), max);
}

function asyncRoute(handler) {
  return async (req, res, next) => {
    try {
      await handler(req, res, next);
    } catch (error) {
      next(error);
    }
  };
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    name: "MastersMon API",
    runtime: "node",
    schema: "game",
  });
});

app.get("/api/health", asyncRoute(async (req, res) => {
  const rows = await query("SELECT now() AS server_time");
  res.json({
    ok: true,
    db: true,
    serverTime: rows[0].server_time,
  });
}));

// =======================================================
// Current player endpoints
// =======================================================

async function sendCurrentProfile(req, res) {
  const rows = await query(
    "SELECT * FROM game.v_trainer_profile WHERE email = $1 LIMIT 1",
    [getCurrentUserEmail()]
  );
  res.json(rows[0] || null);
}

async function sendCurrentInventory(req, res) {
  const rows = await query(
    "SELECT * FROM game.v_player_inventory WHERE email = $1 ORDER BY category_slug, item_slug",
    [getCurrentUserEmail()]
  );
  res.json(rows);
}

async function sendCurrentTeam(req, res) {
  const rows = await query(
    "SELECT * FROM game.v_player_team WHERE email = $1 ORDER BY slot_number",
    [getCurrentUserEmail()]
  );
  res.json(rows);
}

async function sendCurrentCollection(req, res) {
  const limit = getLimit(req.query.limit, 100, 500);

  const rows = await query(
    `
    SELECT *
    FROM game.v_player_collection
    WHERE email = $1
    ORDER BY captured_at DESC
    LIMIT $2
    `,
    [getCurrentUserEmail(), limit]
  );

  res.json(rows);
}

async function sendCurrentPokedexSummary(req, res) {
  const rows = await query(
    "SELECT * FROM game.v_player_pokedex_summary WHERE email = $1 LIMIT 1",
    [getCurrentUserEmail()]
  );
  res.json(rows[0] || null);
}

async function sendCurrentPokedex(req, res) {
  const generation = req.query.generation ? Number(req.query.generation) : null;
  const caught = req.query.caught;

  const params = [getCurrentUserEmail()];
  let where = "email = $1";

  if (generation) {
    params.push(generation);
    where += ` AND generation_number = $${params.length}`;
  }

  if (caught === "true" || caught === "false") {
    params.push(caught === "true");
    where += ` AND COALESCE(caught, false) = $${params.length}`;
  }

  const rows = await query(
    `
    SELECT *
    FROM game.v_player_pokedex
    WHERE ${where}
    ORDER BY dex_number
    `,
    params
  );

  res.json(rows);
}

app.get("/api/me", asyncRoute(sendCurrentProfile));
app.get("/api/me/inventory", asyncRoute(sendCurrentInventory));
app.get("/api/me/team", asyncRoute(sendCurrentTeam));
app.get("/api/me/collection", asyncRoute(sendCurrentCollection));
app.get("/api/me/pokedex-summary", asyncRoute(sendCurrentPokedexSummary));
app.get("/api/me/pokedex", asyncRoute(sendCurrentPokedex));

// =======================================================
// Maps / spawns
// =======================================================

app.get("/api/maps", asyncRoute(async (req, res) => {
  const rows = await query(
    "SELECT * FROM game.v_maps_overview WHERE is_active = true ORDER BY slug"
  );
  res.json(rows);
}));

app.get("/api/maps/:slug/spawns", asyncRoute(async (req, res) => {
  const rows = await query(
    `
    SELECT *
    FROM game.v_map_spawns_detailed
    WHERE map_slug = $1
    ORDER BY spawn_weight DESC, dex_number
    `,
    [req.params.slug]
  );
  res.json(rows);
}));

// =======================================================
// Encounter / capture
// =======================================================

async function createEncounter(req, res) {
  const mapSlug = req.body?.mapSlug || req.body?.map_slug || "bosque-verde";
  const userId = await getCurrentUserId();

  const rows = await query(
    "SELECT * FROM game.create_wild_encounter($1, $2)",
    [userId, mapSlug]
  );

  res.status(201).json(rows[0]);
}

async function sendActiveEncounters(req, res) {
  const rows = await query(
    `
    SELECT *
    FROM game.v_active_encounters
    WHERE email = $1
    ORDER BY created_at DESC
    LIMIT 20
    `,
    [getCurrentUserEmail()]
  );

  res.json(rows);
}

async function captureEncounter(req, res) {
  const encounterId = req.body?.encounterId || req.body?.encounter_id;
  const ballSlug = req.body?.ballSlug || req.body?.ball_slug || "poke-ball";

  if (!encounterId) {
    return res.status(400).json({
      ok: false,
      error: "encounterId is required",
    });
  }

  const userId = await getCurrentUserId();

  const rows = await query(
    "SELECT * FROM game.attempt_capture($1, $2, $3)",
    [userId, encounterId, ballSlug]
  );

  res.json(rows[0]);
}

async function captureLatestActiveEncounter(req, res) {
  const ballSlug = req.body?.ballSlug || req.body?.ball_slug || "poke-ball";
  const email = getCurrentUserEmail();
  const activeRows = await query(
    `
    SELECT encounter_id
    FROM game.v_active_encounters
    WHERE email = $1
    ORDER BY created_at DESC
    LIMIT 1
    `,
    [email]
  );

  if (!activeRows.length) {
    return res.status(404).json({
      ok: false,
      error: "No active encounter found for current user",
    });
  }

  req.body = {
    ...req.body,
    encounterId: activeRows[0].encounter_id,
    ballSlug,
  };

  return captureEncounter(req, res);
}

app.post("/api/encounters", asyncRoute(createEncounter));
app.get("/api/encounters/active", asyncRoute(sendActiveEncounters));
app.post("/api/captures", asyncRoute(captureEncounter));

// Legacy demo endpoints. Keep temporarily for backward compatibility.
app.get("/api/demo/me", asyncRoute(sendCurrentProfile));
app.get("/api/demo/inventory", asyncRoute(sendCurrentInventory));
app.get("/api/demo/team", asyncRoute(sendCurrentTeam));
app.get("/api/demo/collection", asyncRoute(sendCurrentCollection));
app.get("/api/demo/pokedex-summary", asyncRoute(sendCurrentPokedexSummary));
app.get("/api/demo/pokedex", asyncRoute(sendCurrentPokedex));
app.post("/api/demo/encounters", asyncRoute(createEncounter));
app.get("/api/demo/encounters/active", asyncRoute(sendActiveEncounters));
app.post("/api/demo/captures", asyncRoute(captureEncounter));
app.post("/api/demo/captures/latest", asyncRoute(captureLatestActiveEncounter));

// =======================================================
// Server activity
// =======================================================

app.get("/api/server/recent-captures", asyncRoute(async (req, res) => {
  const limit = getLimit(req.query.limit, 20, 100);

  const rows = await query(
    `
    SELECT *
    FROM game.v_server_recent_captures
    LIMIT $1
    `,
    [limit]
  );

  res.json(rows);
}));

app.use((req, res) => {
  res.status(404).json({
    ok: false,
    error: "Route not found",
    path: req.path,
  });
});

app.use((error, req, res, next) => {
  console.error(error);

  res.status(error.status || 500).json({
    ok: false,
    error: error.message || "Internal server error",
  });
});

app.listen(PORT, () => {
  console.log(`MastersMon API running on port ${PORT}`);
});
