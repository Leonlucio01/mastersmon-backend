import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import pg from "pg";

dotenv.config();

const { Pool } = pg;

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const DEMO_EMAIL = process.env.DEMO_EMAIL || "demo@mastersmon.com";

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

async function getDemoUserId() {
  const rows = await query(
    "SELECT id FROM game.users WHERE email = $1 LIMIT 1",
    [DEMO_EMAIL]
  );

  if (!rows.length) {
    const error = new Error(`Demo user not found: ${DEMO_EMAIL}`);
    error.status = 404;
    throw error;
  }

  return rows[0].id;
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
// Demo player endpoints
// =======================================================

app.get("/api/demo/me", asyncRoute(async (req, res) => {
  const rows = await query(
    "SELECT * FROM game.v_trainer_profile WHERE email = $1 LIMIT 1",
    [DEMO_EMAIL]
  );
  res.json(rows[0] || null);
}));

app.get("/api/demo/inventory", asyncRoute(async (req, res) => {
  const rows = await query(
    "SELECT * FROM game.v_player_inventory WHERE email = $1 ORDER BY category_slug, item_slug",
    [DEMO_EMAIL]
  );
  res.json(rows);
}));

app.get("/api/demo/team", asyncRoute(async (req, res) => {
  const rows = await query(
    "SELECT * FROM game.v_player_team WHERE email = $1 ORDER BY slot_number",
    [DEMO_EMAIL]
  );
  res.json(rows);
}));

app.get("/api/demo/collection", asyncRoute(async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 100), 500);

  const rows = await query(
    `
    SELECT *
    FROM game.v_player_collection
    WHERE email = $1
    ORDER BY captured_at DESC
    LIMIT $2
    `,
    [DEMO_EMAIL, limit]
  );

  res.json(rows);
}));

app.get("/api/demo/pokedex-summary", asyncRoute(async (req, res) => {
  const rows = await query(
    "SELECT * FROM game.v_player_pokedex_summary WHERE email = $1 LIMIT 1",
    [DEMO_EMAIL]
  );
  res.json(rows[0] || null);
}));

app.get("/api/demo/pokedex", asyncRoute(async (req, res) => {
  const generation = req.query.generation ? Number(req.query.generation) : null;
  const caught = req.query.caught;

  const params = [DEMO_EMAIL];
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
}));

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

app.post("/api/demo/encounters", asyncRoute(async (req, res) => {
  const mapSlug = req.body?.mapSlug || req.body?.map_slug || "bosque-verde";
  const userId = await getDemoUserId();

  const rows = await query(
    "SELECT * FROM game.create_wild_encounter($1, $2)",
    [userId, mapSlug]
  );

  res.status(201).json(rows[0]);
}));

app.get("/api/demo/encounters/active", asyncRoute(async (req, res) => {
  const rows = await query(
    `
    SELECT *
    FROM game.v_active_encounters
    WHERE email = $1
    ORDER BY created_at DESC
    LIMIT 20
    `,
    [DEMO_EMAIL]
  );

  res.json(rows);
}));

app.post("/api/demo/captures", asyncRoute(async (req, res) => {
  const encounterId = req.body?.encounterId || req.body?.encounter_id;
  const ballSlug = req.body?.ballSlug || req.body?.ball_slug || "poke-ball";

  if (!encounterId) {
    return res.status(400).json({
      ok: false,
      error: "encounterId is required",
    });
  }

  const userId = await getDemoUserId();

  const rows = await query(
    "SELECT * FROM game.attempt_capture($1, $2, $3)",
    [userId, encounterId, ballSlug]
  );

  res.json(rows[0]);
}));

// Convenience demo endpoint: captures latest active encounter.
app.post("/api/demo/captures/latest", asyncRoute(async (req, res) => {
  const ballSlug = req.body?.ballSlug || req.body?.ball_slug || "poke-ball";

  const rows = await query(
    "SELECT * FROM game.demo_attempt_capture($1)",
    [ballSlug]
  );

  res.json(rows[0]);
}));

// =======================================================
// Server activity
// =======================================================

app.get("/api/server/recent-captures", asyncRoute(async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 20), 100);

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
