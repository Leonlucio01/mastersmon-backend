import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import pg from "pg";
import {
  comparePassword,
  getCurrentUser,
  hashPassword,
  normalizeEmail,
  signToken,
} from "./auth.js";
import { createNewPlayer } from "./playerSetup.js";

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
    : process.env.NODE_ENV === "production" ||
        (!DATABASE_URL.includes("localhost") && !DATABASE_URL.includes("127.0.0.1"))
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

async function authRequired(req, res, next) {
  try {
    req.user = await getCurrentUser(req, query);
    next();
  } catch (error) {
    next(error);
  }
}

function publicUser(user) {
  return {
    id: user.id,
    email: user.email,
  };
}

async function getProfileByUserId(userId) {
  const rows = await query(
    "SELECT * FROM game.v_trainer_profile WHERE user_id = $1 LIMIT 1",
    [userId]
  );
  return rows[0] || null;
}

function validateAuthPayload({ email, password, trainerName }, { requireTrainerName = false } = {}) {
  const normalizedEmail = normalizeEmail(email);

  if (!normalizedEmail || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalizedEmail)) {
    const error = new Error("Valid email is required.");
    error.status = 400;
    error.code = "EMAIL_INVALID";
    throw error;
  }

  if (!password || String(password).length < 6) {
    const error = new Error("Password must be at least 6 characters.");
    error.status = 400;
    error.code = "PASSWORD_TOO_SHORT";
    throw error;
  }

  const normalizedTrainerName = String(trainerName || "").trim();

  if (requireTrainerName && normalizedTrainerName.length < 2) {
    const error = new Error("Trainer name must be at least 2 characters.");
    error.status = 400;
    error.code = "TRAINER_NAME_REQUIRED";
    throw error;
  }

  return {
    email: normalizedEmail,
    password: String(password),
    trainerName: normalizedTrainerName,
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
// Auth
// =======================================================

app.post("/api/auth/register", asyncRoute(async (req, res) => {
  const { email, password, trainerName } = validateAuthPayload(req.body || {}, {
    requireTrainerName: true,
  });

  const existingRows = await query(
    "SELECT id FROM game.users WHERE email = $1 LIMIT 1",
    [email]
  );

  if (existingRows.length) {
    return res.status(409).json({
      ok: false,
      code: "EMAIL_USED",
      error: "Email is already registered.",
    });
  }

  const passwordHash = await hashPassword(password);

  try {
    const { user } = await createNewPlayer(pool, {
      email,
      passwordHash,
      trainerName,
    });
    const profile = await getProfileByUserId(user.id);
    const token = signToken(user);

    res.status(201).json({
      ok: true,
      token,
      user: publicUser(user),
      profile,
    });
  } catch (error) {
    if (error.code === "23505") {
      error.status = 409;
      if (String(error.constraint || "").includes("trainer_profiles_trainer_name")) {
        error.code = "TRAINER_NAME_USED";
        error.message = "Trainer name is already registered.";
      } else {
        error.code = "EMAIL_USED";
        error.message = "Email is already registered.";
      }
    }
    throw error;
  }
}));

app.post("/api/auth/login", asyncRoute(async (req, res) => {
  const { email, password } = validateAuthPayload(req.body || {});

  const rows = await query(
    "SELECT id, email, password_hash FROM game.users WHERE email = $1 AND is_active = true LIMIT 1",
    [email]
  );

  if (!rows.length || !rows[0].password_hash) {
    return res.status(401).json({
      ok: false,
      code: "INVALID_CREDENTIALS",
      error: "Invalid email or password.",
    });
  }

  const valid = await comparePassword(password, rows[0].password_hash);
  if (!valid) {
    return res.status(401).json({
      ok: false,
      code: "INVALID_CREDENTIALS",
      error: "Invalid email or password.",
    });
  }

  const user = {
    id: rows[0].id,
    email: rows[0].email,
  };

  await query(
    "UPDATE game.users SET last_login_at = now(), updated_at = now() WHERE id = $1",
    [user.id]
  );

  const profile = await getProfileByUserId(user.id);
  const token = signToken(user);

  res.json({
    ok: true,
    token,
    user: publicUser(user),
    profile,
  });
}));

app.get("/api/auth/me", authRequired, asyncRoute(async (req, res) => {
  const profile = await getProfileByUserId(req.user.id);

  res.json({
    ok: true,
    user: publicUser(req.user),
    profile,
  });
}));

app.post("/api/auth/logout", (req, res) => {
  res.json({
    ok: true,
  });
});

// =======================================================
// Current player endpoints
// =======================================================

async function sendCurrentProfile(req, res) {
  const rows = await query(
    "SELECT * FROM game.v_trainer_profile WHERE user_id = $1 LIMIT 1",
    [req.user.id]
  );
  res.json(rows[0] || null);
}

async function sendCurrentInventory(req, res) {
  const rows = await query(
    "SELECT * FROM game.v_player_inventory WHERE user_id = $1 ORDER BY category_slug, item_slug",
    [req.user.id]
  );
  res.json(rows);
}

async function sendCurrentTeam(req, res) {
  await ensureTeamSlots(req.user.id);
  const rows = await query(
    "SELECT * FROM game.v_player_team WHERE user_id = $1 ORDER BY slot_number",
    [req.user.id]
  );
  res.json(rows);
}

function parseTeamSlot(value) {
  const slotNumber = Number(value);

  if (!Number.isInteger(slotNumber) || slotNumber < 1 || slotNumber > 6) {
    const error = new Error("slotNumber must be between 1 and 6.");
    error.status = 400;
    error.code = "INVALID_SLOT";
    throw error;
  }

  return slotNumber;
}

async function ensureTeamSlots(userId, client = pool) {
  for (let slotNumber = 1; slotNumber <= 6; slotNumber += 1) {
    await client.query(
      `
      INSERT INTO game.player_team_slots (user_id, slot_number)
      VALUES ($1, $2)
      ON CONFLICT (user_id, slot_number) DO NOTHING
      `,
      [userId, slotNumber]
    );
  }
}

async function getCurrentTeamRows(userId, client = pool) {
  const result = await client.query(
    "SELECT * FROM game.v_player_team WHERE user_id = $1 ORDER BY slot_number",
    [userId]
  );
  return result.rows;
}

async function assertOwnedPlayerMonster(client, userId, playerMonsterId) {
  if (!playerMonsterId || typeof playerMonsterId !== "string") {
    const error = new Error("playerMonsterId is required.");
    error.status = 400;
    error.code = "MONSTER_NOT_FOUND";
    throw error;
  }

  const result = await client.query(
    `
    SELECT id, user_id
    FROM game.player_monsters
    WHERE id = $1
    LIMIT 1
    `,
    [playerMonsterId]
  );

  if (!result.rows.length) {
    const error = new Error("Monster was not found.");
    error.status = 404;
    error.code = "MONSTER_NOT_FOUND";
    throw error;
  }

  if (String(result.rows[0].user_id) !== String(userId)) {
    const error = new Error("Monster does not belong to the current user.");
    error.status = 403;
    error.code = "MONSTER_NOT_OWNED";
    throw error;
  }
}

async function updateTeamSlot(req, res) {
  const slotNumber = parseTeamSlot(req.body?.slotNumber ?? req.body?.slot_number);
  const playerMonsterId = req.body?.playerMonsterId || req.body?.player_monster_id;
  const client = await pool.connect();

  try {
    await assertOwnedPlayerMonster(client, req.user.id, playerMonsterId);

    await client.query("BEGIN");
    await ensureTeamSlots(req.user.id, client);
    await client.query(
      `
      UPDATE game.player_team_slots
      SET player_monster_id = NULL, updated_at = now()
      WHERE user_id = $1
        AND player_monster_id = $2
        AND slot_number <> $3
      `,
      [req.user.id, playerMonsterId, slotNumber]
    );
    await client.query(
      `
      INSERT INTO game.player_team_slots (user_id, slot_number, player_monster_id)
      VALUES ($1, $2, $3)
      ON CONFLICT (user_id, slot_number)
      DO UPDATE SET
        player_monster_id = EXCLUDED.player_monster_id,
        updated_at = now()
      `,
      [req.user.id, slotNumber, playerMonsterId]
    );
    await client.query("COMMIT");

    res.json(await getCurrentTeamRows(req.user.id));
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    if (!error.code || error.code === "23505") {
      error.status = error.status || 500;
      error.code = error.code === "23505" ? "TEAM_UPDATE_FAILED" : (error.code || "TEAM_UPDATE_FAILED");
    }
    throw error;
  } finally {
    client.release();
  }
}

async function clearTeamSlot(req, res) {
  const slotNumber = parseTeamSlot(req.params.slotNumber);
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await ensureTeamSlots(req.user.id, client);
    await client.query(
      `
      UPDATE game.player_team_slots
      SET player_monster_id = NULL, updated_at = now()
      WHERE user_id = $1
        AND slot_number = $2
      `,
      [req.user.id, slotNumber]
    );
    await client.query("COMMIT");

    res.json(await getCurrentTeamRows(req.user.id));
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    error.status = error.status || 500;
    error.code = error.code || "TEAM_UPDATE_FAILED";
    throw error;
  } finally {
    client.release();
  }
}

async function autoBuildTeam(req, res) {
  const client = await pool.connect();

  try {
    const best = await client.query(
      `
      SELECT pm.id
      FROM game.player_monsters pm
      JOIN game.monster_species ms ON ms.id = pm.species_id
      WHERE pm.user_id = $1
      ORDER BY
        pm.level DESC,
        CASE LOWER(COALESCE(ms.rarity, 'common'))
          WHEN 'mythic' THEN 6
          WHEN 'legendary' THEN 5
          WHEN 'legend' THEN 5
          WHEN 'epic' THEN 4
          WHEN 'rare' THEN 3
          WHEN 'uncommon' THEN 2
          ELSE 1
        END DESC,
        pm.captured_at ASC,
        pm.id ASC
      LIMIT 6
      `,
      [req.user.id]
    );

    await client.query("BEGIN");
    await ensureTeamSlots(req.user.id, client);
    await client.query(
      `
      UPDATE game.player_team_slots
      SET player_monster_id = NULL, updated_at = now()
      WHERE user_id = $1
      `,
      [req.user.id]
    );

    for (let index = 0; index < best.rows.length; index += 1) {
      await client.query(
        `
        UPDATE game.player_team_slots
        SET player_monster_id = $3, updated_at = now()
        WHERE user_id = $1
          AND slot_number = $2
        `,
        [req.user.id, index + 1, best.rows[index].id]
      );
    }

    await client.query("COMMIT");

    res.json(await getCurrentTeamRows(req.user.id));
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    error.status = error.status || 500;
    error.code = error.code || "TEAM_UPDATE_FAILED";
    throw error;
  } finally {
    client.release();
  }
}

async function sendCurrentCollection(req, res) {
  const limit = getLimit(req.query.limit, 100, 500);

  const rows = await query(
    `
    SELECT *
    FROM game.v_player_collection
    WHERE user_id = $1
    ORDER BY captured_at DESC
    LIMIT $2
    `,
    [req.user.id, limit]
  );

  res.json(rows);
}

async function sendCurrentPokedexSummary(req, res) {
  const rows = await query(
    "SELECT * FROM game.v_player_pokedex_summary WHERE user_id = $1 LIMIT 1",
    [req.user.id]
  );
  res.json(rows[0] || null);
}

async function sendCurrentPokedex(req, res) {
  const generation = req.query.generation ? Number(req.query.generation) : null;
  const caught = req.query.caught;

  const params = [req.user.id];
  let where = "user_id = $1";

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

app.get("/api/me", authRequired, asyncRoute(sendCurrentProfile));
app.get("/api/me/inventory", authRequired, asyncRoute(sendCurrentInventory));
app.get("/api/me/team", authRequired, asyncRoute(sendCurrentTeam));
app.post("/api/me/team/slots", authRequired, asyncRoute(updateTeamSlot));
app.delete("/api/me/team/slots/:slotNumber", authRequired, asyncRoute(clearTeamSlot));
app.post("/api/me/team/auto", authRequired, asyncRoute(autoBuildTeam));
app.get("/api/me/collection", authRequired, asyncRoute(sendCurrentCollection));
app.get("/api/me/pokedex-summary", authRequired, asyncRoute(sendCurrentPokedexSummary));
app.get("/api/me/pokedex", authRequired, asyncRoute(sendCurrentPokedex));

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

  const rows = await query(
    "SELECT * FROM game.create_wild_encounter($1, $2)",
    [req.user.id, mapSlug]
  );

  res.status(201).json(rows[0]);
}

async function sendActiveEncounters(req, res) {
  const rows = await query(
    `
    SELECT *
    FROM game.v_active_encounters
    WHERE user_id = $1
    ORDER BY created_at DESC
    LIMIT 20
    `,
    [req.user.id]
  );

  res.json(rows);
}

async function captureEncounter(req, res) {
  const encounterId = req.body?.encounterId || req.body?.encounter_id;
  const ballSlug = req.body?.ballSlug || req.body?.ball_slug || "poke-ball";

  if (!encounterId) {
    return res.status(400).json({
      ok: false,
      code: "ENCOUNTER_ID_REQUIRED",
      error: "encounterId is required",
    });
  }

  const rows = await query(
    "SELECT * FROM game.attempt_capture($1, $2, $3)",
    [req.user.id, encounterId, ballSlug]
  );

  res.json(rows[0]);
}

async function captureLatestActiveEncounter(req, res) {
  const ballSlug = req.body?.ballSlug || req.body?.ball_slug || "poke-ball";
  const activeRows = await query(
    `
    SELECT encounter_id
    FROM game.v_active_encounters
    WHERE user_id = $1
    ORDER BY created_at DESC
    LIMIT 1
    `,
    [req.user.id]
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

app.post("/api/encounters", authRequired, asyncRoute(createEncounter));
app.get("/api/encounters/active", authRequired, asyncRoute(sendActiveEncounters));
app.post("/api/captures", authRequired, asyncRoute(captureEncounter));

// Legacy demo endpoints. Keep temporarily for backward compatibility.
app.get("/api/demo/me", authRequired, asyncRoute(sendCurrentProfile));
app.get("/api/demo/inventory", authRequired, asyncRoute(sendCurrentInventory));
app.get("/api/demo/team", authRequired, asyncRoute(sendCurrentTeam));
app.get("/api/demo/collection", authRequired, asyncRoute(sendCurrentCollection));
app.get("/api/demo/pokedex-summary", authRequired, asyncRoute(sendCurrentPokedexSummary));
app.get("/api/demo/pokedex", authRequired, asyncRoute(sendCurrentPokedex));
app.post("/api/demo/encounters", authRequired, asyncRoute(createEncounter));
app.get("/api/demo/encounters/active", authRequired, asyncRoute(sendActiveEncounters));
app.post("/api/demo/captures", authRequired, asyncRoute(captureEncounter));
app.post("/api/demo/captures/latest", authRequired, asyncRoute(captureLatestActiveEncounter));

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
    code: error.code || "INTERNAL_ERROR",
    error: error.message || "Internal server error",
  });
});

app.listen(PORT, () => {
  console.log(`MastersMon API running on port ${PORT}`);
});
