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

function createHttpError(status, code, message) {
  const error = new Error(message);
  error.status = status;
  error.code = code;
  return error;
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
    await ensurePlayerQuests(user.id);
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
    const teamSize = await getTeamSize(req.user.id, client);
    await incrementQuestProgress(req.user.id, "team_update", { teamSize }, client);
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
    const teamSize = await getTeamSize(req.user.id, client);
    await incrementQuestProgress(req.user.id, "team_update", { teamSize }, client);
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

    const teamSize = await getTeamSize(req.user.id, client);
    await incrementQuestProgress(req.user.id, "team_update", { teamSize }, client);
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
// Quests
// =======================================================

async function ensurePlayerQuests(userId, client = pool) {
  await client.query(
    `
    INSERT INTO game.player_quests (user_id, quest_id, progress, completed, claimed, status)
    SELECT $1, q.id, 0, false, false, 'active'
    FROM game.quests q
    WHERE q.is_active = true
    ON CONFLICT (user_id, quest_id) DO NOTHING
    `,
    [userId]
  );
}

async function getTeamSize(userId, client = pool) {
  const result = await client.query(
    `
    SELECT COUNT(*)::int AS count
    FROM game.player_team_slots
    WHERE user_id = $1
      AND player_monster_id IS NOT NULL
    `,
    [userId]
  );
  return Number(result.rows[0]?.count || 0);
}

async function getCaughtSpeciesCount(userId, client = pool) {
  const result = await client.query(
    `
    SELECT COUNT(*)::int AS count
    FROM game.player_pokedex
    WHERE user_id = $1
      AND caught = true
    `,
    [userId]
  );
  return Number(result.rows[0]?.count || 0);
}

async function completeEligibleQuests(userId, client = pool) {
  await client.query(
    `
    UPDATE game.player_quests pq
    SET
      completed = true,
      status = 'completed',
      completed_at = COALESCE(pq.completed_at, now()),
      updated_at = now()
    FROM game.quests q
    WHERE q.id = pq.quest_id
      AND pq.user_id = $1
      AND pq.claimed = false
      AND COALESCE(pq.status, 'active') = 'active'
      AND pq.progress >= q.target_value
    `,
    [userId]
  );
}

async function updateQuestProgressByAmount(userId, targetType, amount, client = pool, targetItemSlug = null) {
  const delta = Math.max(0, Number(amount || 0));
  if (!delta) return;

  await client.query(
    `
    UPDATE game.player_quests pq
    SET
      progress = LEAST(q.target_value, pq.progress + $3),
      updated_at = now()
    FROM game.quests q
    WHERE q.id = pq.quest_id
      AND pq.user_id = $1
      AND q.is_active = true
      AND q.target_type = $2
      AND pq.claimed = false
      AND COALESCE(pq.status, 'active') = 'active'
      AND ($4::text IS NULL OR q.target_item_slug IS NULL OR q.target_item_slug = $4)
    `,
    [userId, targetType, delta, targetItemSlug]
  );
}

async function updateQuestProgressMax(userId, targetType, value, client = pool) {
  const progress = Math.max(0, Number(value || 0));

  await client.query(
    `
    UPDATE game.player_quests pq
    SET
      progress = LEAST(q.target_value, GREATEST(pq.progress, $3)),
      updated_at = now()
    FROM game.quests q
    WHERE q.id = pq.quest_id
      AND pq.user_id = $1
      AND q.is_active = true
      AND q.target_type = $2
      AND pq.claimed = false
      AND COALESCE(pq.status, 'active') = 'active'
    `,
    [userId, targetType, progress]
  );
}

async function incrementQuestProgress(userId, eventType, payload = {}, client = pool) {
  await ensurePlayerQuests(userId, client);

  if (eventType === "capture") {
    await updateQuestProgressByAmount(userId, "capture", 1, client);

    const typeSlugs = [
      payload.primaryTypeSlug,
      payload.secondaryTypeSlug,
    ].filter(Boolean);

    if (typeSlugs.length) {
      await client.query(
        `
        UPDATE game.player_quests pq
        SET
          progress = LEAST(q.target_value, pq.progress + 1),
          updated_at = now()
        FROM game.quests q
        WHERE q.id = pq.quest_id
          AND pq.user_id = $1
          AND q.is_active = true
          AND q.target_type = 'capture_type'
          AND q.target_type_slug = ANY($2::text[])
          AND pq.claimed = false
          AND COALESCE(pq.status, 'active') = 'active'
        `,
        [userId, typeSlugs]
      );
    }

    if (payload.isShiny) {
      await updateQuestProgressByAmount(userId, "capture_shiny", 1, client);
    }
  } else if (eventType === "buy_item") {
    await updateQuestProgressByAmount(userId, "buy_item", payload.quantity || 1, client, payload.itemSlug || null);
  } else if (eventType === "use_item") {
    await updateQuestProgressByAmount(userId, "use_item", payload.quantity || 1, client, payload.itemSlug || null);
  } else if (eventType === "evolve") {
    await updateQuestProgressByAmount(userId, "evolve", 1, client);
  } else if (eventType === "battle_win") {
    await updateQuestProgressByAmount(userId, "battle_win", 1, client);
  } else if (eventType === "gym_win") {
    await updateQuestProgressByAmount(userId, "gym_win", 1, client);
  } else if (eventType === "badge_earned") {
    await updateQuestProgressByAmount(userId, "badge_earned", 1, client);
  } else if (eventType === "team_update") {
    await updateQuestProgressMax(userId, "team_size", payload.teamSize || 0, client);
  } else if (eventType === "pokedex_species") {
    await updateQuestProgressMax(userId, "pokedex_species", payload.caughtSpeciesCount || 0, client);
  }

  await completeEligibleQuests(userId, client);
}

async function syncComputedQuestProgress(userId, client = pool) {
  await ensurePlayerQuests(userId, client);

  const captureCountResult = await client.query(
    "SELECT COUNT(*)::int AS count FROM game.capture_logs WHERE user_id = $1",
    [userId]
  );
  await updateQuestProgressMax(userId, "capture", captureCountResult.rows[0]?.count || 0, client);

  const shinyCountResult = await client.query(
    "SELECT COUNT(*)::int AS count FROM game.capture_logs WHERE user_id = $1 AND is_shiny = true",
    [userId]
  );
  await updateQuestProgressMax(userId, "capture_shiny", shinyCountResult.rows[0]?.count || 0, client);

  await updateQuestProgressMax(userId, "team_size", await getTeamSize(userId, client), client);
  await updateQuestProgressMax(userId, "pokedex_species", await getCaughtSpeciesCount(userId, client), client);
  await completeEligibleQuests(userId, client);
}

function questStatus(row) {
  if (row.claimed || row.status === "claimed") return "claimed";
  if (row.completed || row.status === "completed") return "completed";
  return "active";
}

function formatQuest(row) {
  const targetCount = Number(row.target_count || 0);
  const progress = Math.min(Number(row.progress || 0), targetCount || Number(row.progress || 0));
  const status = questStatus(row);

  return {
    quest_id: row.quest_id,
    slug: row.slug,
    title: row.title,
    description: row.description,
    quest_type: row.target_type,
    category: row.quest_category,
    target_type_slug: row.target_type_slug,
    target_item_slug: row.target_item_slug,
    progress,
    target_count: targetCount,
    status,
    completed_at: row.completed_at,
    claimed_at: row.claimed_at,
    rewards: {
      gold: Number(row.reward_gold || 0),
      diamonds: Number(row.reward_diamonds || 0),
      itemSlug: row.reward_item_slug,
      itemName: row.reward_item_name,
      itemQuantity: Number(row.reward_item_quantity || 0),
    },
    percent: targetCount > 0 ? Math.min(100, Math.round((progress / targetCount) * 100)) : 0,
    can_claim: status === "completed",
  };
}

async function getQuestRows(userId, client = pool) {
  const result = await client.query(
    `
    SELECT
      q.id AS quest_id,
      q.slug,
      q.name AS title,
      q.description,
      q.quest_type AS quest_category,
      q.target_type,
      q.target_value AS target_count,
      q.target_type_slug,
      q.target_item_slug,
      q.reward_gold,
      q.reward_diamonds,
      q.reward_item_quantity,
      ri.slug AS reward_item_slug,
      COALESCE(ri.display_name, ri.name) AS reward_item_name,
      pq.progress,
      pq.completed,
      pq.claimed,
      pq.status,
      pq.completed_at,
      pq.claimed_at
    FROM game.player_quests pq
    JOIN game.quests q ON q.id = pq.quest_id
    LEFT JOIN game.items ri ON ri.id = q.reward_item_id
    WHERE pq.user_id = $1
      AND q.is_active = true
    ORDER BY
      CASE
        WHEN pq.claimed OR pq.status = 'claimed' THEN 3
        WHEN pq.completed OR pq.status = 'completed' THEN 1
        ELSE 2
      END,
      CASE WHEN q.target_value > 0 THEN pq.progress::numeric / q.target_value ELSE 0 END DESC,
      q.sort_order,
      q.created_at
    `,
    [userId]
  );
  return result.rows;
}

async function sendCurrentQuests(req, res) {
  await syncComputedQuestProgress(req.user.id);
  const rows = await getQuestRows(req.user.id);
  res.json(rows.map(formatQuest));
}

async function claimQuestReward(req, res) {
  const questId = String(req.params.questId || "").trim();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await ensurePlayerQuests(req.user.id, client);

    const questResult = await client.query(
      `
      SELECT
        q.id AS quest_id,
        q.slug,
        q.name AS title,
        q.description,
        q.quest_type AS quest_category,
        q.target_type,
        q.target_value AS target_count,
        q.target_type_slug,
        q.target_item_slug,
        q.reward_gold,
        q.reward_diamonds,
        q.reward_item_quantity,
        ri.id AS reward_item_id,
        ri.slug AS reward_item_slug,
        COALESCE(ri.display_name, ri.name) AS reward_item_name,
        pq.progress,
        pq.completed,
        pq.claimed,
        pq.status,
        pq.completed_at,
        pq.claimed_at
      FROM game.player_quests pq
      JOIN game.quests q ON q.id = pq.quest_id
      LEFT JOIN game.items ri ON ri.id = q.reward_item_id
      WHERE pq.user_id = $1
        AND (q.id::text = $2 OR q.slug = $2)
      LIMIT 1
      FOR UPDATE OF pq
      `,
      [req.user.id, questId]
    );

    if (!questResult.rows.length) {
      throw createHttpError(404, "QUEST_NOT_FOUND", "Quest was not found.");
    }

    const quest = questResult.rows[0];
    const status = questStatus(quest);

    if (status === "claimed") {
      throw createHttpError(400, "QUEST_ALREADY_CLAIMED", "Quest reward was already claimed.");
    }

    if (status !== "completed" && Number(quest.progress || 0) < Number(quest.target_count || 0)) {
      throw createHttpError(400, "QUEST_NOT_COMPLETED", "Quest is not completed yet.");
    }

    let wallet = null;
    if (Number(quest.reward_gold || 0) > 0 || Number(quest.reward_diamonds || 0) > 0) {
      const walletResult = await client.query(
        `
        UPDATE game.trainer_wallets
        SET
          gold = gold + $2,
          diamonds = diamonds + $3,
          updated_at = now()
        WHERE user_id = $1
        RETURNING user_id, gold, diamonds, boss_tickets, season_exp, updated_at
        `,
        [req.user.id, Number(quest.reward_gold || 0), Number(quest.reward_diamonds || 0)]
      );
      wallet = walletResult.rows[0] || null;

      if (Number(quest.reward_gold || 0) > 0) {
        await client.query(
          `
          INSERT INTO game.wallet_transactions (user_id, currency, amount, reason, reference_type, reference_id)
          VALUES ($1, 'gold', $2, 'quest_reward', 'quest', $3)
          `,
          [req.user.id, Number(quest.reward_gold || 0), quest.quest_id]
        );
      }

      if (Number(quest.reward_diamonds || 0) > 0) {
        await client.query(
          `
          INSERT INTO game.wallet_transactions (user_id, currency, amount, reason, reference_type, reference_id)
          VALUES ($1, 'diamonds', $2, 'quest_reward', 'quest', $3)
          `,
          [req.user.id, Number(quest.reward_diamonds || 0), quest.quest_id]
        );
      }
    }

    if (quest.reward_item_id && Number(quest.reward_item_quantity || 0) > 0) {
      await client.query(
        `
        INSERT INTO game.player_inventory (user_id, item_id, quantity)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id, item_id)
        DO UPDATE SET
          quantity = game.player_inventory.quantity + EXCLUDED.quantity,
          updated_at = now()
        `,
        [req.user.id, quest.reward_item_id, Number(quest.reward_item_quantity)]
      );
    }

    await client.query(
      `
      UPDATE game.player_quests
      SET
        claimed = true,
        status = 'claimed',
        claimed_at = now(),
        completed = true,
        completed_at = COALESCE(completed_at, now()),
        updated_at = now()
      WHERE user_id = $1
        AND quest_id = $2
      `,
      [req.user.id, quest.quest_id]
    );

    await client.query("COMMIT");

    const [questRows, inventoryRows, walletRows] = await Promise.all([
      getQuestRows(req.user.id),
      getCurrentInventoryRows(req.user.id),
      wallet ? Promise.resolve([wallet]) : query("SELECT * FROM game.trainer_wallets WHERE user_id = $1 LIMIT 1", [req.user.id]),
    ]);
    const claimedQuest = questRows.find((row) => String(row.quest_id) === String(quest.quest_id));

    res.json({
      ok: true,
      quest: claimedQuest ? formatQuest(claimedQuest) : null,
      rewards: formatQuest(quest).rewards,
      wallet: walletRows[0] || null,
      inventory: inventoryRows,
    });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    if (!error.code || error.code === "23514" || error.code === "23503") {
      error.status = error.status || 500;
      error.code = "QUEST_REWARD_FAILED";
    }
    throw error;
  } finally {
    client.release();
  }
}

app.get("/api/me/quests", authRequired, asyncRoute(sendCurrentQuests));
app.post("/api/me/quests/:questId/claim", authRequired, asyncRoute(claimQuestReward));

// =======================================================
// PvE Battles / skills
// =======================================================

const GYM_ALIASES = {
  brock: "kanto-boulder-badge",
  misty: "kanto-cascade-badge",
  surge: "kanto-thunder-badge",
  "lt-surge": "kanto-thunder-badge",
  erika: "kanto-rainbow-badge",
};

function battleMaxHp(monster) {
  return 20 + Number(monster.level || 1) * 3 + Number(monster.iv_hp || 0);
}

function hpPercent(monster) {
  const maxHp = Number(monster?.max_hp || 1);
  return Math.max(0, Math.min(100, Math.round((Number(monster?.current_hp || 0) / maxHp) * 100)));
}

function typeEffectiveness(attackType, defender) {
  const atk = String(attackType || "").toLowerCase();
  const defenderTypes = [defender.primary_type, defender.secondary_type].filter(Boolean).map((t) => String(t).toLowerCase());

  if (atk === "normal" && defenderTypes.includes("ghost")) return 0;

  const strong = {
    fire: ["grass", "bug", "ice"],
    water: ["fire", "rock", "ground"],
    grass: ["water", "rock", "ground"],
    electric: ["water", "flying"],
    rock: ["fire", "flying", "bug", "ice"],
    ground: ["electric", "fire", "poison", "rock"],
    psychic: ["poison", "fighting"],
    ice: ["dragon", "flying", "grass", "ground"],
    bug: ["grass", "psychic", "dark"],
    ghost: ["ghost", "psychic"],
    dark: ["psychic", "ghost"],
    fighting: ["normal", "rock", "ice", "dark"],
  };
  const resist = {
    fire: ["fire", "water", "rock", "dragon"],
    water: ["water", "grass", "dragon"],
    grass: ["fire", "grass", "poison", "flying", "bug", "dragon"],
    electric: ["electric", "grass", "dragon", "ground"],
    rock: ["fighting", "ground", "steel"],
    ghost: ["dark"],
  };

  let multiplier = 1;
  for (const type of defenderTypes) {
    if (strong[atk]?.includes(type)) multiplier *= 2;
    if (resist[atk]?.includes(type)) multiplier *= 0.5;
  }
  return multiplier;
}

function calculateBattleDamage(attacker, defender, skill) {
  const level = Number(attacker.level || 1);
  const attackStat = 10 + level * 2 + Number(attacker.iv_attack || 0);
  const defenseStat = 8 + Number(defender.level || 1) + Number(defender.iv_defense || 0);
  const basePower = Number(skill.power || 40);
  const baseDamage = Math.floor(((((level * 0.4 + 2) * basePower * attackStat) / Math.max(1, defenseStat)) / 8) + 2);
  const randomFactor = 0.85 + Math.random() * 0.15;
  const stab = skill.type_slug && [attacker.primary_type, attacker.secondary_type].includes(skill.type_slug) ? 1.2 : 1;
  const typeMultiplier = typeEffectiveness(skill.type_slug, defender);
  const isCritical = Math.random() < 0.05;
  const criticalMultiplier = isCritical ? 1.5 : 1;
  const damage = typeMultiplier === 0 ? 0 : Math.max(1, Math.floor(baseDamage * randomFactor * stab * typeMultiplier * criticalMultiplier));

  return {
    damage,
    isCritical,
    typeMultiplier,
    randomFactor,
    stab,
  };
}

async function getSkillsForSpecies(speciesId, level, client = pool) {
  const result = await client.query(
    `
    SELECT
      s.id AS skill_id,
      s.slug,
      s.name,
      s.description,
      s.type_slug,
      s.power,
      s.accuracy,
      s.energy_cost,
      s.cooldown_turns,
      s.skill_kind
    FROM game.monster_species_skills mss
    JOIN game.skills s ON s.id = mss.skill_id
    WHERE mss.species_id = $1
      AND mss.unlock_level <= $2
      AND s.is_active = true
    ORDER BY mss.slot_order, s.power DESC, s.slug
    LIMIT 4
    `,
    [speciesId, Number(level || 1)]
  );

  if (result.rows.length) return result.rows;

  const fallback = await client.query(
    `
    SELECT
      id AS skill_id,
      slug,
      name,
      description,
      type_slug,
      power,
      accuracy,
      energy_cost,
      cooldown_turns,
      skill_kind
    FROM game.skills
    WHERE slug = 'tackle'
    LIMIT 1
    `
  );
  return fallback.rows;
}

async function getPlayerMonsterWithSpecies(userId, playerMonsterId, client = pool) {
  const result = await client.query(
    `
    SELECT
      pm.id AS player_monster_id,
      pm.user_id,
      pm.species_id,
      pm.nickname,
      pm.level,
      pm.exp,
      pm.current_hp,
      pm.iv_hp,
      pm.iv_attack,
      pm.iv_defense,
      pm.is_shiny,
      ms.dex_number,
      ms.name AS pokemon_name,
      ms.slug AS pokemon_slug,
      ms.sprite_path,
      ms.shiny_sprite_path,
      ms.animated_path,
      ms.animated_shiny_path,
      CASE WHEN pm.is_shiny THEN COALESCE(ms.animated_shiny_path, ms.shiny_sprite_path, ms.animated_path, ms.sprite_path)
        ELSE COALESCE(ms.animated_path, ms.sprite_path) END AS selected_sprite_path,
      pt.slug AS primary_type,
      st.slug AS secondary_type
    FROM game.player_monsters pm
    JOIN game.monster_species ms ON ms.id = pm.species_id
    LEFT JOIN game.monster_types pt ON pt.id = ms.primary_type_id
    LEFT JOIN game.monster_types st ON st.id = ms.secondary_type_id
    WHERE pm.id = $1
      AND pm.user_id = $2
    LIMIT 1
    `,
    [playerMonsterId, userId]
  );
  return result.rows[0] || null;
}

async function getMonsterBattleSkills(req, res) {
  const playerMonsterId = String(req.params.playerMonsterId || "").trim();
  const monster = await getPlayerMonsterWithSpecies(req.user.id, playerMonsterId);

  if (!monster) {
    throw createHttpError(404, "MONSTER_NOT_FOUND", "Monster was not found.");
  }

  const skills = await getSkillsForSpecies(monster.species_id, monster.level);
  res.json({
    ok: true,
    monster,
    skills,
  });
}

function asBattleMonster(row, side, index, skills) {
  const maxHp = battleMaxHp(row);
  const currentHp = row.current_hp === null || row.current_hp === undefined
    ? maxHp
    : Math.max(0, Math.min(maxHp, Number(row.current_hp)));

  return {
    side,
    index,
    player_monster_id: row.player_monster_id || null,
    species_id: row.species_id,
    dex_number: row.dex_number,
    pokemon_name: row.nickname || row.pokemon_name,
    species_name: row.pokemon_name,
    level: Number(row.level || 1),
    current_hp: currentHp,
    max_hp: maxHp,
    iv_hp: Number(row.iv_hp || 0),
    iv_attack: Number(row.iv_attack || 0),
    iv_defense: Number(row.iv_defense || 0),
    is_shiny: !!row.is_shiny,
    primary_type: row.primary_type,
    secondary_type: row.secondary_type,
    selected_sprite_path: row.selected_sprite_path,
    skills,
  };
}

async function getPlayerBattleTeam(userId, client = pool) {
  await ensureTeamSlots(userId, client);
  const result = await client.query(
    `
    SELECT
      pts.slot_number,
      pm.id AS player_monster_id,
      pm.species_id,
      pm.nickname,
      pm.level,
      pm.current_hp,
      pm.iv_hp,
      pm.iv_attack,
      pm.iv_defense,
      pm.is_shiny,
      ms.dex_number,
      ms.name AS pokemon_name,
      CASE WHEN pm.is_shiny THEN COALESCE(ms.animated_shiny_path, ms.shiny_sprite_path, ms.animated_path, ms.sprite_path)
        ELSE COALESCE(ms.animated_path, ms.sprite_path) END AS selected_sprite_path,
      pt.slug AS primary_type,
      st.slug AS secondary_type
    FROM game.player_team_slots pts
    JOIN game.player_monsters pm ON pm.id = pts.player_monster_id
    JOIN game.monster_species ms ON ms.id = pm.species_id
    LEFT JOIN game.monster_types pt ON pt.id = ms.primary_type_id
    LEFT JOIN game.monster_types st ON st.id = ms.secondary_type_id
    WHERE pts.user_id = $1
    ORDER BY pts.slot_number
    `,
    [userId]
  );

  const team = [];
  for (let index = 0; index < result.rows.length; index += 1) {
    const row = result.rows[index];
    const skills = await getSkillsForSpecies(row.species_id, row.level, client);
    team.push(asBattleMonster(row, "player", index, skills));
  }
  return team;
}

function gymBadgeSlug(gym) {
  return `badge_${String(gym.slug || "").replace(/-/g, "_")}`;
}

function gymRewardFor(gym, firstClear = true) {
  if (!firstClear) {
    return {
      gold: 500,
      diamonds: 0,
      items: [],
      first_clear: false,
    };
  }

  const order = Number(gym.gym_order || 1);
  const rewardsByOrder = {
    1: { gold: 2500, items: [{ slug: "great-ball", quantity: 5 }] },
    2: { gold: 3500, items: [{ slug: "rare-candy", quantity: 1 }] },
    3: { gold: 5000, items: [{ slug: "thunder-stone", quantity: 1 }] },
  };
  const configured = rewardsByOrder[order] || {
    gold: Math.max(2500, order * 1500),
    items: order % 2 === 0 ? [{ slug: "great-ball", quantity: 3 }] : [],
  };

  return {
    gold: configured.gold,
    diamonds: 0,
    items: configured.items,
    first_clear: true,
  };
}

const rewardItemCache = new Map();

async function decorateRewardItems(reward, client = pool) {
  const items = reward.items || [];
  if (!items.length) return { ...reward, items: [] };

  const slugs = items.map((item) => item.slug);
  const missingSlugs = slugs.filter((slug) => slug && !rewardItemCache.has(slug));
  if (missingSlugs.length) {
    const result = await client.query(
      `
      SELECT slug, display_name, name, icon_path
      FROM game.items
      WHERE slug = ANY($1::text[])
      `,
      [missingSlugs]
    );
    result.rows.forEach((row) => rewardItemCache.set(row.slug, row));
  }

  return {
    ...reward,
    items: items.map((item) => {
      const row = rewardItemCache.get(item.slug) || {};
      return {
        slug: item.slug,
        quantity: Number(item.quantity || 0),
        display_name: row.display_name || row.name || item.slug,
        icon_path: row.icon_path || null,
      };
    }),
  };
}

async function getGymRowsForUser(userId, client = pool) {
  const result = await client.query(
    `
    WITH ordered_gyms AS (
      SELECT
        g.id,
        g.slug,
        g.name,
        g.gym_order,
        g.badge_name,
        g.badge_icon_path,
        g.recommended_power,
        g.required_trainer_level,
        g.region_id,
        r.slug AS region_slug,
        r.name AS region_name,
        r.generation_id,
        mt.slug AS type_slug,
        mt.name AS type_name,
        nt.slug AS leader_slug,
        nt.name AS leader_name,
        nt.avatar_path AS leader_avatar_path,
        COUNT(gtt.id)::int AS team_size,
        ROW_NUMBER() OVER (
          PARTITION BY g.region_id
          ORDER BY g.gym_order, g.name, g.id
        ) AS region_order,
        LAG(g.id) OVER (
          PARTITION BY g.region_id
          ORDER BY g.gym_order, g.name, g.id
        ) AS previous_gym_id
      FROM game.gyms g
      LEFT JOIN game.regions r ON r.id = g.region_id
      LEFT JOIN game.monster_types mt ON mt.id = g.type_id
      LEFT JOIN game.npc_trainers nt ON nt.id = g.leader_id
      LEFT JOIN game.gym_trainer_team gtt ON gtt.gym_id = g.id
      GROUP BY g.id, r.slug, r.name, r.generation_id, mt.slug, mt.name, nt.slug, nt.name, nt.avatar_path
    )
    SELECT
      og.*,
      pgp.status AS progress_status,
      pgp.wins,
      pgp.best_turns,
      pgp.completed_at,
      prev.status AS previous_status,
      prev.completed_at AS previous_completed_at
    FROM ordered_gyms og
    LEFT JOIN game.player_gym_progress pgp
      ON pgp.user_id = $1
     AND pgp.gym_id = og.id
    LEFT JOIN game.player_gym_progress prev
      ON prev.user_id = $1
     AND prev.gym_id = og.previous_gym_id
    ORDER BY COALESCE(og.generation_id, 1), og.gym_order, og.name
    `,
    [userId]
  );

  const rows = [];
  for (const row of result.rows) {
    const isCompleted = row.progress_status === "completed";
    const isUnlocked = Number(row.region_order || 1) === 1 || row.previous_status === "completed";
    const reward = await decorateRewardItems(gymRewardFor(row, true), client);
    rows.push({
      ...row,
      gym_id: row.id,
      status: isCompleted ? "completed" : isUnlocked ? "available" : "locked",
      is_unlocked: isUnlocked,
      is_completed: isCompleted,
      wins: Number(row.wins || 0),
      reward_gold: reward.gold,
      reward_items: reward.items,
      repeat_reward_gold: 500,
      required_previous_gym: row.previous_gym_id,
    });
  }
  return rows;
}

async function getGymProgressState(userId, gymId, client = pool) {
  const rows = await getGymRowsForUser(userId, client);
  return rows.find((row) => String(row.id) === String(gymId)) || null;
}

async function getGyms(req, res) {
  const rows = await getGymRowsForUser(req.user.id);
  res.json(rows);
}

async function getGymProgress(req, res) {
  const gyms = await getGymRowsForUser(req.user.id);
  const completed = gyms.filter((gym) => gym.is_completed);
  const nextGym = gyms.find((gym) => gym.is_unlocked && !gym.is_completed) || null;
  const badges = await getPlayerBadgesRows(req.user.id);

  res.json({
    ok: true,
    total_gyms: gyms.length,
    completed_gyms: completed.length,
    badges,
    gyms,
    next_gym: nextGym,
  });
}

async function getPlayerBadgesRows(userId, client = pool) {
  const result = await client.query(
    `
    SELECT
      a.slug,
      a.name,
      a.description,
      pa.unlocked_at AS earned_at,
      g.slug AS gym_slug,
      g.badge_icon_path AS icon_path,
      g.badge_name,
      r.slug AS region_slug,
      r.name AS region_name,
      g.gym_order
    FROM game.player_achievements pa
    JOIN game.achievements a ON a.id = pa.achievement_id
    LEFT JOIN game.gyms g ON a.slug = ('badge_' || replace(g.slug, '-', '_'))
    LEFT JOIN game.regions r ON r.id = g.region_id
    WHERE pa.user_id = $1
      AND pa.unlocked = true
      AND a.target_type = 'gym_badge'
    ORDER BY COALESCE(r.generation_id, 1), g.gym_order, a.name
    `,
    [userId]
  );

  return result.rows.map((row) => ({
    slug: row.slug,
    name: row.badge_name || row.name,
    description: row.description,
    icon_path: row.icon_path || null,
    earned_at: row.earned_at,
    gym_slug: row.gym_slug,
    region_slug: row.region_slug,
    region_name: row.region_name,
    gym_order: row.gym_order,
  }));
}

async function getPlayerBadges(req, res) {
  const badges = await getPlayerBadgesRows(req.user.id);
  res.json({
    ok: true,
    badges,
  });
}

async function findGymForBattle(targetSlug, client = pool) {
  const slug = GYM_ALIASES[String(targetSlug || "").toLowerCase()] || String(targetSlug || "").toLowerCase();
  const result = await client.query(
    `
    SELECT
      g.*,
      mt.slug AS type_slug,
      mt.name AS type_name,
      nt.id AS leader_id,
      nt.slug AS leader_slug,
      nt.name AS leader_name
    FROM game.gyms g
    LEFT JOIN game.monster_types mt ON mt.id = g.type_id
    LEFT JOIN game.npc_trainers nt ON nt.id = g.leader_id
    WHERE g.slug = $1
    LIMIT 1
    `,
    [slug]
  );
  return result.rows[0] || null;
}

async function getGymEnemyRows(gym, client = pool) {
  const teamResult = await client.query(
    `
    SELECT
      gtt.species_id,
      gtt.level,
      ms.dex_number,
      ms.name AS pokemon_name,
      COALESCE(ms.animated_path, ms.sprite_path) AS selected_sprite_path,
      pt.slug AS primary_type,
      st.slug AS secondary_type
    FROM game.gym_trainer_team gtt
    JOIN game.monster_species ms ON ms.id = gtt.species_id
    LEFT JOIN game.monster_types pt ON pt.id = ms.primary_type_id
    LEFT JOIN game.monster_types st ON st.id = ms.secondary_type_id
    WHERE gtt.gym_id = $1
    ORDER BY gtt.slot_number
    `,
    [gym.id]
  );

  if (teamResult.rows.length) return teamResult.rows;

  const level = Math.max(3, Number(gym.required_trainer_level || 2) * 3 + Number(gym.gym_order || 1) * 2);
  const fallback = await client.query(
    `
    SELECT
      ms.id AS species_id,
      ($2::int + ROW_NUMBER() OVER (ORDER BY ms.dex_number)::int - 1) AS level,
      ms.dex_number,
      ms.name AS pokemon_name,
      COALESCE(ms.animated_path, ms.sprite_path) AS selected_sprite_path,
      pt.slug AS primary_type,
      st.slug AS secondary_type
    FROM game.monster_species ms
    LEFT JOIN game.monster_types pt ON pt.id = ms.primary_type_id
    LEFT JOIN game.monster_types st ON st.id = ms.secondary_type_id
    WHERE ms.is_active = true
      AND ($1::smallint IS NULL OR ms.primary_type_id = $1 OR ms.secondary_type_id = $1)
    ORDER BY ms.dex_number
    LIMIT 3
    `,
    [gym.type_id, level]
  );

  if (fallback.rows.length) return fallback.rows;

  const anySpecies = await client.query(
    `
    SELECT
      ms.id AS species_id,
      $1::int AS level,
      ms.dex_number,
      ms.name AS pokemon_name,
      COALESCE(ms.animated_path, ms.sprite_path) AS selected_sprite_path,
      pt.slug AS primary_type,
      st.slug AS secondary_type
    FROM game.monster_species ms
    LEFT JOIN game.monster_types pt ON pt.id = ms.primary_type_id
    LEFT JOIN game.monster_types st ON st.id = ms.secondary_type_id
    WHERE ms.is_active = true
    ORDER BY ms.dex_number
    LIMIT 3
    `,
    [level]
  );
  return anySpecies.rows;
}

async function buildGymEnemyTeam(gym, client = pool) {
  const rows = await getGymEnemyRows(gym, client);
  const team = [];
  for (let index = 0; index < rows.length; index += 1) {
    const row = {
      ...rows[index],
      player_monster_id: null,
      current_hp: null,
      iv_hp: Number(gym.gym_order || 1) * 2,
      iv_attack: Number(gym.gym_order || 1) * 2,
      iv_defense: Number(gym.gym_order || 1) * 2,
      is_shiny: false,
    };
    const skills = await getSkillsForSpecies(row.species_id, row.level, client);
    team.push(asBattleMonster(row, "enemy", index, skills));
  }
  return team;
}

function firstAliveIndex(team) {
  return team.findIndex((monster) => Number(monster.current_hp || 0) > 0);
}

const BATTLE_ITEM_DEFAULTS = {
  "potion": { heal: 20, label: "Potion" },
  "super-potion": { heal: 60, label: "Super Potion" },
  "hyper-potion": { heal: 120, label: "Hyper Potion" },
  "revive": { heal: 0, label: "Revive" },
};

function battleMonsterHp(monster) {
  return {
    hp: Math.max(0, Number(monster?.current_hp || 0)),
    max: Math.max(1, Number(monster?.max_hp || 1)),
  };
}

async function getBattleItems(userId, client = pool) {
  const slugs = Object.keys(BATTLE_ITEM_DEFAULTS);
  const result = await client.query(
    `
    SELECT
      i.id AS item_id,
      i.slug,
      i.name,
      i.display_name,
      i.icon_path,
      i.heal_amount,
      COALESCE(pi.quantity, 0)::int AS quantity
    FROM game.items i
    LEFT JOIN game.player_inventory pi
      ON pi.item_id = i.id
     AND pi.user_id = $1
    WHERE i.slug = ANY($2::text[])
    ORDER BY array_position($2::text[], i.slug)
    `,
    [userId, slugs]
  );

  return result.rows.map((row) => ({
    item_id: row.item_id,
    slug: row.slug,
    item_slug: row.slug,
    name: row.name,
    display_name: row.display_name || row.name || BATTLE_ITEM_DEFAULTS[row.slug]?.label || row.slug,
    icon_path: row.icon_path,
    heal_amount: Number(row.heal_amount || BATTLE_ITEM_DEFAULTS[row.slug]?.heal || 0),
    quantity: Number(row.quantity || 0),
  }));
}

async function publicBattleState(session, state, userId = null, client = pool) {
  const activePlayer = state.playerTeam[state.activePlayerIndex] || null;
  const activeEnemy = state.enemyTeam[state.activeEnemyIndex] || null;
  const battleItems = userId ? await getBattleItems(userId, client) : [];

  return {
    ok: true,
    battle_id: session.id || session.battle_id,
    battle_type: session.battle_type || state.battleType,
    target_slug: session.target_slug || state.targetSlug,
    status: session.status,
    winner: session.winner || state.winner || null,
    rewards: session.rewards || state.rewards || null,
    turn: state.turn,
    player_team: state.playerTeam,
    enemy_team: state.enemyTeam,
    active_player_index: state.activePlayerIndex,
    active_enemy_index: state.activeEnemyIndex,
    active_player_monster: activePlayer ? { ...activePlayer, hp_percent: hpPercent(activePlayer) } : null,
    active_enemy_monster: activeEnemy ? { ...activeEnemy, hp_percent: hpPercent(activeEnemy) } : null,
    available_skills: activePlayer?.skills || [],
    battle_items: battleItems,
    log: state.log || [],
  };
}

async function startBattle(req, res) {
  const battleType = String(req.body?.battleType || req.body?.battle_type || "").trim().toLowerCase();
  const targetSlug = String(req.body?.targetSlug || req.body?.target_slug || "").trim().toLowerCase();

  if (!["gym", "arena"].includes(battleType)) {
    throw createHttpError(400, "INVALID_BATTLE_TYPE", "Invalid battle type.");
  }

  if (!targetSlug) {
    throw createHttpError(400, battleType === "gym" ? "GYM_NOT_FOUND" : "NPC_NOT_FOUND", "targetSlug is required.");
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const playerTeam = await getPlayerBattleTeam(req.user.id, client);
    if (!playerTeam.length || firstAliveIndex(playerTeam) < 0) {
      throw createHttpError(400, "TEAM_EMPTY", "You need at least one available monster in your team.");
    }

    const gym = await findGymForBattle(targetSlug, client);
    if (!gym) {
      throw createHttpError(404, battleType === "gym" ? "GYM_NOT_FOUND" : "NPC_NOT_FOUND", "Gym was not found.");
    }

    if (battleType === "gym") {
      const gymState = await getGymProgressState(req.user.id, gym.id, client);
      if (!gymState?.is_unlocked) {
        throw createHttpError(403, "GYM_LOCKED", "Complete the previous gym first.");
      }
    }

    const enemyTeam = await buildGymEnemyTeam(gym, client);
    const state = {
      battleType,
      targetSlug: gym.slug,
      gym: {
        id: gym.id,
        slug: gym.slug,
        name: gym.name,
        gym_order: gym.gym_order,
        badge_name: gym.badge_name,
        badge_icon_path: gym.badge_icon_path,
        type_slug: gym.type_slug,
      },
      playerTeam,
      enemyTeam,
      activePlayerIndex: firstAliveIndex(playerTeam),
      activeEnemyIndex: firstAliveIndex(enemyTeam),
      turn: 1,
      winner: null,
      rewards: null,
      log: [`Batalla iniciada contra ${gym.leader_name || gym.name}.`],
    };

    const inserted = await client.query(
      `
      INSERT INTO game.battle_sessions (user_id, battle_type, gym_id, npc_trainer_id, status, target_slug, battle_state)
      VALUES ($1, $2, $3, $4, 'active', $5, $6::jsonb)
      RETURNING *
      `,
      [req.user.id, battleType, gym.id, gym.leader_id || null, gym.slug, JSON.stringify(state)]
    );

    await client.query("COMMIT");
    res.status(201).json(await publicBattleState(inserted.rows[0], state, req.user.id, client));
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function loadBattleSessionForUser(client, userId, battleId) {
  const result = await client.query(
    `
    SELECT *
    FROM game.battle_sessions
    WHERE id = $1
    LIMIT 1
    FOR UPDATE
    `,
    [battleId]
  );

  if (!result.rows.length) {
    throw createHttpError(404, "BATTLE_NOT_FOUND", "Battle was not found.");
  }

  const session = result.rows[0];
  if (String(session.user_id) !== String(userId)) {
    throw createHttpError(403, "BATTLE_NOT_OWNED", "Battle does not belong to the current user.");
  }

  return session;
}

async function getBattle(req, res) {
  const rows = await query("SELECT * FROM game.battle_sessions WHERE id = $1 LIMIT 1", [req.params.battleId]);
  if (!rows.length) {
    throw createHttpError(404, "BATTLE_NOT_FOUND", "Battle was not found.");
  }
  if (String(rows[0].user_id) !== String(req.user.id)) {
    throw createHttpError(403, "BATTLE_NOT_OWNED", "Battle does not belong to the current user.");
  }
  res.json(await publicBattleState(rows[0], rows[0].battle_state || {}, req.user.id));
}

function findSkill(monster, { skillSlug, skillId }) {
  return (monster.skills || []).find((skill) => {
    if (skillId && String(skill.skill_id) === String(skillId)) return true;
    if (skillSlug && skill.slug === skillSlug) return true;
    return false;
  }) || null;
}

function applySkillTurn(actor, target, skill, actorSide) {
  const accuracy = Number(skill.accuracy || 100);
  const hit = accuracy >= 100 || Math.random() * 100 <= accuracy;
  if (!hit) {
    return {
      actorSide,
      actor,
      target,
      skill,
      damage: 0,
      isCritical: false,
      typeMultiplier: 1,
      logText: `${actor.pokemon_name} falló ${skill.name}.`,
      hit: false,
    };
  }

  const damageResult = calculateBattleDamage(actor, target, skill);
  const nextHp = Math.max(0, Number(target.current_hp || 0) - damageResult.damage);
  target.current_hp = nextHp;

  const extra = damageResult.typeMultiplier > 1 ? " Es supereficaz." : damageResult.typeMultiplier > 0 && damageResult.typeMultiplier < 1 ? " No fue muy eficaz." : damageResult.typeMultiplier === 0 ? " No tuvo efecto." : "";
  const crit = damageResult.isCritical ? " Golpe crítico." : "";

  return {
    actorSide,
    actor,
    target,
    skill,
    damage: damageResult.damage,
    isCritical: damageResult.isCritical,
    typeMultiplier: damageResult.typeMultiplier,
    logText: `${actor.pokemon_name} usó ${skill.name} e hizo ${damageResult.damage} de daño.${extra}${crit}`,
    hit: true,
  };
}

function chooseEnemySkill(enemy, target) {
  const skills = enemy.skills || [];
  if (!skills.length) return null;
  return [...skills].sort((a, b) => {
    const aScore = Number(a.power || 0) * typeEffectiveness(a.type_slug, target);
    const bScore = Number(b.power || 0) * typeEffectiveness(b.type_slug, target);
    return bScore - aScore;
  })[0];
}

function finishBattleState(state, winner) {
  state.winner = winner;
  state.rewards = null;
  state.log.push(winner === "player" ? "Victoria. Recompensas listas." : "Derrota. Tu equipo cayó en combate.");
}

async function persistBattleTurn(client, battleId, turnNumber, result) {
  const actionType = result.actionType || "skill";
  await client.query(
    `
    INSERT INTO game.battle_turns (
      battle_id,
      turn_number,
      actor_monster_id,
      target_monster_id,
      action_type,
      damage,
      log_text,
      actor_side,
      actor_monster_name,
      target_monster_name,
      skill_id,
      skill_slug,
      skill_name,
      is_critical,
      type_multiplier,
      item_slug,
      item_name,
      result
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18::jsonb)
    ON CONFLICT (battle_id, turn_number)
    DO UPDATE SET
      action_type = EXCLUDED.action_type,
      damage = EXCLUDED.damage,
      log_text = EXCLUDED.log_text,
      actor_side = EXCLUDED.actor_side,
      actor_monster_name = EXCLUDED.actor_monster_name,
      target_monster_name = EXCLUDED.target_monster_name,
      skill_id = EXCLUDED.skill_id,
      skill_slug = EXCLUDED.skill_slug,
      skill_name = EXCLUDED.skill_name,
      is_critical = EXCLUDED.is_critical,
      type_multiplier = EXCLUDED.type_multiplier,
      item_slug = EXCLUDED.item_slug,
      item_name = EXCLUDED.item_name,
      result = EXCLUDED.result
    `,
    [
      battleId,
      turnNumber,
      result.actorSide === "player" ? result.actor.player_monster_id : null,
      result.actorSide === "enemy" ? result.target.player_monster_id : null,
      actionType,
      Number(result.damage || 0),
      result.logText,
      result.actorSide,
      result.actor?.pokemon_name || null,
      result.target?.pokemon_name || null,
      result.skill?.skill_id || null,
      result.skill?.slug || null,
      result.skill?.name || null,
      !!result.isCritical,
      Number(result.typeMultiplier || 1),
      result.item?.slug || null,
      result.item?.display_name || result.item?.name || null,
      JSON.stringify({
        ...(result.result || {}),
        action: actionType,
        hit: result.hit,
        damage: Number(result.damage || 0),
        isCritical: !!result.isCritical,
        typeMultiplier: Number(result.typeMultiplier || 1),
      }),
    ]
  );
}

async function grantRewardItems(client, userId, items = []) {
  if (!items.length) return [];

  const granted = [];
  for (const rewardItem of items) {
    const quantity = Math.max(0, Math.floor(Number(rewardItem.quantity || 0)));
    if (!rewardItem.slug || quantity <= 0) continue;

    const itemResult = await client.query(
      `
      SELECT id, slug, display_name, name, icon_path
      FROM game.items
      WHERE slug = $1
      LIMIT 1
      `,
      [rewardItem.slug]
    );

    if (!itemResult.rows.length) {
      console.warn(`Gym reward item not found: ${rewardItem.slug}`);
      continue;
    }

    const item = itemResult.rows[0];
    await client.query(
      `
      INSERT INTO game.player_inventory (user_id, item_id, quantity, updated_at)
      VALUES ($1, $2, $3, now())
      ON CONFLICT (user_id, item_id)
      DO UPDATE SET
        quantity = game.player_inventory.quantity + EXCLUDED.quantity,
        updated_at = now()
      `,
      [userId, item.id, quantity]
    );

    granted.push({
      slug: item.slug,
      quantity,
      display_name: item.display_name || item.name || item.slug,
      icon_path: item.icon_path || null,
    });
  }

  return granted;
}

async function grantGymBadge(client, userId, gym) {
  const slug = gymBadgeSlug(gym);
  const achievementResult = await client.query(
    `
    INSERT INTO game.achievements (slug, name, description, target_type, target_value, reward_title)
    VALUES ($1, $2, $3, 'gym_badge', 1, $2)
    ON CONFLICT (slug) DO UPDATE
    SET
      name = EXCLUDED.name,
      description = EXCLUDED.description,
      target_type = EXCLUDED.target_type,
      target_value = EXCLUDED.target_value,
      reward_title = EXCLUDED.reward_title
    RETURNING *
    `,
    [
      slug,
      gym.badge_name || `${gym.name} Badge`,
      `Medalla obtenida al derrotar ${gym.name}.`,
    ]
  );

  const achievement = achievementResult.rows[0];
  const grantResult = await client.query(
    `
    INSERT INTO game.player_achievements (user_id, achievement_id, progress, unlocked, claimed, unlocked_at)
    VALUES ($1, $2, 1, true, true, now())
    ON CONFLICT (user_id, achievement_id)
    DO UPDATE SET
      progress = GREATEST(game.player_achievements.progress, 1),
      unlocked = true,
      claimed = true,
      unlocked_at = COALESCE(game.player_achievements.unlocked_at, now())
    RETURNING unlocked_at
    `,
    [userId, achievement.id]
  );

  return {
    slug: achievement.slug,
    name: achievement.name,
    description: achievement.description,
    icon_path: gym.badge_icon_path || null,
    earned_at: grantResult.rows[0]?.unlocked_at || new Date().toISOString(),
    gym_slug: gym.slug,
  };
}

async function updateGymProgressOnWin(client, userId, session, state) {
  const gymResult = await client.query(
    `
    SELECT
      g.*,
      r.slug AS region_slug,
      r.name AS region_name,
      r.generation_id,
      mt.slug AS type_slug,
      mt.name AS type_name
    FROM game.gyms g
    LEFT JOIN game.regions r ON r.id = g.region_id
    LEFT JOIN game.monster_types mt ON mt.id = g.type_id
    WHERE g.id = $1
       OR g.slug = $2
    LIMIT 1
    `,
    [session.gym_id || null, session.target_slug || state.targetSlug || null]
  );

  if (!gymResult.rows.length) {
    throw createHttpError(404, "GYM_NOT_FOUND", "Gym was not found.");
  }

  const gym = gymResult.rows[0];
  await client.query(
    `
    INSERT INTO game.player_gym_progress (user_id, gym_id, status, wins, created_at, updated_at)
    VALUES ($1, $2, 'available', 0, now(), now())
    ON CONFLICT (user_id, gym_id) DO NOTHING
    `,
    [userId, gym.id]
  );

  const progressResult = await client.query(
    `
    SELECT *
    FROM game.player_gym_progress
    WHERE user_id = $1
      AND gym_id = $2
    FOR UPDATE
    `,
    [userId, gym.id]
  );

  const progress = progressResult.rows[0];
  const firstClear = progress.status !== "completed";
  const turnsTaken = Math.max(1, Number(state.turn || 1) - 1);
  const rewards = await decorateRewardItems(gymRewardFor(gym, firstClear), client);
  const grantedItems = await grantRewardItems(client, userId, rewards.items);
  rewards.items = grantedItems;

  await client.query(
    `
    UPDATE game.player_gym_progress
    SET
      status = 'completed',
      wins = wins + 1,
      best_turns = CASE
        WHEN best_turns IS NULL THEN $3
        ELSE LEAST(best_turns, $3)
      END,
      last_battle_session_id = $4,
      completed_at = COALESCE(completed_at, now()),
      updated_at = now()
    WHERE user_id = $1
      AND gym_id = $2
    `,
    [userId, gym.id, turnsTaken, session.id]
  );

  let badge = null;
  if (firstClear) {
    try {
      badge = await grantGymBadge(client, userId, gym);
    } catch (error) {
      throw createHttpError(500, "BADGE_GRANT_FAILED", "Could not grant gym badge.");
    }
    await incrementQuestProgress(userId, "badge_earned", { gymSlug: gym.slug }, client);
  }

  return {
    gym,
    firstClear,
    rewards: {
      ...rewards,
      badge,
      gym_slug: gym.slug,
      gym_name: gym.name,
    },
  };
}

async function rewardBattleWin(client, userId, session, state) {
  try {
    let rewards = state.rewards || null;
    if (session.battle_type === "gym") {
      const gymReward = await updateGymProgressOnWin(client, userId, session, state);
      rewards = gymReward.rewards;
    } else {
      rewards = {
        gold: 1000,
        diamonds: 0,
        items: [],
        first_clear: true,
      };
    }

    const gold = Number(rewards.gold || 0);
    const diamonds = Number(rewards.diamonds || 0);

    if (gold > 0 || diamonds > 0) {
      await client.query(
        `
        UPDATE game.trainer_wallets
        SET gold = gold + $2,
            diamonds = diamonds + $3,
            updated_at = now()
        WHERE user_id = $1
        `,
        [userId, gold, diamonds]
      );

      if (gold > 0) {
        await client.query(
          `
          INSERT INTO game.wallet_transactions (user_id, currency, amount, reason, reference_type, reference_id)
          VALUES ($1, 'gold', $2, $3, 'battle', $4)
          `,
          [userId, gold, session.battle_type === "gym" ? "gym_reward" : "battle_reward", session.id]
        );
      }

      if (diamonds > 0) {
        await client.query(
          `
          INSERT INTO game.wallet_transactions (user_id, currency, amount, reason, reference_type, reference_id)
          VALUES ($1, 'diamonds', $2, $3, 'battle', $4)
          `,
          [userId, diamonds, session.battle_type === "gym" ? "gym_reward" : "battle_reward", session.id]
        );
      }
    }

    state.rewards = rewards;
    await incrementQuestProgress(userId, "battle_win", { battleType: session.battle_type }, client);
    if (session.battle_type === "gym") {
      await incrementQuestProgress(userId, "gym_win", { targetSlug: session.target_slug }, client);
    }
  } catch (error) {
    if (error.status) throw error;
    throw createHttpError(500, session.battle_type === "gym" ? "GYM_PROGRESS_FAILED" : "BATTLE_REWARD_FAILED", "Could not deliver battle rewards.");
  }
}

function findBattleMonsterIndex(state, playerMonsterId) {
  return (state.playerTeam || []).findIndex((monster) => String(monster.player_monster_id) === String(playerMonsterId));
}

function applyFaintingAndVictory(state, turnLog) {
  const enemy = state.enemyTeam?.[state.activeEnemyIndex];
  if (enemy && Number(enemy.current_hp || 0) <= 0) {
    turnLog.push(`${enemy.pokemon_name} cayÃ³.`);
    state.activeEnemyIndex = firstAliveIndex(state.enemyTeam || []);
    if (state.activeEnemyIndex < 0) {
      finishBattleState(state, "player");
    } else {
      turnLog.push(`${state.enemyTeam[state.activeEnemyIndex].pokemon_name} entra al combate.`);
    }
  }
}

function applyPlayerFaintingAndDefeat(state, turnLog) {
  const player = state.playerTeam?.[state.activePlayerIndex];
  if (player && Number(player.current_hp || 0) <= 0) {
    turnLog.push(`${player.pokemon_name} cayÃ³.`);
    state.activePlayerIndex = firstAliveIndex(state.playerTeam || []);
    if (state.activePlayerIndex < 0) {
      finishBattleState(state, "enemy");
    } else {
      turnLog.push(`${state.playerTeam[state.activePlayerIndex].pokemon_name} entra al combate.`);
    }
  }
}

async function applyEnemyResponse(client, session, state, turnLog, turnNumber) {
  if (state.winner) return;

  const activeEnemy = state.enemyTeam?.[state.activeEnemyIndex];
  const activePlayer = state.playerTeam?.[state.activePlayerIndex];
  if (!activeEnemy || Number(activeEnemy.current_hp || 0) <= 0 || !activePlayer || Number(activePlayer.current_hp || 0) <= 0) return;

  const enemySkill = chooseEnemySkill(activeEnemy, activePlayer);
  if (!enemySkill) {
    throw createHttpError(500, "BATTLE_TURN_FAILED", "Enemy has no available skills.");
  }

  const enemyTurn = applySkillTurn(activeEnemy, activePlayer, enemySkill, "enemy");
  turnLog.push(enemyTurn.logText);
  await persistBattleTurn(client, session.id, turnNumber, enemyTurn);
  applyPlayerFaintingAndDefeat(state, turnLog);
}

function switchBattleMonster(state, targetPlayerMonsterId) {
  const current = state.playerTeam?.[state.activePlayerIndex];
  const targetIndex = findBattleMonsterIndex(state, targetPlayerMonsterId);
  if (targetIndex < 0) {
    throw createHttpError(400, "MONSTER_NOT_IN_BATTLE", "Monster is not in this battle.");
  }
  const target = state.playerTeam[targetIndex];
  if (Number(target.current_hp || 0) <= 0) {
    throw createHttpError(400, "MONSTER_FAINTED", "Monster is fainted.");
  }
  if (targetIndex === state.activePlayerIndex) {
    throw createHttpError(400, "MONSTER_ALREADY_ACTIVE", "Monster is already active.");
  }

  state.activePlayerIndex = targetIndex;
  return {
    actionType: "switch",
    actorSide: "player",
    actor: current,
    target,
    damage: 0,
    isCritical: false,
    typeMultiplier: 1,
    hit: true,
    logText: `Cambiaste a ${target.pokemon_name}.`,
    result: {
      from: {
        player_monster_id: current?.player_monster_id || null,
        pokemon_name: current?.pokemon_name || null,
      },
      to: {
        player_monster_id: target.player_monster_id,
        pokemon_name: target.pokemon_name,
      },
    },
  };
}

async function useBattleItem(client, userId, state, itemSlug, targetPlayerMonsterId) {
  const slug = String(itemSlug || "").trim().toLowerCase();
  if (!BATTLE_ITEM_DEFAULTS[slug]) {
    throw createHttpError(400, "ITEM_NOT_ALLOWED_IN_BATTLE", "Item is not allowed in battle.");
  }

  const targetIndex = findBattleMonsterIndex(state, targetPlayerMonsterId);
  if (targetIndex < 0) {
    throw createHttpError(400, "MONSTER_NOT_IN_BATTLE", "Monster is not in this battle.");
  }
  const target = state.playerTeam[targetIndex];

  const itemResult = await client.query(
    `
    SELECT
      i.id,
      i.slug,
      i.name,
      i.display_name,
      i.heal_amount
    FROM game.items i
    WHERE i.slug = $1
    LIMIT 1
    `,
    [slug]
  );

  if (!itemResult.rows.length) {
    throw createHttpError(404, "ITEM_NOT_FOUND", "Item was not found.");
  }

  const item = itemResult.rows[0];
  const inventoryResult = await client.query(
    `
    SELECT quantity
    FROM game.player_inventory
    WHERE user_id = $1
      AND item_id = $2
    LIMIT 1
    FOR UPDATE
    `,
    [userId, item.id]
  );

  if (!inventoryResult.rows.length || Number(inventoryResult.rows[0].quantity || 0) < 1) {
    throw createHttpError(400, "INSUFFICIENT_ITEM", "Not enough item quantity.");
  }

  const hp = battleMonsterHp(target);
  let healed = 0;
  let nextHp = hp.hp;

  if (slug === "revive") {
    if (hp.hp > 0) {
      throw createHttpError(400, "MONSTER_NOT_FAINTED", "Monster is not fainted.");
    }
    nextHp = Math.max(1, Math.floor(hp.max / 2));
    healed = nextHp;
  } else {
    if (hp.hp <= 0) {
      throw createHttpError(400, "MONSTER_FAINTED", "Monster is fainted.");
    }
    if (hp.hp >= hp.max) {
      throw createHttpError(400, "MONSTER_ALREADY_FULL_HP", "Monster is already full HP.");
    }
    const healAmount = Number(item.heal_amount || BATTLE_ITEM_DEFAULTS[slug].heal || 0);
    nextHp = Math.min(hp.max, hp.hp + healAmount);
    healed = nextHp - hp.hp;
  }

  target.current_hp = nextHp;
  await client.query(
    `
    UPDATE game.player_inventory
    SET quantity = quantity - 1,
        updated_at = now()
    WHERE user_id = $1
      AND item_id = $2
      AND quantity > 0
    `,
    [userId, item.id]
  );

  await incrementQuestProgress(userId, "use_item", { itemSlug: slug, quantity: 1 }, client);

  const displayName = item.display_name || item.name || BATTLE_ITEM_DEFAULTS[slug].label || slug;
  return {
    actionType: "use_item",
    actorSide: "player",
    actor: state.playerTeam[state.activePlayerIndex],
    target,
    item: {
      slug,
      name: item.name,
      display_name: displayName,
    },
    damage: 0,
    isCritical: false,
    typeMultiplier: 1,
    hit: true,
    logText: `Usaste ${displayName} en ${target.pokemon_name}. RecuperÃ³ ${healed} HP.`,
    result: {
      itemSlug: slug,
      targetPlayerMonsterId: target.player_monster_id,
      healed,
      currentHp: nextHp,
      maxHp: hp.max,
    },
  };
}

async function submitBattleTurnLegacy(req, res) {
  const battleId = String(req.params.battleId || "").trim();
  const action = String(req.body?.action || "").trim().toLowerCase();
  const skillSlug = req.body?.skillSlug || req.body?.skill_slug || null;
  const skillId = req.body?.skillId || req.body?.skill_id || null;

  if (action !== "skill") {
    throw createHttpError(400, "INVALID_ACTION", "Only skill actions are supported.");
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const session = await loadBattleSessionForUser(client, req.user.id, battleId);

    if (session.status !== "active") {
      throw createHttpError(400, "BATTLE_ALREADY_FINISHED", "Battle is already finished.");
    }

    const state = session.battle_state || {};
    const player = state.playerTeam?.[state.activePlayerIndex];
    const enemy = state.enemyTeam?.[state.activeEnemyIndex];

    if (!player || Number(player.current_hp || 0) <= 0) {
      throw createHttpError(400, "ACTIVE_MONSTER_FAINTED", "Active monster is fainted.");
    }

    if (!enemy || Number(enemy.current_hp || 0) <= 0) {
      throw createHttpError(400, "BATTLE_TURN_FAILED", "Active enemy is not available.");
    }

    const skill = findSkill(player, { skillSlug, skillId });
    if (!skill) {
      throw createHttpError(404, skillSlug || skillId ? "SKILL_NOT_AVAILABLE" : "SKILL_NOT_FOUND", "Skill is not available.");
    }

    const turnLog = [];
    const playerTurn = applySkillTurn(player, enemy, skill, "player");
    turnLog.push(playerTurn.logText);
    await persistBattleTurn(client, session.id, Number(state.turn || 1) * 2 - 1, playerTurn);

    if (Number(enemy.current_hp || 0) <= 0) {
      turnLog.push(`${enemy.pokemon_name} cayó.`);
      state.activeEnemyIndex = firstAliveIndex(state.enemyTeam);
      if (state.activeEnemyIndex < 0) {
        finishBattleState(state, "player");
      } else {
        turnLog.push(`${state.enemyTeam[state.activeEnemyIndex].pokemon_name} entra al combate.`);
      }
    }

    if (!state.winner) {
      const activeEnemy = state.enemyTeam[state.activeEnemyIndex];
      const activePlayer = state.playerTeam[state.activePlayerIndex];
      const enemySkill = chooseEnemySkill(activeEnemy, activePlayer);
      if (!enemySkill) {
        throw createHttpError(500, "BATTLE_TURN_FAILED", "Enemy has no available skills.");
      }

      const enemyTurn = applySkillTurn(activeEnemy, activePlayer, enemySkill, "enemy");
      turnLog.push(enemyTurn.logText);
      await persistBattleTurn(client, session.id, Number(state.turn || 1) * 2, enemyTurn);

      if (Number(activePlayer.current_hp || 0) <= 0) {
        turnLog.push(`${activePlayer.pokemon_name} cayó.`);
        state.activePlayerIndex = firstAliveIndex(state.playerTeam);
        if (state.activePlayerIndex < 0) {
          finishBattleState(state, "enemy");
        } else {
          turnLog.push(`${state.playerTeam[state.activePlayerIndex].pokemon_name} entra al combate.`);
        }
      }
    }

    state.log = [...(state.log || []), ...turnLog].slice(-40);
    state.turn = Number(state.turn || 1) + 1;

    const finalStatus = state.winner === "player" ? "victory" : state.winner === "enemy" ? "defeat" : "active";
    if (state.winner === "player") {
      await rewardBattleWin(client, req.user.id, session, state);
    }

    const updated = await client.query(
      `
      UPDATE game.battle_sessions
      SET battle_state = $2::jsonb,
          status = $3,
          winner = $4,
          rewards = $5::jsonb,
          finished_at = CASE WHEN $3 <> 'active' THEN COALESCE(finished_at, now()) ELSE finished_at END,
          completed_at = CASE WHEN $3 <> 'active' THEN COALESCE(completed_at, now()) ELSE completed_at END
      WHERE id = $1
      RETURNING *
      `,
      [
        session.id,
        JSON.stringify(state),
        finalStatus,
        state.winner,
        state.rewards ? JSON.stringify(state.rewards) : null,
      ]
    );

    await client.query("COMMIT");
    res.json(publicBattleState(updated.rows[0], state));
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    if (!error.code || error.code === "23514" || error.code === "23503") {
      error.status = error.status || 500;
      error.code = "BATTLE_TURN_FAILED";
    }
    throw error;
  } finally {
    client.release();
  }
}

async function submitBattleTurn(req, res) {
  const battleId = String(req.params.battleId || "").trim();
  const action = String(req.body?.action || "").trim().toLowerCase();
  const skillSlug = req.body?.skillSlug || req.body?.skill_slug || null;
  const skillId = req.body?.skillId || req.body?.skill_id || null;
  const targetPlayerMonsterId = req.body?.targetPlayerMonsterId || req.body?.target_player_monster_id || null;
  const itemSlug = req.body?.itemSlug || req.body?.item_slug || null;

  if (!["skill", "switch", "use_item"].includes(action)) {
    throw createHttpError(400, "INVALID_ACTION", "Unsupported battle action.");
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const session = await loadBattleSessionForUser(client, req.user.id, battleId);

    if (session.status !== "active") {
      throw createHttpError(400, "BATTLE_ALREADY_FINISHED", "Battle is already finished.");
    }

    const state = session.battle_state || {};
    const player = state.playerTeam?.[state.activePlayerIndex];
    const enemy = state.enemyTeam?.[state.activeEnemyIndex];

    if (action === "skill" && (!player || Number(player.current_hp || 0) <= 0)) {
      throw createHttpError(400, "ACTIVE_MONSTER_FAINTED", "Active monster is fainted.");
    }

    if (!enemy || Number(enemy.current_hp || 0) <= 0) {
      throw createHttpError(400, "BATTLE_TURN_FAILED", "Active enemy is not available.");
    }

    const turnLog = [];
    const playerTurnNumber = Number(state.turn || 1) * 2 - 1;
    const enemyTurnNumber = Number(state.turn || 1) * 2;

    if (action === "skill") {
      const skill = findSkill(player, { skillSlug, skillId });
      if (!skill) {
        throw createHttpError(404, skillSlug || skillId ? "SKILL_NOT_AVAILABLE" : "SKILL_NOT_FOUND", "Skill is not available.");
      }

      const playerTurn = applySkillTurn(player, enemy, skill, "player");
      turnLog.push(playerTurn.logText);
      await persistBattleTurn(client, session.id, playerTurnNumber, playerTurn);
      applyFaintingAndVictory(state, turnLog);
      await applyEnemyResponse(client, session, state, turnLog, enemyTurnNumber);
    } else if (action === "switch") {
      if (!targetPlayerMonsterId) {
        throw createHttpError(400, "MONSTER_NOT_IN_BATTLE", "targetPlayerMonsterId is required.");
      }
      const switchTurn = switchBattleMonster(state, targetPlayerMonsterId);
      turnLog.push(switchTurn.logText);
      await persistBattleTurn(client, session.id, playerTurnNumber, switchTurn);
      await applyEnemyResponse(client, session, state, turnLog, enemyTurnNumber);
    } else if (action === "use_item") {
      if (!targetPlayerMonsterId) {
        throw createHttpError(400, "MONSTER_NOT_IN_BATTLE", "targetPlayerMonsterId is required.");
      }
      const itemTurn = await useBattleItem(client, req.user.id, state, itemSlug, targetPlayerMonsterId);
      turnLog.push(itemTurn.logText);
      await persistBattleTurn(client, session.id, playerTurnNumber, itemTurn);
      await applyEnemyResponse(client, session, state, turnLog, enemyTurnNumber);
    }

    state.log = [...(state.log || []), ...turnLog].slice(-40);
    state.turn = Number(state.turn || 1) + 1;

    const finalStatus = state.winner === "player" ? "victory" : state.winner === "enemy" ? "defeat" : "active";
    if (state.winner === "player") {
      await rewardBattleWin(client, req.user.id, session, state);
    }

    const updated = await client.query(
      `
      UPDATE game.battle_sessions
      SET battle_state = $2::jsonb,
          status = $3,
          winner = $4,
          rewards = $5::jsonb,
          finished_at = CASE WHEN $3 <> 'active' THEN COALESCE(finished_at, now()) ELSE finished_at END,
          completed_at = CASE WHEN $3 <> 'active' THEN COALESCE(completed_at, now()) ELSE completed_at END
      WHERE id = $1
      RETURNING *
      `,
      [
        session.id,
        JSON.stringify(state),
        finalStatus,
        state.winner,
        state.rewards ? JSON.stringify(state.rewards) : null,
      ]
    );

    await client.query("COMMIT");
    res.json(await publicBattleState(updated.rows[0], state, req.user.id, client));
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    if (!error.code || error.code === "23514" || error.code === "23503") {
      error.status = error.status || 500;
      error.code = action === "switch" ? "SWITCH_FAILED" : action === "use_item" ? "ITEM_USE_FAILED" : "BATTLE_TURN_FAILED";
    }
    throw error;
  } finally {
    client.release();
  }
}

app.get("/api/me/monsters/:playerMonsterId/skills", authRequired, asyncRoute(getMonsterBattleSkills));
app.get("/api/gyms", authRequired, asyncRoute(getGyms));
app.get("/api/me/gym-progress", authRequired, asyncRoute(getGymProgress));
app.get("/api/me/badges", authRequired, asyncRoute(getPlayerBadges));
app.post("/api/battles/start", authRequired, asyncRoute(startBattle));
app.get("/api/battles/:battleId", authRequired, asyncRoute(getBattle));
app.post("/api/battles/:battleId/turn", authRequired, asyncRoute(submitBattleTurn));

// =======================================================
// Shop
// =======================================================

function normalizeShopQuantity(value) {
  const quantity = Number(value);

  if (!Number.isInteger(quantity) || quantity < 1 || quantity > 99) {
    throw createHttpError(400, "INVALID_QUANTITY", "quantity must be an integer between 1 and 99.");
  }

  return quantity;
}

const USABLE_ITEM_SLUGS = new Set([
  "potion",
  "super-potion",
  "hyper-potion",
  "revive",
  "rare-candy",
]);

function calculateMonsterMaxHp(monster) {
  return 20 + Number(monster.level || 1) * 3 + Number(monster.iv_hp || 0);
}

function defaultHealAmount(item) {
  const fallback = {
    "potion": 20,
    "super-potion": 60,
    "hyper-potion": 120,
    "revive": 0,
  };

  return Number(item.heal_amount || fallback[item.slug] || 0);
}

function isShopItemPurchasable(item) {
  return Number(item?.cost_gold || 0) > 0 || Number(item?.cost_diamonds || 0) > 0;
}

async function getShopItems(req, res) {
  const rows = await query(
    `
    SELECT
      i.id AS item_id,
      i.slug,
      i.name,
      COALESCE(i.display_name, i.name) AS display_name,
      c.slug AS category_slug,
      c.name AS category_name,
      i.icon_path,
      i.cost_gold,
      COALESCE(i.cost_diamonds, 0) AS cost_diamonds,
      i.is_premium,
      i.is_custom,
      i.is_tradeable,
      i.capture_bonus,
      i.heal_amount,
      (COALESCE(i.cost_gold, 0) > 0 OR COALESCE(i.cost_diamonds, 0) > 0) AS is_purchasable
    FROM game.items i
    LEFT JOIN game.item_categories c ON c.id = i.category_id
    ORDER BY
      (COALESCE(i.cost_gold, 0) > 0 OR COALESCE(i.cost_diamonds, 0) > 0) DESC,
      i.is_premium,
      c.slug,
      i.cost_gold,
      i.cost_diamonds,
      i.slug
    `
  );

  res.json(rows);
}

async function buyShopItem(req, res) {
  const itemSlug = String(req.body?.itemSlug || req.body?.item_slug || "").trim();
  const quantity = normalizeShopQuantity(req.body?.quantity ?? 1);

  if (!itemSlug) {
    throw createHttpError(400, "ITEM_NOT_FOUND", "itemSlug is required.");
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const itemResult = await client.query(
      `
      SELECT
        i.id AS item_id,
        i.slug,
        i.name,
        COALESCE(i.display_name, i.name) AS display_name,
        c.slug AS category_slug,
        c.name AS category_name,
        i.icon_path,
        i.cost_gold,
        COALESCE(i.cost_diamonds, 0) AS cost_diamonds,
        i.is_premium,
        i.is_custom,
        i.is_tradeable,
        i.capture_bonus,
        i.heal_amount
      FROM game.items i
      LEFT JOIN game.item_categories c ON c.id = i.category_id
      WHERE i.slug = $1
      LIMIT 1
      `,
      [itemSlug]
    );

    if (!itemResult.rows.length) {
      throw createHttpError(404, "ITEM_NOT_FOUND", "Item was not found.");
    }

    const item = itemResult.rows[0];
    if (!isShopItemPurchasable(item)) {
      throw createHttpError(400, "ITEM_NOT_PURCHASABLE", "Item is not purchasable.");
    }

    const walletResult = await client.query(
      `
      SELECT user_id, gold, diamonds, boss_tickets, season_exp
      FROM game.trainer_wallets
      WHERE user_id = $1
      FOR UPDATE
      `,
      [req.user.id]
    );

    if (!walletResult.rows.length) {
      throw createHttpError(404, "SHOP_BUY_FAILED", "Wallet was not found.");
    }

    const wallet = walletResult.rows[0];
    const totalGold = Number(item.cost_gold || 0) * quantity;
    const totalDiamonds = Number(item.cost_diamonds || 0) * quantity;

    if (Number(wallet.gold) < totalGold) {
      throw createHttpError(402, "INSUFFICIENT_GOLD", "Not enough gold.");
    }

    if (Number(wallet.diamonds) < totalDiamonds) {
      throw createHttpError(402, "INSUFFICIENT_DIAMONDS", "Not enough diamonds.");
    }

    const updatedWallet = await client.query(
      `
      UPDATE game.trainer_wallets
      SET
        gold = gold - $2,
        diamonds = diamonds - $3,
        updated_at = now()
      WHERE user_id = $1
      RETURNING user_id, gold, diamonds, boss_tickets, season_exp, updated_at
      `,
      [req.user.id, totalGold, totalDiamonds]
    );

    const inventoryResult = await client.query(
      `
      INSERT INTO game.player_inventory (user_id, item_id, quantity)
      VALUES ($1, $2, $3)
      ON CONFLICT (user_id, item_id)
      DO UPDATE SET
        quantity = game.player_inventory.quantity + EXCLUDED.quantity,
        updated_at = now()
      RETURNING user_id, item_id, quantity, updated_at
      `,
      [req.user.id, item.item_id, quantity]
    );

    if (totalGold > 0) {
      await client.query(
        `
        INSERT INTO game.wallet_transactions (user_id, currency, amount, reason, reference_type, reference_id)
        VALUES ($1, 'gold', $2, 'shop_purchase', 'item', $3)
        `,
        [req.user.id, -totalGold, item.item_id]
      );
    }

    if (totalDiamonds > 0) {
      await client.query(
        `
        INSERT INTO game.wallet_transactions (user_id, currency, amount, reason, reference_type, reference_id)
        VALUES ($1, 'diamonds', $2, 'shop_purchase', 'item', $3)
        `,
        [req.user.id, -totalDiamonds, item.item_id]
      );
    }

    await incrementQuestProgress(req.user.id, "buy_item", { itemSlug: item.slug, quantity }, client);
    await client.query("COMMIT");

    res.json({
      ok: true,
      item: {
        ...item,
        is_purchasable: true,
      },
      quantity,
      wallet: updatedWallet.rows[0],
      inventoryItem: inventoryResult.rows[0],
    });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    if (!error.code || error.code === "23514" || error.code === "23503") {
      error.status = error.status || 500;
      error.code = "SHOP_BUY_FAILED";
    }
    throw error;
  } finally {
    client.release();
  }
}

app.get("/api/shop/items", authRequired, asyncRoute(getShopItems));
app.post("/api/shop/buy", authRequired, asyncRoute(buyShopItem));

// =======================================================
// Item usage
// =======================================================

async function getMonsterSnapshot(userId, playerMonsterId, client = pool) {
  const result = await client.query(
    `
    SELECT
      c.*,
      (20 + c.level * 3 + COALESCE(c.iv_hp, 0)) AS max_hp
    FROM game.v_player_collection c
    WHERE c.user_id = $1
      AND c.player_monster_id = $2
    LIMIT 1
    `,
    [userId, playerMonsterId]
  );

  return result.rows[0] || null;
}

async function getCurrentInventoryRows(userId, client = pool) {
  const result = await client.query(
    "SELECT * FROM game.v_player_inventory WHERE user_id = $1 ORDER BY category_slug, item_slug",
    [userId]
  );
  return result.rows;
}

async function getMonsterDetail(req, res) {
  const playerMonsterId = req.params.playerMonsterId;

  if (!playerMonsterId) {
    throw createHttpError(400, "MONSTER_NOT_FOUND", "playerMonsterId is required.");
  }

  const monster = await getMonsterSnapshot(req.user.id, playerMonsterId);
  if (!monster) {
    throw createHttpError(404, "MONSTER_NOT_FOUND", "Monster was not found.");
  }

  res.json(monster);
}

async function useInventoryItem(req, res) {
  const itemSlug = String(req.body?.itemSlug || req.body?.item_slug || "").trim();
  const playerMonsterId = String(req.body?.playerMonsterId || req.body?.player_monster_id || "").trim();
  const quantity = normalizeShopQuantity(req.body?.quantity ?? 1);

  if (!itemSlug) {
    throw createHttpError(400, "ITEM_NOT_FOUND", "itemSlug is required.");
  }

  if (!playerMonsterId) {
    throw createHttpError(400, "MONSTER_NOT_FOUND", "playerMonsterId is required.");
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const itemResult = await client.query(
      `
      SELECT
        i.id AS item_id,
        i.slug,
        i.name,
        COALESCE(i.display_name, i.name) AS display_name,
        c.slug AS category_slug,
        c.name AS category_name,
        i.icon_path,
        i.heal_amount,
        i.capture_bonus
      FROM game.items i
      LEFT JOIN game.item_categories c ON c.id = i.category_id
      WHERE i.slug = $1
      LIMIT 1
      `,
      [itemSlug]
    );

    if (!itemResult.rows.length) {
      throw createHttpError(404, "ITEM_NOT_FOUND", "Item was not found.");
    }

    const item = itemResult.rows[0];
    if (!USABLE_ITEM_SLUGS.has(item.slug)) {
      throw createHttpError(400, "ITEM_NOT_USABLE", "Item is not usable yet.");
    }

    const inventoryResult = await client.query(
      `
      SELECT user_id, item_id, quantity
      FROM game.player_inventory
      WHERE user_id = $1
        AND item_id = $2
      FOR UPDATE
      `,
      [req.user.id, item.item_id]
    );

    if (!inventoryResult.rows.length || Number(inventoryResult.rows[0].quantity) < quantity) {
      throw createHttpError(400, "INSUFFICIENT_ITEM", "Not enough items.");
    }

    const monsterResult = await client.query(
      `
      SELECT *
      FROM game.player_monsters
      WHERE id = $1
      LIMIT 1
      FOR UPDATE
      `,
      [playerMonsterId]
    );

    if (!monsterResult.rows.length) {
      throw createHttpError(404, "MONSTER_NOT_FOUND", "Monster was not found.");
    }

    const monster = monsterResult.rows[0];
    if (String(monster.user_id) !== String(req.user.id)) {
      throw createHttpError(403, "MONSTER_NOT_OWNED", "Monster does not belong to the current user.");
    }

    const maxHp = calculateMonsterMaxHp(monster);
    const currentHp = monster.current_hp === null || monster.current_hp === undefined
      ? maxHp
      : Number(monster.current_hp);
    let nextLevel = Number(monster.level || 1);
    let nextHp = currentHp;
    let resultCode = "ITEM_USED";
    let message = "Item used.";

    if (["potion", "super-potion", "hyper-potion"].includes(item.slug)) {
      if (currentHp >= maxHp) {
        throw createHttpError(400, "MONSTER_ALREADY_FULL_HP", "Monster is already at full HP.");
      }

      const healAmount = defaultHealAmount(item) * quantity;
      nextHp = Math.min(maxHp, currentHp + healAmount);
      resultCode = "MONSTER_HEALED";
      message = `${item.display_name} restored ${nextHp - currentHp} HP.`;
    } else if (item.slug === "revive") {
      if (currentHp > 0) {
        throw createHttpError(400, "MONSTER_NOT_FAINTED", "Monster is not fainted.");
      }

      nextHp = Math.max(1, Math.floor(maxHp / 2));
      resultCode = "MONSTER_REVIVED";
      message = "Monster was revived.";
    } else if (item.slug === "rare-candy") {
      if (nextLevel >= 100 || nextLevel + quantity > 100) {
        throw createHttpError(400, "MAX_LEVEL_REACHED", "Monster has reached the maximum level.");
      }

      const wasFullHp = currentHp >= maxHp;
      nextLevel += quantity;
      const nextMaxHp = calculateMonsterMaxHp({
        ...monster,
        level: nextLevel,
      });
      nextHp = wasFullHp ? nextMaxHp : currentHp;
      resultCode = "LEVEL_INCREASED";
      message = "Monster level increased.";
    }

    await client.query(
      `
      UPDATE game.player_monsters
      SET
        level = $2,
        current_hp = $3,
        updated_at = now()
      WHERE id = $1
      `,
      [monster.id, nextLevel, nextHp]
    );

    await client.query(
      `
      UPDATE game.player_inventory
      SET quantity = quantity - $3,
          updated_at = now()
      WHERE user_id = $1
        AND item_id = $2
      `,
      [req.user.id, item.item_id, quantity]
    );

    await incrementQuestProgress(req.user.id, "use_item", { itemSlug: item.slug, quantity }, client);
    await client.query("COMMIT");

    const [inventory, updatedMonster, team] = await Promise.all([
      getCurrentInventoryRows(req.user.id),
      getMonsterSnapshot(req.user.id, monster.id),
      getCurrentTeamRows(req.user.id),
    ]);

    res.json({
      ok: true,
      result: {
        code: resultCode,
        message,
      },
      item,
      quantity,
      monster: updatedMonster,
      inventory,
      team,
    });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    if (!error.code || error.code === "23514" || error.code === "23503") {
      error.status = error.status || 500;
      error.code = "ITEM_USE_FAILED";
    }
    throw error;
  } finally {
    client.release();
  }
}

app.get("/api/me/monsters/:playerMonsterId", authRequired, asyncRoute(getMonsterDetail));
app.post("/api/items/use", authRequired, asyncRoute(useInventoryItem));

// =======================================================
// Evolutions
// =======================================================

function isLevelEvolutionTrigger(triggerType) {
  const trigger = String(triggerType || "").toLowerCase();
  return trigger === "level" || trigger === "level-up" || trigger === "min_level";
}

function isItemEvolutionTrigger(triggerType) {
  const trigger = String(triggerType || "").toLowerCase();
  return trigger === "item" || trigger === "use-item" || trigger === "stone" || trigger === "evolution-stone";
}

function evolutionReason(code, rule) {
  if (code === "LEVEL_TOO_LOW") return `Requiere nivel ${rule.required_level}.`;
  if (code === "REQUIRED_ITEM_MISSING") return `Necesitas ${rule.required_item_name || rule.required_item_slug}.`;
  if (code === "UNSUPPORTED_EVOLUTION") return "Esta evolucion requiere una condicion aun no disponible.";
  if (code === "INVALID_EVOLUTION_RULE") return "La regla de evolucion no es valida.";
  return null;
}

function evaluateEvolutionRule(rule, monster) {
  if (isLevelEvolutionTrigger(rule.trigger)) {
    if (Number(rule.required_friendship || 0) > 0 || rule.required_trade || !rule.required_level) {
      return { canEvolve: false, code: "UNSUPPORTED_EVOLUTION" };
    }

    if (Number(monster.level || 1) < Number(rule.required_level)) {
      return { canEvolve: false, code: "LEVEL_TOO_LOW" };
    }

    return { canEvolve: true, code: null };
  }

  if (isItemEvolutionTrigger(rule.trigger)) {
    if (!rule.required_item_slug) {
      return { canEvolve: false, code: "INVALID_EVOLUTION_RULE" };
    }

    if (Number(rule.owned_item_quantity || 0) < 1) {
      return { canEvolve: false, code: "REQUIRED_ITEM_MISSING" };
    }

    return { canEvolve: true, code: null };
  }

  return { canEvolve: false, code: "UNSUPPORTED_EVOLUTION" };
}

function formatEvolutionOption(row, monster) {
  const rule = {
    rule_id: row.rule_id,
    to_species_id: row.to_species_id,
    to_pokemon_name: row.to_pokemon_name,
    to_dex_number: row.to_dex_number,
    to_sprite_path: row.to_sprite_path,
    to_animated_path: row.to_animated_path,
    to_shiny_sprite_path: row.to_shiny_sprite_path,
    to_animated_shiny_path: row.to_animated_shiny_path,
    trigger: row.trigger,
    required_level: row.required_level,
    required_item_slug: row.required_item_slug,
    required_item_name: row.required_item_name,
    required_item_icon_path: row.required_item_icon_path,
    owned_item_quantity: Number(row.owned_item_quantity || 0),
    required_friendship: Number(row.required_friendship || 0),
    required_trade: !!row.required_trade,
    required_condition: row.required_condition,
  };
  const availability = evaluateEvolutionRule(rule, monster);

  return {
    ...rule,
    can_evolve: availability.canEvolve,
    reason_code: availability.code,
    reason: evolutionReason(availability.code, rule),
  };
}

async function getEvolutionRows(userId, speciesId, client = pool) {
  const result = await client.query(
    `
    SELECT
      er.id AS rule_id,
      er.from_species_id,
      er.to_species_id,
      er.trigger_type AS trigger,
      er.required_level,
      er.required_item_slug,
      er.required_friendship,
      er.required_trade,
      er.required_time,
      er.required_condition,
      ts.dex_number AS to_dex_number,
      ts.name AS to_pokemon_name,
      ts.sprite_path AS to_sprite_path,
      ts.animated_path AS to_animated_path,
      ts.shiny_sprite_path AS to_shiny_sprite_path,
      ts.animated_shiny_path AS to_animated_shiny_path,
      i.id AS required_item_id,
      COALESCE(i.display_name, i.name) AS required_item_name,
      i.icon_path AS required_item_icon_path,
      COALESCE(pi.quantity, 0) AS owned_item_quantity
    FROM game.evolution_rules er
    JOIN game.monster_species ts ON ts.id = er.to_species_id
    LEFT JOIN game.items i ON i.slug = er.required_item_slug
    LEFT JOIN game.player_inventory pi ON pi.user_id = $1 AND pi.item_id = i.id
    WHERE er.from_species_id = $2
    ORDER BY
      CASE WHEN er.trigger_type IN ('use-item', 'item', 'stone', 'evolution-stone') THEN 0 ELSE 1 END,
      er.required_level NULLS LAST,
      ts.dex_number
    `,
    [userId, speciesId]
  );

  return result.rows;
}

async function getMonsterEvolutions(req, res) {
  const playerMonsterId = req.params.playerMonsterId;
  const monster = await getMonsterSnapshot(req.user.id, playerMonsterId);

  if (!monster) {
    throw createHttpError(404, "MONSTER_NOT_FOUND", "Monster was not found.");
  }

  const rows = await getEvolutionRows(req.user.id, monster.species_id);

  res.json({
    ok: true,
    monster: {
      player_monster_id: monster.player_monster_id,
      species_id: monster.species_id,
      pokemon_name: monster.pokemon_name,
      level: monster.level,
      is_shiny: monster.is_shiny,
      selected_sprite_path: monster.selected_sprite_path,
    },
    evolutions: rows.map((row) => formatEvolutionOption(row, monster)),
  });
}

function throwEvolutionAvailability(errorCode, rule) {
  if (errorCode === "LEVEL_TOO_LOW") {
    throw createHttpError(400, "LEVEL_TOO_LOW", evolutionReason(errorCode, rule));
  }

  if (errorCode === "REQUIRED_ITEM_MISSING") {
    throw createHttpError(400, "INSUFFICIENT_ITEM", evolutionReason(errorCode, rule));
  }

  if (errorCode === "INVALID_EVOLUTION_RULE") {
    throw createHttpError(400, "INVALID_EVOLUTION_RULE", evolutionReason(errorCode, rule));
  }

  throw createHttpError(400, "EVOLUTION_NOT_AVAILABLE", evolutionReason(errorCode, rule));
}

async function evolveMonster(req, res) {
  const playerMonsterId = String(req.body?.playerMonsterId || req.body?.player_monster_id || "").trim();
  const ruleId = String(req.body?.ruleId || req.body?.rule_id || "").trim();
  const toSpeciesId = req.body?.toSpeciesId || req.body?.to_species_id || null;
  const itemSlug = req.body?.itemSlug || req.body?.item_slug || null;

  if (!playerMonsterId) {
    throw createHttpError(400, "MONSTER_NOT_FOUND", "playerMonsterId is required.");
  }

  if (!ruleId && !toSpeciesId) {
    throw createHttpError(400, "EVOLUTION_NOT_FOUND", "ruleId or toSpeciesId is required.");
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const monsterResult = await client.query(
      `
      SELECT pm.*, ms.name AS pokemon_name, ms.dex_number
      FROM game.player_monsters pm
      JOIN game.monster_species ms ON ms.id = pm.species_id
      WHERE pm.id = $1
      LIMIT 1
      FOR UPDATE
      `,
      [playerMonsterId]
    );

    if (!monsterResult.rows.length) {
      throw createHttpError(404, "MONSTER_NOT_FOUND", "Monster was not found.");
    }

    const monster = monsterResult.rows[0];
    if (String(monster.user_id) !== String(req.user.id)) {
      throw createHttpError(403, "MONSTER_NOT_OWNED", "Monster does not belong to the current user.");
    }

    const ruleParams = [monster.species_id, req.user.id];
    let ruleWhere = "er.from_species_id = $1";

    if (ruleId) {
      ruleParams.push(ruleId);
      ruleWhere += ` AND er.id = $${ruleParams.length}`;
    } else {
      ruleParams.push(Number(toSpeciesId));
      ruleWhere += ` AND er.to_species_id = $${ruleParams.length}`;
      if (itemSlug) {
        ruleParams.push(String(itemSlug));
        ruleWhere += ` AND er.required_item_slug = $${ruleParams.length}`;
      }
    }

    const ruleResult = await client.query(
      `
      SELECT
        er.id AS rule_id,
        er.from_species_id,
        er.to_species_id,
        er.trigger_type AS trigger,
        er.required_level,
        er.required_item_slug,
        er.required_friendship,
        er.required_trade,
        er.required_time,
        er.required_condition,
        fs.name AS from_pokemon_name,
        ts.name AS to_pokemon_name,
        ts.dex_number AS to_dex_number,
        ts.sprite_path AS to_sprite_path,
        ts.animated_path AS to_animated_path,
        ts.shiny_sprite_path AS to_shiny_sprite_path,
        ts.animated_shiny_path AS to_animated_shiny_path,
        i.id AS required_item_id,
        COALESCE(i.display_name, i.name) AS required_item_name,
        i.icon_path AS required_item_icon_path,
        COALESCE(pi.quantity, 0) AS owned_item_quantity
      FROM game.evolution_rules er
      JOIN game.monster_species fs ON fs.id = er.from_species_id
      JOIN game.monster_species ts ON ts.id = er.to_species_id
      LEFT JOIN game.items i ON i.slug = er.required_item_slug
      LEFT JOIN game.player_inventory pi ON pi.user_id = $2 AND pi.item_id = i.id
      WHERE ${ruleWhere}
      LIMIT 1
      `,
      ruleParams
    );

    if (!ruleResult.rows.length) {
      const existingRules = await client.query(
        "SELECT 1 FROM game.evolution_rules WHERE from_species_id = $1 LIMIT 1",
        [monster.species_id]
      );
      if (!existingRules.rows.length) {
        throw createHttpError(400, "ALREADY_FINAL_EVOLUTION", "This monster has no available evolution.");
      }
      throw createHttpError(404, "EVOLUTION_NOT_FOUND", "Evolution rule was not found.");
    }

    let rule = ruleResult.rows[0];

    if (isItemEvolutionTrigger(rule.trigger)) {
      if (!rule.required_item_id) {
        throw createHttpError(400, "INVALID_EVOLUTION_RULE", "Evolution item was not found.");
      }

      const inventoryResult = await client.query(
        `
        SELECT user_id, item_id, quantity
        FROM game.player_inventory
        WHERE user_id = $1
          AND item_id = $2
        FOR UPDATE
        `,
        [req.user.id, rule.required_item_id]
      );

      const lockedQuantity = Number(inventoryResult.rows[0]?.quantity || 0);
      rule = {
        ...rule,
        owned_item_quantity: lockedQuantity,
      };

      if (lockedQuantity < 1) {
        throw createHttpError(400, "INSUFFICIENT_ITEM", evolutionReason("REQUIRED_ITEM_MISSING", rule));
      }
    }

    const availability = evaluateEvolutionRule(rule, monster);
    if (!availability.canEvolve) {
      throwEvolutionAvailability(availability.code, rule);
    }

    if (isItemEvolutionTrigger(rule.trigger)) {
      await client.query(
        `
        UPDATE game.player_inventory
        SET quantity = quantity - 1,
            updated_at = now()
        WHERE user_id = $1
          AND item_id = $2
        `,
        [req.user.id, rule.required_item_id]
      );
    }

    await client.query(
      `
      UPDATE game.player_monsters
      SET species_id = $2,
          updated_at = now()
      WHERE id = $1
      `,
      [monster.id, rule.to_species_id]
    );

    await client.query(
      `
      INSERT INTO game.player_pokedex (
        user_id,
        species_id,
        seen,
        caught,
        shiny_seen,
        shiny_caught,
        total_seen,
        total_caught,
        total_shiny_caught,
        first_seen_at,
        first_caught_at,
        updated_at
      )
      VALUES ($1, $2, true, true, $3, $3, 1, 1, $4, now(), now(), now())
      ON CONFLICT (user_id, species_id)
      DO UPDATE SET
        seen = true,
        caught = true,
        shiny_seen = game.player_pokedex.shiny_seen OR EXCLUDED.shiny_seen,
        shiny_caught = game.player_pokedex.shiny_caught OR EXCLUDED.shiny_caught,
        total_seen = game.player_pokedex.total_seen + 1,
        total_caught = game.player_pokedex.total_caught + 1,
        total_shiny_caught = game.player_pokedex.total_shiny_caught + EXCLUDED.total_shiny_caught,
        first_seen_at = COALESCE(game.player_pokedex.first_seen_at, EXCLUDED.first_seen_at),
        first_caught_at = COALESCE(game.player_pokedex.first_caught_at, EXCLUDED.first_caught_at),
        updated_at = now()
      `,
      [req.user.id, rule.to_species_id, !!monster.is_shiny, monster.is_shiny ? 1 : 0]
    );

    await incrementQuestProgress(
      req.user.id,
      "evolve",
      {
        fromSpeciesId: rule.from_species_id,
        toSpeciesId: rule.to_species_id,
        trigger: rule.trigger,
        itemSlug: rule.required_item_slug,
      },
      client
    );
    const caughtSpeciesCount = await getCaughtSpeciesCount(req.user.id, client);
    await incrementQuestProgress(req.user.id, "pokedex_species", { caughtSpeciesCount }, client);
    await client.query("COMMIT");

    const [updatedMonster, inventory, team, pokedexSummary] = await Promise.all([
      getMonsterSnapshot(req.user.id, monster.id),
      getCurrentInventoryRows(req.user.id),
      getCurrentTeamRows(req.user.id),
      query("SELECT * FROM game.v_player_pokedex_summary WHERE user_id = $1 LIMIT 1", [req.user.id]),
    ]);

    res.json({
      ok: true,
      evolution: {
        rule_id: rule.rule_id,
        from_species_id: rule.from_species_id,
        from_pokemon_name: rule.from_pokemon_name,
        to_species_id: rule.to_species_id,
        to_pokemon_name: rule.to_pokemon_name,
        trigger: rule.trigger,
        consumed_item_slug: isItemEvolutionTrigger(rule.trigger) ? rule.required_item_slug : null,
      },
      monster: updatedMonster,
      inventory,
      team,
      pokedexSummary: pokedexSummary[0] || null,
    });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    if (!error.code || error.code === "23514" || error.code === "23503") {
      error.status = error.status || 500;
      error.code = "EVOLUTION_FAILED";
    }
    throw error;
  } finally {
    client.release();
  }
}

app.get("/api/me/monsters/:playerMonsterId/evolutions", authRequired, asyncRoute(getMonsterEvolutions));
app.post("/api/evolutions/evolve", authRequired, asyncRoute(evolveMonster));

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

async function syncCaptureQuestProgress(userId, captureResult) {
  if (!captureResult?.success) return;

  const speciesRows = await query(
    `
    SELECT
      cl.species_id,
      COALESCE(cl.is_shiny, $3::boolean) AS is_shiny,
      pt.slug AS primary_type,
      st.slug AS secondary_type
    FROM game.capture_logs cl
    JOIN game.monster_species ms ON ms.id = cl.species_id
    LEFT JOIN game.monster_types pt ON pt.id = ms.primary_type_id
    LEFT JOIN game.monster_types st ON st.id = ms.secondary_type_id
    WHERE cl.user_id = $1
      AND ($2::uuid IS NULL OR cl.player_monster_id = $2::uuid)
    ORDER BY cl.created_at DESC
    LIMIT 1
    `,
    [
      userId,
      captureResult.player_monster_id || captureResult.playerMonsterId || null,
      !!captureResult.is_shiny,
    ]
  );

  const capture = speciesRows[0] || {};
  await incrementQuestProgress(userId, "capture", {
    speciesId: capture.species_id || captureResult.species_id,
    primaryTypeSlug: capture.primary_type || captureResult.primary_type,
    secondaryTypeSlug: capture.secondary_type || captureResult.secondary_type,
    isShiny: capture.is_shiny || captureResult.is_shiny,
  });

  const caughtSpeciesCount = await getCaughtSpeciesCount(userId);
  await incrementQuestProgress(userId, "pokedex_species", { caughtSpeciesCount });
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

  const result = rows[0];

  try {
    await syncCaptureQuestProgress(req.user.id, result);
  } catch (error) {
    console.warn("Quest progress failed after capture:", error.code || error.message);
  }

  res.json(result);
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
