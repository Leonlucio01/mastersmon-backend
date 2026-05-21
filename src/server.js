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
