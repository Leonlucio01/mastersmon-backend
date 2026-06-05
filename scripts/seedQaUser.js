import dotenv from "dotenv";
import pg from "pg";
import bcrypt from "bcryptjs";

dotenv.config();

const { Pool } = pg;

const QA_EMAIL = "qa@mastersmon.com";
const QA_TRAINER_NAME = "QA Trainer";
const QA_PASSWORD = process.env.QA_USER_PASSWORD;
const DATABASE_URL = process.env.DATABASE_URL;

const QA_WALLET = {
  gold: 75000,
  diamonds: 500,
  boss_tickets: 8,
};

const QA_INVENTORY = [
  ["poke-ball", 80],
  ["great-ball", 40],
  ["ultra-ball", 20],
  ["master-ball", 2],
  ["potion", 30],
  ["super-potion", 15],
  ["hyper-potion", 8],
  ["revive", 8],
  ["rare-candy", 10],
  ["fire-stone", 2],
  ["water-stone", 2],
  ["thunder-stone", 2],
  ["leaf-stone", 2],
];

const QA_MONSTERS = [
  { nickname: "QA: Stone Pikachu", dexNumber: 25, slug: "pikachu", level: 30, teamSlot: 1, ivs: [14, 16, 11] },
  { nickname: "QA: Level Bulbasaur", dexNumber: 1, slug: "bulbasaur", level: 16, teamSlot: 2, ivs: [11, 12, 14] },
  { nickname: "QA: Fire Charmander", dexNumber: 4, slug: "charmander", level: 18, teamSlot: 3, ivs: [10, 15, 10] },
  { nickname: "QA: Water Squirtle", dexNumber: 7, slug: "squirtle", level: 18, teamSlot: 4, ivs: [13, 10, 15] },
  { nickname: "QA: Rock Geodude", dexNumber: 74, slug: "geodude", level: 16, teamSlot: 5, ivs: [16, 13, 16] },
  { nickname: "QA: Ghost Gastly", dexNumber: 92, slug: "gastly", level: 20, teamSlot: 6, ivs: [9, 16, 8] },
  { nickname: "QA: Stone Eevee", dexNumber: 133, slug: "eevee", level: 22, teamSlot: null, ivs: [13, 13, 13] },
  { nickname: "QA: Trade Vulpix", dexNumber: 37, slug: "vulpix", level: 19, teamSlot: null, ivs: [10, 12, 11] },
  { nickname: "QA: Market Ekans", dexNumber: 23, slug: "ekans", level: 14, teamSlot: null, ivs: [8, 14, 9] },
  { nickname: "QA: Shiny Pidgey", dexNumber: 16, slug: "pidgey", level: 12, teamSlot: null, isShiny: true, ivs: [12, 12, 10] },
  { nickname: "QA: Bug Caterpie", dexNumber: 10, slug: "caterpie", level: 9, teamSlot: null, ivs: [7, 8, 8] },
  { nickname: "QA: Locked Abra", dexNumber: 63, slug: "abra", level: 21, teamSlot: null, isLocked: true, ivs: [9, 15, 7] },
];

if (!QA_PASSWORD) {
  console.error("Missing QA_USER_PASSWORD environment variable. Refusing to seed QA user without an explicit password.");
  process.exit(1);
}

if (String(QA_PASSWORD).length < 6) {
  console.error("QA_USER_PASSWORD must be at least 6 characters.");
  process.exit(1);
}

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL environment variable.");
  process.exit(1);
}

const needsSsl =
  DATABASE_URL.includes("sslmode=require") ||
  (!DATABASE_URL.includes("localhost") && !DATABASE_URL.includes("127.0.0.1"));

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: needsSsl ? { rejectUnauthorized: false } : undefined,
});

const tableColumnCache = new Map();

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function q(value) {
  return `"${value.replaceAll('"', '""')}"`;
}

async function getColumns(client, tableName) {
  if (tableColumnCache.has(tableName)) return tableColumnCache.get(tableName);

  const result = await client.query(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'game'
      AND table_name = $1
    `,
    [tableName]
  );

  const columns = new Set(result.rows.map((row) => row.column_name));
  tableColumnCache.set(tableName, columns);
  return columns;
}

async function tableExists(client, tableName) {
  const result = await client.query(
    `
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'game'
      AND table_name = $1
    LIMIT 1
    `,
    [tableName]
  );
  return result.rows.length > 0;
}

function setIfColumn(payload, columns, columnName, value) {
  if (columns.has(columnName)) payload[columnName] = value;
}

async function upsertQaUser(client, passwordHash) {
  const userColumns = await getColumns(client, "users");
  const email = normalizeEmail(QA_EMAIL);
  const existing = await client.query(
    "SELECT id FROM game.users WHERE email = $1 LIMIT 1 FOR UPDATE",
    [email]
  );

  if (existing.rows.length) {
    const updates = {
      password_hash: passwordHash,
    };
    setIfColumn(updates, userColumns, "is_active", true);
    setIfColumn(updates, userColumns, "updated_at", new Date());

    const columns = Object.keys(updates);
    await client.query(
      `
      UPDATE game.users
      SET ${columns.map((column, index) => `${q(column)} = $${index + 1}`).join(", ")}
      WHERE id = $${columns.length + 1}
      `,
      [...columns.map((column) => updates[column]), existing.rows[0].id]
    );
    return { userId: existing.rows[0].id, action: "updated" };
  }

  const insertPayload = {
    email,
    password_hash: passwordHash,
  };
  setIfColumn(insertPayload, userColumns, "is_active", true);

  const columns = Object.keys(insertPayload);
  const inserted = await client.query(
    `
    INSERT INTO game.users (${columns.map(q).join(", ")})
    VALUES (${columns.map((_, index) => `$${index + 1}`).join(", ")})
    RETURNING id
    `,
    columns.map((column) => insertPayload[column])
  );

  return { userId: inserted.rows[0].id, action: "created" };
}

async function upsertProfile(client, userId) {
  const profileColumns = await getColumns(client, "trainer_profiles");
  const existing = await client.query(
    "SELECT id FROM game.trainer_profiles WHERE user_id = $1 LIMIT 1 FOR UPDATE",
    [userId]
  );

  if (existing.rows.length) {
    const updates = {};
    setIfColumn(updates, profileColumns, "trainer_name", QA_TRAINER_NAME);
    setIfColumn(updates, profileColumns, "updated_at", new Date());

    if (Object.keys(updates).length) {
      const updateColumns = Object.keys(updates);
      await client.query(
        `
        UPDATE game.trainer_profiles
        SET ${updateColumns.map((column, index) => `${q(column)} = $${index + 1}`).join(", ")}
        WHERE user_id = $${updateColumns.length + 1}
        `,
        [...updateColumns.map((column) => updates[column]), userId]
      );
    }
    return "updated";
  }

  const insertPayload = {
    user_id: userId,
  };
  setIfColumn(insertPayload, profileColumns, "trainer_name", QA_TRAINER_NAME);

  const columns = Object.keys(insertPayload);
  await client.query(
    `
    INSERT INTO game.trainer_profiles (${columns.map(q).join(", ")})
    VALUES (${columns.map((_, index) => `$${index + 1}`).join(", ")})
    `,
    columns.map((column) => insertPayload[column])
  );
  return "created";
}

async function upsertWallet(client, userId) {
  const walletColumns = await getColumns(client, "trainer_wallets");
  const existing = await client.query(
    "SELECT user_id FROM game.trainer_wallets WHERE user_id = $1 LIMIT 1 FOR UPDATE",
    [userId]
  );

  const payload = { user_id: userId };
  for (const [column, value] of Object.entries(QA_WALLET)) {
    setIfColumn(payload, walletColumns, column, value);
  }
  setIfColumn(payload, walletColumns, "updated_at", new Date());

  if (!existing.rows.length) {
    const insertColumns = Object.keys(payload);
    await client.query(
      `
      INSERT INTO game.trainer_wallets (${insertColumns.map(q).join(", ")})
      VALUES (${insertColumns.map((_, index) => `$${index + 1}`).join(", ")})
      `,
      insertColumns.map((column) => payload[column])
    );
    return "created";
  }

  const updates = { ...payload };
  delete updates.user_id;
  const updateColumns = Object.keys(updates);
  await client.query(
    `
    UPDATE game.trainer_wallets
    SET ${updateColumns.map((column, index) => `${q(column)} = $${index + 1}`).join(", ")}
    WHERE user_id = $${updateColumns.length + 1}
    `,
    [...updateColumns.map((column) => updates[column]), userId]
  );
  return "updated";
}

async function upsertInventory(client, userId) {
  const inventoryColumns = await getColumns(client, "player_inventory");
  const missingItems = [];
  const seededItems = [];

  for (const [slug, quantity] of QA_INVENTORY) {
    const itemResult = await client.query(
      "SELECT id, slug FROM game.items WHERE slug = $1 LIMIT 1",
      [slug]
    );

    if (!itemResult.rows.length) {
      missingItems.push(slug);
      continue;
    }

    const item = itemResult.rows[0];
    const existing = await client.query(
      "SELECT user_id, item_id FROM game.player_inventory WHERE user_id = $1 AND item_id = $2 LIMIT 1 FOR UPDATE",
      [userId, item.id]
    );

    if (existing.rows.length) {
      const updates = { quantity };
      setIfColumn(updates, inventoryColumns, "updated_at", new Date());
      const updateColumns = Object.keys(updates);
      await client.query(
        `
        UPDATE game.player_inventory
        SET ${updateColumns.map((column, index) => `${q(column)} = $${index + 1}`).join(", ")}
        WHERE user_id = $${updateColumns.length + 1}
          AND item_id = $${updateColumns.length + 2}
        `,
        [...updateColumns.map((column) => updates[column]), userId, item.id]
      );
    } else {
      const insertPayload = {
        user_id: userId,
        item_id: item.id,
        quantity,
      };
      const insertColumns = Object.keys(insertPayload);
      await client.query(
        `
        INSERT INTO game.player_inventory (${insertColumns.map(q).join(", ")})
        VALUES (${insertColumns.map((_, index) => `$${index + 1}`).join(", ")})
        `,
        insertColumns.map((column) => insertPayload[column])
      );
    }

    seededItems.push({ slug, quantity });
  }

  return { missingItems, seededItems };
}

async function findSpecies(client, spec) {
  const result = await client.query(
    `
    SELECT
      ms.id,
      ms.dex_number,
      ms.slug,
      ms.pokemon_name,
      ms.base_hp,
      ms.rarity,
      pt.slug AS primary_type,
      st.slug AS secondary_type
    FROM game.monster_species ms
    LEFT JOIN game.monster_types pt ON pt.id = ms.primary_type_id
    LEFT JOIN game.monster_types st ON st.id = ms.secondary_type_id
    WHERE (LOWER(ms.slug) = $1 OR ms.dex_number = $2)
      AND COALESCE(ms.is_active, true) = true
    ORDER BY CASE WHEN LOWER(ms.slug) = $1 THEN 0 ELSE 1 END
    LIMIT 1
    `,
    [spec.slug, spec.dexNumber]
  );

  return result.rows[0] || null;
}

function maxHpFor(species, level, ivHp) {
  const baseHp = Number(species?.base_hp || 20);
  return Math.max(1, baseHp + level * 3 + Number(ivHp || 0));
}

async function upsertMonster(client, userId, spec, species) {
  const monsterColumns = await getColumns(client, "player_monsters");
  const [ivHp, ivAttack, ivDefense] = spec.ivs || [10, 10, 10];
  const currentHp = maxHpFor(species, spec.level, ivHp);

  const payload = {
    user_id: userId,
    species_id: species.id,
  };

  setIfColumn(payload, monsterColumns, "level", spec.level);
  setIfColumn(payload, monsterColumns, "exp", 0);
  setIfColumn(payload, monsterColumns, "current_hp", currentHp);
  setIfColumn(payload, monsterColumns, "iv_hp", ivHp);
  setIfColumn(payload, monsterColumns, "iv_attack", ivAttack);
  setIfColumn(payload, monsterColumns, "iv_defense", ivDefense);
  setIfColumn(payload, monsterColumns, "is_shiny", !!spec.isShiny);
  setIfColumn(payload, monsterColumns, "nickname", spec.nickname);
  setIfColumn(payload, monsterColumns, "status", "healthy");
  setIfColumn(payload, monsterColumns, "friendship", 90);
  setIfColumn(payload, monsterColumns, "is_locked", !!spec.isLocked);
  setIfColumn(payload, monsterColumns, "is_favorite", !!spec.isFavorite);
  setIfColumn(payload, monsterColumns, "captured_at", new Date());
  setIfColumn(payload, monsterColumns, "updated_at", new Date());

  const existing = await client.query(
    "SELECT id FROM game.player_monsters WHERE user_id = $1 AND nickname = $2 LIMIT 1 FOR UPDATE",
    [userId, spec.nickname]
  );

  if (existing.rows.length) {
    const updates = { ...payload };
    delete updates.user_id;
    delete updates.captured_at;
    const updateColumns = Object.keys(updates);
    await client.query(
      `
      UPDATE game.player_monsters
      SET ${updateColumns.map((column, index) => `${q(column)} = $${index + 1}`).join(", ")}
      WHERE id = $${updateColumns.length + 1}
      `,
      [...updateColumns.map((column) => updates[column]), existing.rows[0].id]
    );
    return existing.rows[0].id;
  }

  const insertColumns = Object.keys(payload);
  const inserted = await client.query(
    `
    INSERT INTO game.player_monsters (${insertColumns.map(q).join(", ")})
    VALUES (${insertColumns.map((_, index) => `$${index + 1}`).join(", ")})
    RETURNING id
    `,
    insertColumns.map((column) => payload[column])
  );

  return inserted.rows[0].id;
}

async function ensureTeam(client, userId, teamEntries) {
  for (let slot = 1; slot <= 6; slot += 1) {
    await client.query(
      `
      INSERT INTO game.player_team_slots (user_id, slot_number)
      VALUES ($1, $2)
      ON CONFLICT (user_id, slot_number) DO NOTHING
      `,
      [userId, slot]
    );
  }

  await client.query(
    `
    UPDATE game.player_team_slots
    SET player_monster_id = NULL, updated_at = now()
    WHERE user_id = $1
    `,
    [userId]
  );

  for (const entry of teamEntries) {
    await client.query(
      `
      UPDATE game.player_team_slots
      SET player_monster_id = $3, updated_at = now()
      WHERE user_id = $1
        AND slot_number = $2
      `,
      [userId, entry.slot, entry.playerMonsterId]
    );
  }
}

async function markPokedexCaught(client, userId, monster) {
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
      total_seen = GREATEST(game.player_pokedex.total_seen, 1),
      total_caught = GREATEST(game.player_pokedex.total_caught, 1),
      total_shiny_caught = GREATEST(game.player_pokedex.total_shiny_caught, EXCLUDED.total_shiny_caught),
      first_seen_at = COALESCE(game.player_pokedex.first_seen_at, EXCLUDED.first_seen_at),
      first_caught_at = COALESCE(game.player_pokedex.first_caught_at, EXCLUDED.first_caught_at),
      updated_at = now()
    `,
    [userId, monster.speciesId, !!monster.isShiny, monster.isShiny ? 1 : 0]
  );
}

async function upsertMonstersAndTeam(client, userId) {
  const missingSpecies = [];
  const seededMonsters = [];
  const teamEntries = [];

  for (const spec of QA_MONSTERS) {
    const species = await findSpecies(client, spec);
    if (!species) {
      missingSpecies.push(`${spec.slug} (#${spec.dexNumber})`);
      continue;
    }

    const playerMonsterId = await upsertMonster(client, userId, spec, species);
    const seeded = {
      playerMonsterId,
      nickname: spec.nickname,
      speciesId: species.id,
      pokemonName: species.pokemon_name,
      slug: species.slug,
      level: spec.level,
      teamSlot: spec.teamSlot,
      isShiny: !!spec.isShiny,
      isLocked: !!spec.isLocked,
      rarity: species.rarity,
      primaryType: species.primary_type,
      secondaryType: species.secondary_type,
    };
    seededMonsters.push(seeded);
    await markPokedexCaught(client, userId, seeded);

    if (spec.teamSlot) {
      teamEntries.push({ slot: spec.teamSlot, playerMonsterId });
    }
  }

  if (teamEntries.length < 6) {
    throw new Error(`QA team needs 6 creatures, but only ${teamEntries.length} could be prepared. Missing species: ${missingSpecies.join(", ") || "none"}`);
  }

  await ensureTeam(client, userId, teamEntries);
  return { seededMonsters, missingSpecies };
}

async function ensurePlayerQuests(client, userId) {
  if (!(await tableExists(client, "quests")) || !(await tableExists(client, "player_quests"))) {
    return { ensured: 0 };
  }

  const result = await client.query(
    `
    INSERT INTO game.player_quests (user_id, quest_id, progress, completed, claimed, status)
    SELECT $1, q.id, 0, false, false, 'active'
    FROM game.quests q
    WHERE q.is_active = true
      AND NOT EXISTS (
        SELECT 1
        FROM game.player_quests pq
        WHERE pq.user_id = $1
          AND pq.quest_id = q.id
      )
    RETURNING id
    `,
    [userId]
  );

  return { ensured: result.rowCount };
}

async function collectOpenListingWarnings(client, userId) {
  const checks = [
    ["trade_offers", "owner_user_id", "status = 'open'"],
    ["market_listings", "seller_user_id", "status IN ('open', 'active')"],
    ["auction_listings", "seller_user_id", "status IN ('open', 'active')"],
  ];
  const warnings = [];

  for (const [tableName, userColumn, statusFilter] of checks) {
    if (!(await tableExists(client, tableName))) continue;
    const result = await client.query(
      `
      SELECT COUNT(*)::int AS count
      FROM game.${q(tableName)}
      WHERE ${q(userColumn)} = $1
        AND ${statusFilter}
      `,
      [userId]
    );
    const count = Number(result.rows[0]?.count || 0);
    if (count > 0) {
      warnings.push(`${tableName}: ${count} open row(s) for QA user were left untouched`);
    }
  }

  return warnings;
}

async function main() {
  const client = await pool.connect();

  try {
    const passwordHash = await bcrypt.hash(String(QA_PASSWORD), 12);
    let summary;

    await client.query("BEGIN");

    const userResult = await upsertQaUser(client, passwordHash);
    const userId = userResult.userId;
    const profileAction = await upsertProfile(client, userId);
    const walletAction = await upsertWallet(client, userId);
    const inventory = await upsertInventory(client, userId);
    const monsters = await upsertMonstersAndTeam(client, userId);
    const quests = await ensurePlayerQuests(client, userId);
    const warnings = await collectOpenListingWarnings(client, userId);

    await client.query("COMMIT");

    summary = {
      user: {
        id: userId,
        email: QA_EMAIL,
        trainerName: QA_TRAINER_NAME,
        action: userResult.action,
      },
      profile: {
        action: profileAction,
      },
      wallet: QA_WALLET,
      walletAction,
      inventory: inventory.seededItems,
      missingItems: inventory.missingItems,
      monsters: monsters.seededMonsters.map((monster) => ({
        nickname: monster.nickname,
        species: monster.pokemonName || monster.slug,
        level: monster.level,
        teamSlot: monster.teamSlot,
        shiny: monster.isShiny,
        locked: monster.isLocked,
        rarity: monster.rarity,
        types: [monster.primaryType, monster.secondaryType].filter(Boolean),
      })),
      missingSpecies: monsters.missingSpecies,
      questsCreated: quests.ensured,
      warnings,
    };

    console.log(`QA user ${userResult.action}: ${QA_EMAIL}`);
    console.log(`Profile ${profileAction}: ${QA_TRAINER_NAME}`);
    console.log(`Wallet OK: ${walletAction}`);
    console.log(`Inventory OK: ${inventory.seededItems.length} item rows set`);
    console.log(`Creatures OK: ${monsters.seededMonsters.length} QA creatures set`);
    console.log("Team OK: 6/6 slots assigned");
    console.log("Pokedex OK: QA species marked seen/caught");
    if (warnings.length) {
      console.warn(`Warnings: ${warnings.join("; ")}`);
    }
    console.log(JSON.stringify(summary, null, 2));
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    console.error("QA seed failed:");
    console.error(error.message || error);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();
