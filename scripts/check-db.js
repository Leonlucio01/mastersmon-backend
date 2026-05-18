import dotenv from "dotenv";
import pg from "pg";

dotenv.config();

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
const CURRENT_USER_EMAIL = process.env.CURRENT_USER_EMAIL;

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL environment variable.");
  process.exit(1);
}

if (!CURRENT_USER_EMAIL) {
  console.error("Missing CURRENT_USER_EMAIL environment variable.");
  process.exit(1);
}

const needsSsl =
  DATABASE_URL.includes("sslmode=require") ||
  (!DATABASE_URL.includes("localhost") && !DATABASE_URL.includes("127.0.0.1"));

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: needsSsl ? { rejectUnauthorized: false } : undefined,
});

async function countRows(label, sql, params = []) {
  const { rows } = await pool.query(sql, params);
  const value = rows[0]?.count ?? rows[0]?.total ?? 0;
  console.log(`${label}: ${value}`);
}

async function validateView(viewName) {
  await pool.query(`SELECT * FROM ${viewName} LIMIT 1`);
  console.log(`OK view: ${viewName}`);
}

async function validateColumn(tableName, columnName) {
  const { rows } = await pool.query(
    `
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'game'
      AND table_name = $1
      AND column_name = $2
    LIMIT 1
    `,
    [tableName, columnName]
  );

  if (!rows.length) {
    throw new Error(`Missing column: game.${tableName}.${columnName}`);
  }

  console.log(`OK column: game.${tableName}.${columnName}`);
}

async function main() {
  try {
    const now = await pool.query("SELECT now() AS server_time");
    console.log(`Connected. DB time: ${now.rows[0].server_time.toISOString()}`);
    console.log(`Current user: ${CURRENT_USER_EMAIL}`);

    await validateColumn("users", "email");
    await validateColumn("users", "password_hash");
    await validateColumn("users", "last_login_at");

    const user = await pool.query(
      "SELECT id, email FROM game.users WHERE email = $1 LIMIT 1",
      [CURRENT_USER_EMAIL]
    );

    if (!user.rows.length) {
      throw new Error(`Current user not found: ${CURRENT_USER_EMAIL}`);
    }

    console.log(`OK fallback user: ${user.rows[0].id}`);

    await validateView("game.v_trainer_profile");
    await validateView("game.v_player_inventory");
    await validateView("game.v_player_collection");
    await validateView("game.v_maps_overview");
    await validateView("game.v_player_pokedex_summary");

    console.log("\nUseful counts");
    await countRows("users", "SELECT COUNT(*) FROM game.users");
    await countRows("monster_species", "SELECT COUNT(*) FROM game.monster_species");
    await countRows("maps", "SELECT COUNT(*) FROM game.maps");
    await countRows("map_spawns", "SELECT COUNT(*) FROM game.map_spawns");
    await countRows("player_monsters", "SELECT COUNT(*) FROM game.player_monsters");
    await countRows("items", "SELECT COUNT(*) FROM game.items");
  } catch (error) {
    console.error("Database validation failed:");
    console.error(error);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
}

main();
