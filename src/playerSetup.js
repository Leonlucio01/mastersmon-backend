const INITIAL_ITEMS = [
  ["poke-ball", 30],
  ["great-ball", 5],
  ["potion", 10],
  ["super-potion", 3],
  ["revive", 1],
];

const INITIAL_WALLET = {
  gold: 12500,
  diamonds: 150,
  bossTickets: 3,
};

export async function createNewPlayer(pool, { email, passwordHash, trainerName }) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const userResult = await client.query(
      `
      INSERT INTO game.users (email, password_hash)
      VALUES ($1, $2)
      RETURNING id, email
      `,
      [email, passwordHash]
    );
    const user = userResult.rows[0];

    const profileResult = await client.query(
      `
      INSERT INTO game.trainer_profiles (user_id, trainer_name)
      VALUES ($1, $2)
      RETURNING id, user_id, trainer_name, level, exp
      `,
      [user.id, trainerName]
    );
    const profile = profileResult.rows[0];

    await client.query(
      `
      INSERT INTO game.trainer_wallets (user_id, gold, diamonds, boss_tickets)
      VALUES ($1, $2, $3, $4)
      `,
      [user.id, INITIAL_WALLET.gold, INITIAL_WALLET.diamonds, INITIAL_WALLET.bossTickets]
    );

    for (const [slug, quantity] of INITIAL_ITEMS) {
      const itemResult = await client.query(
        "SELECT id FROM game.items WHERE slug = $1 LIMIT 1",
        [slug]
      );

      if (!itemResult.rows.length) {
        console.warn(`Starter inventory item not found: ${slug}`);
        continue;
      }

      await client.query(
        `
        INSERT INTO game.player_inventory (user_id, item_id, quantity)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id, item_id)
        DO UPDATE SET quantity = game.player_inventory.quantity + EXCLUDED.quantity,
                      updated_at = now()
        `,
        [user.id, itemResult.rows[0].id, quantity]
      );
    }

    for (let slot = 1; slot <= 6; slot += 1) {
      await client.query(
        `
        INSERT INTO game.player_team_slots (user_id, slot_number)
        VALUES ($1, $2)
        ON CONFLICT (user_id, slot_number) DO NOTHING
        `,
        [user.id, slot]
      );
    }

    const starterResult = await client.query(
      `
      SELECT id, base_hp
      FROM game.monster_species
      WHERE dex_number = 25 AND is_active = true
      LIMIT 1
      `
    );

    if (!starterResult.rows.length) {
      const error = new Error("Starter species Pikachu was not found.");
      error.status = 500;
      throw error;
    }

    const starter = starterResult.rows[0];
    const monsterResult = await client.query(
      `
      INSERT INTO game.player_monsters (
        user_id,
        species_id,
        level,
        is_shiny,
        current_hp,
        friendship
      )
      VALUES ($1, $2, 5, false, $3, 70)
      RETURNING id
      `,
      [user.id, starter.id, starter.base_hp]
    );
    const starterMonsterId = monsterResult.rows[0].id;

    await client.query(
      `
      UPDATE game.player_team_slots
      SET player_monster_id = $3,
          updated_at = now()
      WHERE user_id = $1 AND slot_number = $2
      `,
      [user.id, 1, starterMonsterId]
    );

    await client.query(
      `
      INSERT INTO game.player_pokedex (
        user_id,
        species_id,
        seen,
        caught,
        total_seen,
        total_caught,
        first_seen_at,
        first_caught_at
      )
      VALUES ($1, $2, true, true, 1, 1, now(), now())
      ON CONFLICT (user_id, species_id)
      DO UPDATE SET seen = true,
                    caught = true,
                    total_seen = GREATEST(game.player_pokedex.total_seen, 1),
                    total_caught = GREATEST(game.player_pokedex.total_caught, 1),
                    first_seen_at = COALESCE(game.player_pokedex.first_seen_at, now()),
                    first_caught_at = COALESCE(game.player_pokedex.first_caught_at, now()),
                    updated_at = now()
      `,
      [user.id, starter.id]
    );

    await client.query("COMMIT");

    return {
      user,
      profile,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
