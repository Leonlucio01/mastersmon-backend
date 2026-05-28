CREATE TABLE IF NOT EXISTS game.player_arena_progress (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES game.users(id) ON DELETE CASCADE,
  arena_points INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  losses INTEGER NOT NULL DEFAULT 0,
  win_streak INTEGER NOT NULL DEFAULT 0,
  best_streak INTEGER NOT NULL DEFAULT 0,
  current_rank TEXT NOT NULL DEFAULT 'Bronce I',
  highest_rank TEXT NOT NULL DEFAULT 'Bronce I',
  last_battle_session_id UUID NULL REFERENCES game.battle_sessions(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT player_arena_progress_user_key UNIQUE (user_id),
  CONSTRAINT player_arena_progress_nonnegative CHECK (
    arena_points >= 0 AND wins >= 0 AND losses >= 0 AND win_streak >= 0 AND best_streak >= 0
  )
);

CREATE INDEX IF NOT EXISTS idx_player_arena_progress_points
ON game.player_arena_progress (arena_points DESC, wins DESC, best_streak DESC);

CREATE TABLE IF NOT EXISTS game.arena_npc_profiles (
  npc_trainer_id UUID PRIMARY KEY REFERENCES game.npc_trainers(id) ON DELETE CASCADE,
  rank_name TEXT NOT NULL,
  recommended_level INTEGER NOT NULL DEFAULT 10,
  reward_gold INTEGER NOT NULL DEFAULT 1000,
  reward_points INTEGER NOT NULL DEFAULT 25,
  sort_order INTEGER NOT NULL DEFAULT 0,
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS game.arena_npc_teams (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  npc_trainer_id UUID NOT NULL REFERENCES game.npc_trainers(id) ON DELETE CASCADE,
  species_id INTEGER NOT NULL REFERENCES game.monster_species(id) ON DELETE CASCADE,
  slot_number INTEGER NOT NULL,
  level INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT arena_npc_teams_slot_check CHECK (slot_number BETWEEN 1 AND 6),
  CONSTRAINT arena_npc_teams_level_check CHECK (level BETWEEN 1 AND 100),
  CONSTRAINT arena_npc_teams_npc_slot_key UNIQUE (npc_trainer_id, slot_number)
);

CREATE INDEX IF NOT EXISTS idx_arena_npc_teams_npc
ON game.arena_npc_teams (npc_trainer_id, slot_number);

WITH npc_seed(slug, name, rank_name, recommended_level, reward_gold, reward_points, sort_order) AS (
  VALUES
    ('ragnar-bronce-ii', 'Ragnar', 'Bronce II', 10, 1000, 25, 10),
    ('nova-bronce-iii', 'Nova', 'Bronce III', 15, 1400, 35, 20),
    ('mika-plata-i', 'Mika', 'Plata I', 18, 1800, 45, 30),
    ('luna-plata-iii', 'Luna', 'Plata III', 20, 2200, 60, 40),
    ('ashen-oro-iv', 'Ashen', 'Oro IV', 25, 3000, 85, 50)
)
INSERT INTO game.npc_trainers (slug, name, trainer_type, avatar_path, description)
SELECT
  slug,
  name,
  'arena_rival',
  '/img/trainers/arena/' || slug || '.png',
  'Rival de Arena ' || rank_name
FROM npc_seed
ON CONFLICT (slug) DO UPDATE
SET
  name = EXCLUDED.name,
  trainer_type = 'arena_rival',
  description = EXCLUDED.description;

WITH npc_seed(slug, rank_name, recommended_level, reward_gold, reward_points, sort_order) AS (
  VALUES
    ('ragnar-bronce-ii', 'Bronce II', 10, 1000, 25, 10),
    ('nova-bronce-iii', 'Bronce III', 15, 1400, 35, 20),
    ('mika-plata-i', 'Plata I', 18, 1800, 45, 30),
    ('luna-plata-iii', 'Plata III', 20, 2200, 60, 40),
    ('ashen-oro-iv', 'Oro IV', 25, 3000, 85, 50)
)
INSERT INTO game.arena_npc_profiles (
  npc_trainer_id,
  rank_name,
  recommended_level,
  reward_gold,
  reward_points,
  sort_order
)
SELECT
  nt.id,
  ns.rank_name,
  ns.recommended_level,
  ns.reward_gold,
  ns.reward_points,
  ns.sort_order
FROM npc_seed ns
JOIN game.npc_trainers nt ON nt.slug = ns.slug
ON CONFLICT (npc_trainer_id) DO UPDATE
SET
  rank_name = EXCLUDED.rank_name,
  recommended_level = EXCLUDED.recommended_level,
  reward_gold = EXCLUDED.reward_gold,
  reward_points = EXCLUDED.reward_points,
  sort_order = EXCLUDED.sort_order,
  is_active = true,
  updated_at = now();

WITH team_seed(npc_slug, dex_number, slot_number, level) AS (
  VALUES
    ('ragnar-bronce-ii', 58, 1, 10),
    ('ragnar-bronce-ii', 74, 2, 10),
    ('nova-bronce-iii', 92, 1, 15),
    ('nova-bronce-iii', 23, 2, 15),
    ('mika-plata-i', 25, 1, 18),
    ('mika-plata-i', 2, 2, 18),
    ('luna-plata-iii', 133, 1, 20),
    ('luna-plata-iii', 64, 2, 20),
    ('ashen-oro-iv', 147, 1, 25),
    ('ashen-oro-iv', 8, 2, 25)
)
INSERT INTO game.arena_npc_teams (npc_trainer_id, species_id, slot_number, level)
SELECT
  nt.id,
  ms.id,
  ts.slot_number,
  ts.level
FROM team_seed ts
JOIN game.npc_trainers nt ON nt.slug = ts.npc_slug
JOIN game.monster_species ms ON ms.dex_number = ts.dex_number
ON CONFLICT (npc_trainer_id, slot_number) DO UPDATE
SET
  species_id = EXCLUDED.species_id,
  level = EXCLUDED.level;

INSERT INTO game.quests (
  slug,
  name,
  description,
  quest_type,
  target_type,
  target_value,
  reward_gold,
  reward_item_id,
  reward_item_quantity,
  sort_order
)
VALUES
  ('win_1_arena', 'Gana 1 batalla de arena', 'Gana una batalla en la Arena PvE.', 'main', 'arena_win', 1, 1000, NULL, 0, 93),
  ('win_3_arena', 'Gana 3 batallas de arena', 'Gana tres batallas en la Arena PvE.', 'main', 'arena_win', 3, 2000, (SELECT id FROM game.items WHERE slug = 'rare-candy' LIMIT 1), 1, 94),
  ('arena_streak_3', 'Logra racha de 3 victorias', 'Consigue una racha de 3 victorias en Arena.', 'main', 'arena_streak', 3, 0, (SELECT id FROM game.items WHERE slug = 'great-ball' LIMIT 1), 5, 95)
ON CONFLICT (slug) DO UPDATE
SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  quest_type = EXCLUDED.quest_type,
  target_type = EXCLUDED.target_type,
  target_value = EXCLUDED.target_value,
  reward_gold = EXCLUDED.reward_gold,
  reward_item_id = EXCLUDED.reward_item_id,
  reward_item_quantity = EXCLUDED.reward_item_quantity,
  sort_order = EXCLUDED.sort_order,
  is_active = true,
  updated_at = now();
