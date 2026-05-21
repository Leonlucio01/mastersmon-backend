CREATE TABLE IF NOT EXISTS game.skills (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  slug TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  type_slug TEXT REFERENCES game.monster_types(slug),
  power INTEGER NOT NULL DEFAULT 40,
  accuracy INTEGER NOT NULL DEFAULT 100,
  energy_cost INTEGER NOT NULL DEFAULT 0,
  cooldown_turns INTEGER NOT NULL DEFAULT 0,
  skill_kind TEXT NOT NULL DEFAULT 'damage',
  target TEXT NOT NULL DEFAULT 'enemy',
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS game.monster_species_skills (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  species_id INTEGER NOT NULL REFERENCES game.monster_species(id) ON DELETE CASCADE,
  skill_id UUID NOT NULL REFERENCES game.skills(id) ON DELETE CASCADE,
  unlock_level INTEGER NOT NULL DEFAULT 1,
  slot_order INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (species_id, skill_id)
);

ALTER TABLE game.battle_sessions
ADD COLUMN IF NOT EXISTS battle_state JSONB;

ALTER TABLE game.battle_sessions
ADD COLUMN IF NOT EXISTS target_slug TEXT;

ALTER TABLE game.battle_sessions
ADD COLUMN IF NOT EXISTS winner TEXT;

ALTER TABLE game.battle_sessions
ADD COLUMN IF NOT EXISTS rewards JSONB;

ALTER TABLE game.battle_sessions
ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS actor_side TEXT;

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS actor_monster_name TEXT;

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS target_monster_name TEXT;

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS skill_id UUID REFERENCES game.skills(id);

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS skill_slug TEXT;

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS skill_name TEXT;

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS is_critical BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS type_multiplier NUMERIC NOT NULL DEFAULT 1;

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS result JSONB;

INSERT INTO game.skills (slug, name, description, type_slug, power, accuracy)
VALUES
  ('tackle', 'Tackle', 'A basic physical hit.', 'normal', 40, 100),
  ('quick-attack', 'Quick Attack', 'A fast neutral strike.', 'normal', 45, 100),
  ('ember', 'Ember', 'A small burst of fire.', 'fire', 40, 100),
  ('water-gun', 'Water Gun', 'A focused water shot.', 'water', 40, 100),
  ('vine-whip', 'Vine Whip', 'A snapping grass attack.', 'grass', 45, 100),
  ('thunder-shock', 'Thunder Shock', 'A sharp electric jolt.', 'electric', 40, 100),
  ('rock-throw', 'Rock Throw', 'Throws hard stones.', 'rock', 50, 90),
  ('poison-sting', 'Poison Sting', 'A venomous sting.', 'poison', 35, 100),
  ('bug-bite', 'Bug Bite', 'A quick bug-type bite.', 'bug', 40, 100),
  ('gust', 'Gust', 'A slicing gust of air.', 'flying', 40, 100),
  ('confusion', 'Confusion', 'A psychic pulse.', 'psychic', 50, 100),
  ('mud-slap', 'Mud Slap', 'A ground-type slap of mud.', 'ground', 35, 100),
  ('ice-shard', 'Ice Shard', 'A fast shard of ice.', 'ice', 40, 100),
  ('dragon-breath', 'Dragon Breath', 'A breath of draconic force.', 'dragon', 60, 100),
  ('bite', 'Bite', 'A dark biting attack.', 'dark', 60, 100),
  ('lick', 'Lick', 'A ghostly lick.', 'ghost', 35, 100),
  ('karate-chop', 'Karate Chop', 'A clean fighting strike.', 'fighting', 50, 100)
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  type_slug = EXCLUDED.type_slug,
  power = EXCLUDED.power,
  accuracy = EXCLUDED.accuracy,
  is_active = true,
  updated_at = now();

WITH typed_skill AS (
  SELECT * FROM (
    VALUES
      ('normal', 'tackle'),
      ('fire', 'ember'),
      ('water', 'water-gun'),
      ('grass', 'vine-whip'),
      ('electric', 'thunder-shock'),
      ('rock', 'rock-throw'),
      ('poison', 'poison-sting'),
      ('bug', 'bug-bite'),
      ('flying', 'gust'),
      ('psychic', 'confusion'),
      ('ground', 'mud-slap'),
      ('ice', 'ice-shard'),
      ('dragon', 'dragon-breath'),
      ('dark', 'bite'),
      ('ghost', 'lick'),
      ('fighting', 'karate-chop')
  ) AS t(type_slug, skill_slug)
),
base_assignments AS (
  SELECT ms.id AS species_id, s.id AS skill_id, 1 AS slot_order
  FROM game.monster_species ms
  JOIN game.skills s ON s.slug = 'tackle'
  WHERE ms.is_active = true
),
primary_assignments AS (
  SELECT ms.id AS species_id, s.id AS skill_id, 2 AS slot_order
  FROM game.monster_species ms
  JOIN game.monster_types mt ON mt.id = ms.primary_type_id
  JOIN typed_skill ts ON ts.type_slug = mt.slug
  JOIN game.skills s ON s.slug = ts.skill_slug
  WHERE ms.is_active = true
),
secondary_assignments AS (
  SELECT ms.id AS species_id, s.id AS skill_id, 3 AS slot_order
  FROM game.monster_species ms
  JOIN game.monster_types mt ON mt.id = ms.secondary_type_id
  JOIN typed_skill ts ON ts.type_slug = mt.slug
  JOIN game.skills s ON s.slug = ts.skill_slug
  WHERE ms.is_active = true
),
quick_assignments AS (
  SELECT ms.id AS species_id, s.id AS skill_id, 4 AS slot_order
  FROM game.monster_species ms
  JOIN game.skills s ON s.slug = 'quick-attack'
  WHERE ms.is_active = true
)
INSERT INTO game.monster_species_skills (species_id, skill_id, unlock_level, slot_order)
SELECT species_id, skill_id, 1, slot_order FROM base_assignments
UNION
SELECT species_id, skill_id, 1, slot_order FROM primary_assignments
UNION
SELECT species_id, skill_id, 1, slot_order FROM secondary_assignments
UNION
SELECT species_id, skill_id, 1, slot_order FROM quick_assignments
ON CONFLICT (species_id, skill_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_monster_species_skills_species
ON game.monster_species_skills (species_id, unlock_level, slot_order);

CREATE INDEX IF NOT EXISTS idx_battle_sessions_user_status
ON game.battle_sessions (user_id, status);

CREATE INDEX IF NOT EXISTS idx_battle_turns_battle_created
ON game.battle_turns (battle_id, created_at);
