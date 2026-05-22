CREATE TABLE IF NOT EXISTS game.player_gym_progress (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES game.users(id) ON DELETE CASCADE,
  gym_id UUID NOT NULL REFERENCES game.gyms(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'available',
  wins INTEGER NOT NULL DEFAULT 0,
  best_turns INTEGER NULL,
  last_battle_session_id UUID NULL REFERENCES game.battle_sessions(id) ON DELETE SET NULL,
  completed_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT player_gym_progress_status_check CHECK (status IN ('available', 'completed')),
  CONSTRAINT player_gym_progress_wins_check CHECK (wins >= 0),
  CONSTRAINT player_gym_progress_user_gym_key UNIQUE (user_id, gym_id)
);

CREATE INDEX IF NOT EXISTS idx_player_gym_progress_user
ON game.player_gym_progress (user_id, status, completed_at);

CREATE INDEX IF NOT EXISTS idx_player_gym_progress_gym
ON game.player_gym_progress (gym_id);

INSERT INTO game.achievements (slug, name, description, target_type, target_value, reward_title)
SELECT
  'badge_' || replace(g.slug, '-', '_') AS slug,
  COALESCE(g.badge_name, g.name || ' Badge') AS name,
  'Medalla obtenida al derrotar ' || g.name || '.' AS description,
  'gym_badge' AS target_type,
  1 AS target_value,
  COALESCE(g.badge_name, g.name || ' Badge') AS reward_title
FROM game.gyms g
ON CONFLICT (slug) DO UPDATE
SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  target_type = EXCLUDED.target_type,
  target_value = EXCLUDED.target_value,
  reward_title = EXCLUDED.reward_title;

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
  ('win_1_battle', 'Gana 1 batalla', 'Gana una batalla PvE.', 'main', 'battle_win', 1, 1000, NULL, 0, 90),
  ('defeat_1_gym', 'Derrota 1 gimnasio', 'Derrota a un líder de gimnasio.', 'main', 'gym_win', 1, 2000, (SELECT id FROM game.items WHERE slug = 'rare-candy' LIMIT 1), 1, 91),
  ('earn_1_badge', 'Consigue tu primera medalla', 'Obtén una medalla de gimnasio.', 'main', 'badge_earned', 1, 0, (SELECT id FROM game.items WHERE slug = 'great-ball' LIMIT 1), 5, 92)
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
