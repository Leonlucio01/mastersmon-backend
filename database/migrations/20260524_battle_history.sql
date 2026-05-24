ALTER TABLE game.battle_sessions
DROP CONSTRAINT IF EXISTS battle_sessions_status_check;

ALTER TABLE game.battle_sessions
ADD CONSTRAINT battle_sessions_status_check
CHECK (status = ANY (ARRAY[
  'active'::text,
  'completed'::text,
  'victory'::text,
  'defeat'::text,
  'draw'::text,
  'expired'::text,
  'cancelled'::text
]));

CREATE INDEX IF NOT EXISTS idx_battle_sessions_user_started
ON game.battle_sessions (user_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_battle_sessions_user_status
ON game.battle_sessions (user_id, status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_battle_turns_battle_turn
ON game.battle_turns (battle_id, turn_number);
