ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS item_slug TEXT;

ALTER TABLE game.battle_turns
ADD COLUMN IF NOT EXISTS item_name TEXT;
