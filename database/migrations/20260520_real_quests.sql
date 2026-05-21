ALTER TABLE game.quests
ADD COLUMN IF NOT EXISTS target_type_slug TEXT;

ALTER TABLE game.quests
ADD COLUMN IF NOT EXISTS target_item_slug TEXT;

ALTER TABLE game.quests
ADD COLUMN IF NOT EXISTS is_daily BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE game.quests
ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0;

ALTER TABLE game.quests
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE game.player_quests
ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';

ALTER TABLE game.player_quests
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE game.player_quests
SET status = CASE
  WHEN claimed THEN 'claimed'
  WHEN completed THEN 'completed'
  ELSE 'active'
END,
updated_at = now()
WHERE status IS NULL OR status NOT IN ('active', 'completed', 'claimed');

WITH quest_seed AS (
  SELECT *
  FROM (
    VALUES
      ('capture_3', 'Captura 3 criaturas', 'Captura cualquier 3 criaturas salvajes.', 'main', 'capture', 3, NULL, NULL, 1000, 0, 'poke-ball', 5, false, 10),
      ('capture_10', 'Captura 10 criaturas', 'Captura cualquier 10 criaturas salvajes.', 'main', 'capture', 10, NULL, NULL, 3000, 0, 'rare-candy', 1, false, 20),
      ('buy_5_items', 'Compra 5 items', 'Compra 5 items en la tienda.', 'main', 'buy_item', 5, NULL, NULL, 1500, 0, NULL, 0, false, 30),
      ('use_1_item', 'Usa 1 item', 'Usa un item desde la Mochila.', 'main', 'use_item', 1, NULL, NULL, 0, 0, 'potion', 3, false, 40),
      ('evolve_1', 'Evoluciona 1 criatura', 'Evoluciona una criatura de tu coleccion.', 'main', 'evolve', 1, NULL, NULL, 2000, 0, 'rare-candy', 1, false, 50),
      ('team_6', 'Completa tu equipo 6/6', 'Llena los seis slots de tu equipo activo.', 'main', 'team_size', 6, NULL, NULL, 0, 0, 'great-ball', 5, false, 60),
      ('pokedex_10', 'Registra 10 especies en Pokedex', 'Registra 10 especies capturadas en la Pokedex.', 'main', 'pokedex_species', 10, NULL, NULL, 1500, 0, 'poke-ball', 10, false, 70),
      ('capture_grass_3', 'Captura 3 criaturas de tipo Planta', 'Captura 3 criaturas cuyo tipo sea Planta.', 'main', 'capture_type', 3, 'grass', NULL, 0, 0, 'leaf-stone', 1, false, 80)
  ) AS q(slug, name, description, quest_type, target_type, target_value, target_type_slug, target_item_slug, reward_gold, reward_diamonds, reward_item_slug, reward_item_quantity, is_daily, sort_order)
)
INSERT INTO game.quests (
  slug,
  name,
  description,
  quest_type,
  target_type,
  target_value,
  target_type_slug,
  target_item_slug,
  reward_gold,
  reward_diamonds,
  reward_item_id,
  reward_item_quantity,
  is_daily,
  is_active,
  sort_order,
  updated_at
)
SELECT
  qs.slug,
  qs.name,
  qs.description,
  qs.quest_type,
  qs.target_type,
  qs.target_value,
  qs.target_type_slug,
  qs.target_item_slug,
  qs.reward_gold,
  qs.reward_diamonds,
  i.id,
  qs.reward_item_quantity,
  qs.is_daily,
  true,
  qs.sort_order,
  now()
FROM quest_seed qs
LEFT JOIN game.items i ON i.slug = qs.reward_item_slug
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  quest_type = EXCLUDED.quest_type,
  target_type = EXCLUDED.target_type,
  target_value = EXCLUDED.target_value,
  target_type_slug = EXCLUDED.target_type_slug,
  target_item_slug = EXCLUDED.target_item_slug,
  reward_gold = EXCLUDED.reward_gold,
  reward_diamonds = EXCLUDED.reward_diamonds,
  reward_item_id = EXCLUDED.reward_item_id,
  reward_item_quantity = EXCLUDED.reward_item_quantity,
  is_daily = EXCLUDED.is_daily,
  is_active = true,
  sort_order = EXCLUDED.sort_order,
  updated_at = now();
