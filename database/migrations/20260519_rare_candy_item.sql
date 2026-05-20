INSERT INTO game.item_categories (slug, name)
VALUES ('candy', 'Candy')
ON CONFLICT (slug)
DO UPDATE SET name = EXCLUDED.name;

INSERT INTO game.items (
  slug,
  name,
  display_name,
  category_id,
  icon_path,
  cost_gold,
  cost_diamonds,
  capture_bonus,
  heal_amount,
  is_custom,
  is_premium,
  is_tradeable
)
SELECT
  'rare-candy',
  'Rare Candy',
  'Rare Candy',
  c.id,
  '/img/items/official/rare-candy.png',
  0,
  25,
  1.00,
  0,
  false,
  true,
  true
FROM game.item_categories c
WHERE c.slug = 'candy'
ON CONFLICT (slug)
DO UPDATE SET
  display_name = EXCLUDED.display_name,
  category_id = EXCLUDED.category_id,
  icon_path = EXCLUDED.icon_path,
  cost_gold = EXCLUDED.cost_gold,
  cost_diamonds = EXCLUDED.cost_diamonds,
  capture_bonus = EXCLUDED.capture_bonus,
  heal_amount = EXCLUDED.heal_amount,
  is_premium = EXCLUDED.is_premium,
  updated_at = now();
