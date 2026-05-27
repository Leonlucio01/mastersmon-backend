UPDATE game.gyms
SET required_trainer_level = v.required_trainer_level,
    recommended_power = v.recommended_power
FROM (
  VALUES
    ('kanto-boulder-badge', 2, 1000),
    ('kanto-cascade-badge', 4, 2000),
    ('kanto-thunder-badge', 6, 3000),
    ('kanto-rainbow-badge', 8, 4000)
) AS v(slug, required_trainer_level, recommended_power)
WHERE game.gyms.slug = v.slug;

WITH team_seed(gym_slug, dex_number, slot_number, level) AS (
  VALUES
    ('kanto-boulder-badge', 74, 1, 8),
    ('kanto-boulder-badge', 95, 2, 12),
    ('kanto-cascade-badge', 120, 1, 14),
    ('kanto-cascade-badge', 121, 2, 18),
    ('kanto-thunder-badge', 100, 1, 20),
    ('kanto-thunder-badge', 25, 2, 22),
    ('kanto-thunder-badge', 26, 3, 24),
    ('kanto-rainbow-badge', 69, 1, 26),
    ('kanto-rainbow-badge', 114, 2, 29),
    ('kanto-rainbow-badge', 45, 3, 32)
)
INSERT INTO game.gym_trainer_team (gym_id, species_id, slot_number, level)
SELECT
  g.id,
  ms.id,
  ts.slot_number,
  ts.level
FROM team_seed ts
JOIN game.gyms g ON g.slug = ts.gym_slug
JOIN game.monster_species ms ON ms.dex_number = ts.dex_number
WHERE NOT EXISTS (
  SELECT 1
  FROM game.gym_trainer_team existing
  WHERE existing.gym_id = g.id
);
