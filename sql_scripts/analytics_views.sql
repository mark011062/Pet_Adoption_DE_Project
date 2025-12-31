-- =========================
-- DROP (so re-running is safe)
-- =========================
DROP VIEW IF EXISTS v_latest_animals_by_type;
DROP VIEW IF EXISTS v_latest_animals_by_breed;
DROP VIEW IF EXISTS v_latest_animals_by_shelter;
DROP VIEW IF EXISTS v_latest_adoptions_by_shelter;

DROP VIEW IF EXISTS v_animals_by_type_by_day;
DROP VIEW IF EXISTS v_adoptions_by_shelter_by_day;

-- =========================
-- A) LATEST SNAPSHOT (CURRENT STATE)
-- =========================

CREATE VIEW v_latest_animals_by_type AS
SELECT pt.type, COUNT(*) AS total_animals
FROM animals a
JOIN pet_types pt ON pt.type_id = a.type_id
WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM animals)
GROUP BY pt.type
ORDER BY total_animals DESC;

CREATE VIEW v_latest_animals_by_breed AS
SELECT b.breed, COUNT(*) AS total_animals
FROM animals a
JOIN breeds b ON b.breed_id = a.breed_id
WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM animals)
GROUP BY b.breed
ORDER BY total_animals DESC;

CREATE VIEW v_latest_animals_by_shelter AS
SELECT s.shelter_name, COUNT(*) AS total_animals
FROM animals a
JOIN shelters s ON s.shelter_id = a.shelter_id
WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM animals)
GROUP BY s.shelter_name
ORDER BY total_animals DESC;

CREATE VIEW v_latest_adoptions_by_shelter AS
SELECT s.shelter_name, COUNT(*) AS total_adoptions
FROM animals a
JOIN shelters s ON s.shelter_id = a.shelter_id
WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM animals)
  AND a.adopted_date IS NOT NULL
GROUP BY s.shelter_name
ORDER BY total_adoptions DESC;

-- =========================
-- B) TRENDS (OVER TIME)
-- =========================

CREATE VIEW v_animals_by_type_by_day AS
SELECT
  a.snapshot_date,
  pt.type,
  COUNT(*) AS total_animals
FROM animals a
JOIN pet_types pt ON pt.type_id = a.type_id
GROUP BY a.snapshot_date, pt.type
ORDER BY a.snapshot_date, pt.type;

CREATE VIEW v_adoptions_by_shelter_by_day AS
SELECT
  a.snapshot_date,
  s.shelter_name,
  COUNT(*) AS total_adoptions
FROM animals a
JOIN shelters s ON s.shelter_id = a.shelter_id
WHERE a.adopted_date IS NOT NULL
GROUP BY a.snapshot_date, s.shelter_name
ORDER BY a.snapshot_date, s.shelter_name;


-- =========================
-- C) LATEST SNAPSHOT HELPERS (PORTFOLIO-FRIENDLY)
-- =========================

DROP VIEW IF EXISTS v_latest_animals;
DROP VIEW IF EXISTS v_latest_available_animals;
DROP VIEW IF EXISTS v_latest_adopted_animals;

-- Base: latest snapshot rows only
CREATE VIEW v_latest_animals AS
SELECT *
FROM animals
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM animals);

-- Latest snapshot: available animals (status-based)
CREATE VIEW v_latest_available_animals AS
SELECT *
FROM v_latest_animals
WHERE status ILIKE 'available';

-- Latest snapshot: adopted animals (date-based, safest)
CREATE VIEW v_latest_adopted_animals AS
SELECT *
FROM v_latest_animals
WHERE adopted_date IS NOT NULL;
