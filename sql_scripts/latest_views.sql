-- Latest snapshot_file = most recently loaded snapshot
CREATE OR REPLACE VIEW v_latest_snapshot AS
SELECT snapshot_file, snapshot_date, loaded_at
FROM loaded_snapshots
ORDER BY loaded_at DESC
LIMIT 1;

-- All animals from the latest snapshot
CREATE OR REPLACE VIEW v_latest_animals AS
SELECT a.*
FROM animals a
JOIN v_latest_snapshot ls
  ON a.snapshot_file = ls.snapshot_file;

-- Latest available animals
CREATE OR REPLACE VIEW v_latest_available_animals AS
SELECT *
FROM v_latest_animals
WHERE status ILIKE 'available'
  AND adopted_date IS NULL;

-- Latest adopted animals
CREATE OR REPLACE VIEW v_latest_adopted_animals AS
SELECT *
FROM v_latest_animals
WHERE adopted_date IS NOT NULL
   OR status ILIKE 'adopted';
