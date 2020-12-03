SELECT
  CAST((
    CASE
      WHEN MET_sumet < 0 THEN 0
      WHEN MET_sumet > 2000 THEN 2000
      ELSE MET_sumet
    END - 10) / 20 AS BIGINT) * 20 + 10 AS x,
  COUNT(*) AS y
FROM memory.cern.view
GROUP BY CAST((
    CASE
      WHEN MET_sumet < 0 THEN 0
      WHEN MET_sumet > 2000 THEN 2000
      ELSE MET_sumet
    END - 10) / 20 AS BIGINT) * 20 + 10
ORDER BY x;
