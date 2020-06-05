-- For some reason, this query produces ever so slightly 
-- different results than the classic SQL version. That 
-- is, some bins will feature either one more or one 
-- less element that the one in the reference SQL results.
SELECT
  CAST((
    CASE
      WHEN jet.pt < 15 THEN 15
      WHEN jet.pt > 60 THEN 60
      ELSE jet.pt
    END - 0.375) / 0.45 AS BIGINT) * 0.45 + 0.375 AS x,
  COUNT(*) AS y
FROM memory.cern.view
CROSS JOIN UNNEST(Jets) AS jet 
WHERE eta > 1
GROUP BY CAST((
    CASE
      WHEN jet.pt < 15 THEN 15
      WHEN jet.pt > 60 THEN 60
      ELSE jet.pt
    END - 0.375) / 0.45 AS BIGINT) * 0.45 + 0.375
ORDER BY x;
