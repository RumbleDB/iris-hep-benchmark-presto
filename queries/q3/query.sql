SELECT
  CAST((
    CASE
      WHEN jet.pt < 15 THEN 15
      WHEN jet.pt > 60 THEN 60
      ELSE jet.pt
    END - 0.225) / 0.45 AS BIGINT) * 0.45 + 0.225 AS x,
  COUNT(*) AS y
FROM memory.cern.view
CROSS JOIN UNNEST(Jets) AS jet 
WHERE abs(eta) < 1
GROUP BY CAST((
    CASE
      WHEN jet.pt < 15 THEN 15
      WHEN jet.pt > 60 THEN 60
      ELSE jet.pt
    END - 0.225) / 0.45 AS BIGINT) * 0.45 + 0.225
ORDER BY x;