SELECT
  CAST((
    CASE
      WHEN jet.pt < 15 THEN 15
      WHEN jet.pt > 60 THEN 60
      ELSE jet.pt
    END - 0.225) / 0.45 AS BIGINT) * 0.45 + 0.225 AS x,
  COUNT(*) AS y
FROM {input_table}
CROSS JOIN UNNEST(Jets) AS jet
GROUP BY CAST((
    CASE
      WHEN jet.pt < 15 THEN 15
      WHEN jet.pt > 60 THEN 60
      ELSE jet.pt
    END - 0.225) / 0.45 AS BIGINT) * 0.45 + 0.225
ORDER BY x;
