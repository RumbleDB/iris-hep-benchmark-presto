SELECT
  CAST((
    CASE
      WHEN j.pt < 15 THEN 15
      WHEN j.pt > 60 THEN 60
      ELSE j.pt
    END - 0.225) / 0.45 AS BIGINT) * 0.45 + 0.225 AS x,
  COUNT(*) AS y
FROM {input_table}
CROSS JOIN UNNEST(Jet) AS j
WHERE abs(eta) < 1
GROUP BY CAST((
    CASE
      WHEN j.pt < 15 THEN 15
      WHEN j.pt > 60 THEN 60
      ELSE j.pt
    END - 0.225) / 0.45 AS BIGINT) * 0.45 + 0.225
ORDER BY x;
