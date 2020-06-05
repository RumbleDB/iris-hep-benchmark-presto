SELECT
  CAST((
    CASE
      WHEN jet < 15 THEN 15
      WHEN jet > 60 THEN 60
      ELSE jet
    END - 0.375) / 0.45 AS BIGINT) * 0.45 + 0.375 AS x,
  COUNT(*) AS y
FROM memory.cern.Run2012B_SingleMu_small
CROSS JOIN UNNEST(Jet_pt) AS t (jet)
GROUP BY CAST((
    CASE
      WHEN jet < 15 THEN 15
      WHEN jet > 60 THEN 60
      ELSE jet
    END - 0.375) / 0.45 AS BIGINT) * 0.45 + 0.375
ORDER BY x;
