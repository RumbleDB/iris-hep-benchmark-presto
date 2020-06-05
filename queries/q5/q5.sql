WITH temp AS (
	SELECT event, MET_sumet, COUNT(*)
	FROM memory.cern.view
	CROSS JOIN UNNEST(Muons) WITH ORDINALITY AS m1 (pt, eta, phi, mass, charge, pfRelIso03_all, 
		pfRelIso04_all, tightId, softId, dxy, dxyErr, dz, dzErr, jetIdx, genPartIdx, idx)
	CROSS JOIN UNNEST(Muons) WITH ORDINALITY AS m2 (pt, eta, phi, mass, charge, pfRelIso03_all, 
		pfRelIso04_all, tightId, softId, dxy, dxyErr, dz, dzErr, jetIdx, genPartIdx, idx)
	WHERE m1.idx < m2.idx AND m1.charge <> m2.charge  AND
	    SQRT(2 * m1.pt * m2.pt * (COSH(m1.eta - m2.eta) - COS(m1.phi - m2.phi))) BETWEEN 60 AND 100
	GROUP BY event, MET_sumet
	HAVING COUNT(*) > 0
)
SELECT
  CAST((
    CASE
      WHEN MET_sumet < 0 THEN 0
      WHEN MET_sumet > 2000 THEN 2000
      ELSE MET_sumet
    END - 10) / 20 AS BIGINT) * 20 + 10 AS x,
  COUNT(*) AS y
  FROM temp
  GROUP BY CAST((
    CASE
      WHEN MET_sumet < 0 THEN 0
      WHEN MET_sumet > 2000 THEN 2000
      ELSE MET_sumet
    END - 10) / 20 AS BIGINT) * 20 + 10
  ORDER BY x;