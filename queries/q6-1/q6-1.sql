-- Remove any tables that might have been left from executing this query in the past
DROP TABLE IF EXISTS memory.cern.tri_jets; 
DROP TABLE IF EXISTS memory.cern.expanded_tri_jet; 
DROP TABLE IF EXISTS memory.cern.condensed_tri_jet;
DROP TABLE IF EXISTS memory.cern.computed_system;
DROP TABLE IF EXISTS memory.cern.singular_system;


-- Create the TriJet systems
CREATE TABLE memory.cern.tri_jets AS
SELECT 
	event,
	CAST( ROW( m1.pt, m1.eta, m1.phi, m1.mass ) AS ROW( pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE ) ) AS m1,
	CAST( ROW( m2.pt, m2.eta, m2.phi, m2.mass ) AS ROW( pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE ) ) AS m2,
	CAST( ROW( m3.pt, m3.eta, m3.phi, m3.mass ) AS ROW( pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE ) ) AS m3
FROM memory.cern.view
CROSS JOIN UNNEST(Jets) WITH ORDINALITY AS m1 (pt, eta, phi, mass, puId, btag, idx)
CROSS JOIN UNNEST(Jets) WITH ORDINALITY AS m2 (pt, eta, phi, mass, puId, btag, idx)
CROSS JOIN UNNEST(Jets) WITH ORDINALITY AS m3 (pt, eta, phi, mass, puId, btag, idx)
WHERE m1.idx < m2.idx AND m2.idx < m3.idx;


-- Compute the PtEtaPhiM2PxPyPzE for each particle
CREATE TABLE memory.cern.expanded_tri_jet AS
SELECT 
	event,
	CAST(
		ROW(
			m1.pt * cos(m1.phi),
			m1.pt * sin(m1.phi),
			m1.pt * ( ( exp(m1.eta) - exp(-m1.eta) ) / 2.0 ), 
			m1.pt * cosh(m1.eta) * m1.pt * cosh(m1.eta) * m1.pt + m1.mass * m1.mass
		) AS
		ROW (x DOUBLE, y DOUBLE, z DOUBLE, e DOUBLE)
	) AS m1,
	CAST(
		ROW(
			m2.pt * cos(m2.phi),
			m2.pt * sin(m2.phi),
			m2.pt * ( ( exp(m2.eta) - exp(-m2.eta) ) / 2.0 ), 
			m2.pt * cosh(m2.eta) * m2.pt * cosh(m2.eta) * m2.pt + m2.mass * m2.mass
		) AS
		ROW (x DOUBLE, y DOUBLE, z DOUBLE, e DOUBLE)
	) AS m2,
	CAST(
		ROW(
			m3.pt * cos(m3.phi),
			m3.pt * sin(m3.phi),
			m3.pt * ( ( exp(m3.eta) - exp(-m3.eta) ) / 2.0 ), 
			m3.pt * cosh(m3.eta) * m3.pt * cosh(m3.eta) * m3.pt + m3.mass * m3.mass
		) AS
		ROW (x DOUBLE, y DOUBLE, z DOUBLE, e DOUBLE)
	) AS m3
FROM memory.cern.tri_jets;

-- Compute the AddPxPyPzE3 for each TriJet system
CREATE TABLE memory.cern.condensed_tri_jet AS
SELECT 
	event,
	m1.x + m2.x + m3.x AS x,
	m1.y + m2.y + m3.y AS y,
	m1.z + m2.z + m3.z AS z,
	m1.e + m2.e + m3.e AS e,
	(m1.x + m2.x + m3.x) * (m1.x + m2.x + m3.x) AS x2,
	(m1.y + m2.y + m3.y) * (m1.y + m2.y + m3.y) AS y2,
	(m1.z + m2.z + m3.z) * (m1.z + m2.z + m3.z) AS z2,
	(m1.e + m2.e + m3.e) * (m1.e + m2.e + m3.e) AS e2
FROM memory.cern.expanded_tri_jet;


-- Compute the PxPyPzE2PtEtaPhiM
CREATE TABLE memory.cern.computed_system AS 
SELECT
	event,
	sqrt(x2 * y2) AS pt,
	ln( (z / sqrt(x2 * y2)) + sqrt((z / sqrt(x2 * y2)) * (z / sqrt(x2 * y2)) + 1.0)) AS eta,
	CASE
	    WHEN x = 0 AND y = 0 THEN 0.0
		ELSE atan2(y, x)
	END AS phi,
	sqrt(e2 - x2 - y2 - z2) AS mass
FROM memory.cern.condensed_tri_jet;


-- Find the system with the lowest mass
CREATE TABLE memory.cern.singular_system AS
SELECT event, min_by(CAST(ROW(pt, eta, phi, mass) AS ROW(pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE)), abs(172.5 - mass)) AS jet_system
FROM memory.cern.computed_system
GROUP BY event;


-- Compute the AddPxPyPzE3 and PxPyPzE2PtEtaPhiM for each TriJet system
-- CREATE TABLE memory.cern.condensed_tri_jet AS
-- SELECT 
-- 	event,
-- 	CAST(
-- 		ROW(
-- 			sqrt( (m1.x + m2.x + m3.x) * (m1.x + m2.x + m3.x) * (m1.y + m2.y + m3.y) * (m1.y + m2.y + m3.y) ),
-- 			ln( ((m1.z + m2.z + m3.z) / sqrt( (m1.x + m2.x + m3.x) * (m1.x + m2.x + m3.x) * (m1.y + m2.y + m3.y) * (m1.y + m2.y + m3.y) )) + sqrt( ((m1.z + m2.z + m3.z) / sqrt( (m1.x + m2.x + m3.x) * (m1.x + m2.x + m3.x) * (m1.y + m2.y + m3.y) * (m1.y + m2.y + m3.y) )) * ((m1.z + m2.z + m3.z) / sqrt( (m1.x + m2.x + m3.x) * (m1.x + m2.x + m3.x) * (m1.y + m2.y + m3.y) * (m1.y + m2.y + m3.y) )) + 1.0)),
-- 			CASE
-- 			    WHEN (m1.x + m2.x + m3.x) = 0 AND (m1.y + m2.y + m3.y) = 0 THEN 0.0
-- 			    ELSE atan2((m1.y + m2.y + m3.y), (m1.x + m2.x + m3.x))
-- 			END,
-- 			sqrt(
-- 					(m1.e + m2.e + m3.e) * (m1.e + m2.e + m3.e)
-- 					- (m1.x + m2.x + m3.x) * (m1.x + m2.x + m3.x)
-- 					- (m1.y + m2.y + m3.y) * (m1.y + m2.y + m3.y) 
-- 					- (m1.z + m2.z + m3.z) * (m1.z + m2.z + m3.z) 
-- 			)
-- 		) AS
-- 		ROW (pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE)
-- 	) AS jet_system
-- FROM memory.cern.expanded_tri_jet;


-- -- Find the system with the lowest mass
-- CREATE TABLE memory.cern.singular_system AS
-- SELECT event, min_by(jet_system, abs(172.5 - jet_system.mass)) AS jet_system
-- FROM memory.cern.condensed_tri_jet
-- GROUP BY event;

-- -- Generate the histogram -- Do we need to get the min pt in the trijet system or the pt of the trijet? 
SELECT
  CAST((
    CASE
      WHEN jet_system.pt < 15 THEN 15
      WHEN jet_system.pt > 40 THEN 40
      ELSE jet_system.pt
    END - 0.125) / 0.25 AS BIGINT) * 0.25 + 0.125 AS x,
  COUNT(*) AS y
  FROM memory.cern.singular_system
  GROUP BY CAST((
    CASE
      WHEN jet_system.pt < 15 THEN 15
      WHEN jet_system.pt > 40 THEN 40
      ELSE jet_system.pt
    END - 0.125) / 0.25 AS BIGINT) * 0.25 + 0.125
  ORDER BY x;

