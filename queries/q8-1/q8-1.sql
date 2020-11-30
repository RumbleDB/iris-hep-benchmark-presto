-- Remove any tables that might have been left from executing this query in the past
DROP TABLE IF EXISTS memory.cern.uniform_structure_leptons; 
DROP TABLE IF EXISTS memory.cern.lepton_pairs; 
DROP TABLE IF EXISTS memory.cern.processed_pairs; 
DROP TABLE IF EXISTS memory.cern.other_max_pt; 


-- Make the structure of Electrons and Muons uniform, and then union their arrays
CREATE TABLE memory.cern.uniform_structure_leptons AS
SELECT 
	event,
	MET_pt,
	MET_phi,
	array_union(
		transform(
			COALESCE(Muons, ARRAY []),
			x -> CAST( ROW(x.pt, x.eta, x.phi, x.mass, x.charge, 'm') AS ROW( pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE, charge INTEGER, type CHAR ) )
		),
		transform(
			COALESCE(Electrons, ARRAY []),
			x -> CAST( ROW(x.pt, x.eta, x.phi, x.mass, x.charge, 'e') AS ROW( pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE, charge INTEGER, type CHAR ) )
		)
	) AS Leptons
FROM memory.cern.view
WHERE nMuon + nElectron > 2;


-- Create the Lepton pairs, transform the leptons using PtEtaPhiM2PxPyPzE and then sum the transformed leptons
CREATE TABLE memory.cern.lepton_pairs AS
SELECT 
	*,
	CAST(
		ROW(
			pt1 * cos(phi1) + pt2 * cos(phi2),
			pt1 * sin(phi1) + pt2 * sin(phi2),
			pt1 * ( ( exp(eta1) - exp(-eta1) ) / 2.0 ) + pt2 * ( ( exp(eta2) - exp(-eta2) ) / 2.0 ), 
			pt1 * cosh(eta1) * pt1 * cosh(eta1) * pt1 + mass1 * mass1 + pt2 * cosh(eta2) * pt2 * cosh(eta2) * pt2 + mass2 * mass2
		) AS
		ROW (x DOUBLE, y DOUBLE, z DOUBLE, e DOUBLE)
	) AS l,
	idx1 AS l1_idx,
	idx2 AS l2_idx
FROM memory.cern.uniform_structure_leptons
CROSS JOIN UNNEST(Leptons) WITH ORDINALITY AS l1 (pt1, eta1, phi1, mass1, charge1, type1, idx1)
CROSS JOIN UNNEST(Leptons) WITH ORDINALITY AS l2 (pt2, eta2, phi2, mass2, charge2, type2, idx2)
WHERE idx1 < idx2 AND type1 = type2 AND charge1 != charge2;


-- Apply the PtEtaPhiM2PxPyPzE transformation on the particle pairs, then retrieve the one with the mass closest to 91.2 for each event
CREATE TABLE memory.cern.processed_pairs AS
SELECT 
	event,
	min_by(
		ROW(
			l1_idx,
			l2_idx,
			Leptons,
			MET_pt,
			MET_phi
		),
		abs(91.2 - sqrt(l.e * l.e - l.x * l.x - l.y * l.y - l.z * l.z))
	) AS system
FROM memory.cern.lepton_pairs
GROUP BY event;


-- For each event get the max pt of the other leptons
CREATE TABLE memory.cern.other_max_pt AS
SELECT event, max_by(2 * system[4] * pt * (1.0 - cos((system[5]- phi + pi()) % (2 * pi()) - pi())), pt) AS pt
FROM memory.cern.processed_pairs
CROSS JOIN UNNEST(system[3]) WITH ORDINALITY AS l (pt, eta, phi, mass, charge, type, idx)
WHERE idx != system[1] AND idx != system[2]
GROUP BY event;


-- Compute the histogram
SELECT
  CAST((
    CASE
      WHEN pt < 15 THEN 15
      WHEN pt > 250 THEN 250
      ELSE pt
    END - 1.175) / 2.35 AS BIGINT) * 2.35 + 1.175 AS x,
  COUNT(*) AS y
  FROM memory.cern.other_max_pt
  GROUP BY CAST((
    CASE
      WHEN pt < 15 THEN 15
      WHEN pt > 250 THEN 250
      ELSE pt
    END - 1.175) / 2.35 AS BIGINT) * 2.35 + 1.175
  ORDER BY x;
