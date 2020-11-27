-- Remove any tables that might have been left from executing this query in the past
DROP TABLE IF EXISTS memory.cern.unnested_jets; 
DROP TABLE IF EXISTS memory.cern.filtered_particles;
DROP TABLE IF EXISTS memory.cern.pt_sums; 


-- UNNEST the Jets and filter out those with pt < 30
CREATE TABLE memory.cern.unnested_jets AS
SELECT 
	*,  
	CAST( ROW( j.pt, j.eta, j.phi, j.mass, j.idx ) AS ROW( pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE, idx INTEGER ) ) AS j
FROM memory.cern.view
CROSS JOIN UNNEST(Jets) WITH ORDINALITY AS j (pt, eta, phi, mass, puId, btag, idx)
WHERE j.pt > 30;


-- Create the tables which hold the jet-other_particle pairs
CREATE TABLE memory.cern.filtered_particles AS
SELECT 
	event, 
	j, 
	cardinality(filter(Electrons, x -> x.pt > 10 AND sqrt( (j.eta - x.eta) * (j.eta - x.eta) + pow( (j.phi - x.phi + pi()) % (2 * pi()) - pi(), 2) ) < 40)) AS filtered_electron_count,
	cardinality(filter(Muons, x -> x.pt > 10 AND sqrt( (j.eta - x.eta) * (j.eta - x.eta) + pow( (j.phi - x.phi + pi()) % (2 * pi()) - pi(), 2) ) < 40)) AS filtered_muon_count
FROM memory.cern.unnested_jets;


-- Compute the per event jet.pt sums for the remaining jets
CREATE TABLE memory.cern.pt_sums AS
SELECT event, SUM(j.pt) AS pt_sum
FROM memory.cern.filtered_particles
WHERE (filtered_electron_count = 0 OR filtered_electron_count = null) AND (filtered_muon_count = 0 OR filtered_muon_count = null)
GROUP BY event;

