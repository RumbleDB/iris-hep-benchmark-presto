-- UNNEST the Jets and filter out those with pt < 30
WITH unnested_jets AS (
  SELECT
    *,
    CAST( ROW( j.pt, j.eta, j.phi, j.mass, j.idx ) AS ROW( pt REAL, eta REAL, phi REAL, mass REAL, idx INTEGER ) ) AS j
  FROM {input_table}
  CROSS JOIN UNNEST(Jet) WITH ORDINALITY AS j (pt, eta, phi, mass, puId, btag, idx)
  WHERE j.pt > 30
),

-- Create the tables which hold the jet-other_particle pairs
filtered_particles AS (
  SELECT
    event,
    j,
    COALESCE(
      cardinality(filter(Electron, x -> x.pt > 10 AND sqrt( (j.eta - x.eta) * (j.eta - x.eta) + pow( (j.phi - x.phi + pi()) % (2 * pi()) - pi(), 2) ) < 40) ),
      0
    ) AS filtered_electron_count,
    COALESCE(
      cardinality(filter(Muon, x -> x.pt > 10 AND sqrt( (j.eta - x.eta) * (j.eta - x.eta) + pow( (j.phi - x.phi + pi()) % (2 * pi()) - pi(), 2) ) < 40) ),
      0
    ) AS filtered_muon_count
  FROM unnested_jets
),


-- Compute the per event jet.pt sums for the remaining jets
pt_sums AS (
  SELECT event, SUM(j.pt) AS pt_sum
  FROM filtered_particles
  WHERE filtered_electron_count = 0 AND filtered_muon_count = 0
  GROUP BY event
)


-- Compute the histogram
SELECT
  mysql.default.HistogramBin(pt_sum, 15, 200, 100) AS x,
  COUNT(*) AS y
FROM pt_sums
GROUP BY mysql.default.HistogramBin(pt_sum, 15, 200, 100)
ORDER BY x;
