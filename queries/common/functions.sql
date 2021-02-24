CREATE OR REPLACE FUNCTION mysql.default.HistogramBin(
    value REAL, lo REAL, hi REAL, num_bins INTEGER)
RETURNS REAL DETERMINISTIC RETURNS NULL ON NULL INPUT
RETURN
  CAST((
    CASE
      WHEN value < lo THEN lo - ((hi - lo) / num_bins) / 4
      WHEN value > hi THEN hi + ((hi - lo) / num_bins) / 4
      ELSE value
    END - ((hi - lo) / num_bins) / 2) / ((hi - lo) / num_bins)
    AS INTEGER) *
      ((hi - lo) / num_bins) + ((hi - lo) / num_bins) / 2;
