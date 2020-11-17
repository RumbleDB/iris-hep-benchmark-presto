DROP TABLE IF EXISTS jet_table;
DROP TABLE IF EXISTS electron_table;
DROP TABLE IF EXISTS muon_table;

CREATE TABLE IF NOT EXISTS jet_table AS
SELECT event, 
  array_agg(
    CAST(
      ROW(
        pt, 
        eta, 
        phi,
        mass, 
        puId, 
        btag
      ) 
      AS ROW(
        pt DOUBLE, 
        eta DOUBLE, 
        phi DOUBLE, 
        mass DOUBLE, 
        puId BOOLEAN, 
        btag DOUBLE
      )
    )
  ) AS Jet
FROM memory.cern.run2012b_singlemu_small
CROSS JOIN UNNEST(Jet_pt, Jet_eta, Jet_phi, Jet_mass, Jet_puId, Jet_btag) AS t (pt, eta, phi, mass, puId, btag)
GROUP BY event;

CREATE TABLE IF NOT EXISTS electron_table AS
SELECT event,
  array_agg(
    CAST(
      ROW(
        pt, 
        eta, 
        phi, 
        mass, 
        charge, 
        pfRelIso03_all, 
        dxy, 
        dxyErr, 
        dz, 
        dzErr, 
        cutBasedId, 
        pfId, 
        jetIdx, 
        genPartIdx
      )
      AS ROW(
        pt DOUBLE, 
        eta DOUBLE,
        phi DOUBLE, 
        mass DOUBLE, 
        charge INTEGER, 
        pfRelIso03_all DOUBLE, 
        dxy DOUBLE, 
        dxyErr DOUBLE, 
        dz DOUBLE, 
        dzErr DOUBLE, 
        cutBasedId BOOLEAN, 
        pfId BOOLEAN, 
        jetIdx INTEGER, 
        genPartIdx INTEGER
      )
    )
  ) AS Electron
FROM memory.cern.run2012b_singlemu_small
CROSS JOIN UNNEST(Electron_pt, Electron_eta, Electron_phi, Electron_mass, Electron_charge, Electron_pfRelIso03_all,
        Electron_dxy, Electron_dxyErr,  Electron_dz, Electron_dzErr, Electron_cutBasedId, Electron_pfId, Electron_jetIdx,
        Electron_genPartIdx)
      AS t (pt, eta, phi, mass, charge, pfRelIso03_all, dxy, dxyErr, dz, dzErr, cutBasedId, pfId, jetIdx, genPartIdx)
GROUP BY event;

CREATE TABLE IF NOT EXISTS muon_table AS
SELECT event,
  array_agg(
    CAST(
      ROW(
        pt, 
        eta, 
        phi, 
        mass, 
        charge, 
        pfRelIso03_all, 
        pfRelIso04_all, 
        tightId, 
        softId, 
        dxy, 
        dxyErr,
        dz, 
        dzErr, 
        jetIdx, 
        genPartIdx
      )
      AS ROW(
        pt DOUBLE, 
        eta DOUBLE, 
        phi DOUBLE, 
        mass DOUBLE, 
        charge INTEGER, 
        pfRelIso03_all DOUBLE, 
        pfRelIso04_all DOUBLE, 
        tightId BOOLEAN, 
        softId BOOLEAN, 
        dxy DOUBLE, 
        dxyErr DOUBLE,
        dz DOUBLE, 
        dzErr DOUBLE, 
        jetIdx INTEGER, 
        genPartIdx INTEGER

      )
    )
  ) AS Muon
FROM memory.cern.run2012b_singlemu_small
CROSS JOIN UNNEST(Muon_pt, Muon_eta, Muon_phi, Muon_mass, Muon_charge, Muon_pfRelIso03_all, Muon_pfRelIso04_all,  
        Muon_tightId, Muon_softId, Muon_dxy, Muon_dxyErr, Muon_dz, Muon_dzErr, Muon_jetIdx, Muon_genPartIdx)
      AS t (pt, eta, phi, mass, charge, pfRelIso03_all, pfRelIso04_all, tightId, softId, dxy, dxyErr, dz, dzErr, 
        jetIdx, genPartIdx)
GROUP BY event;

CREATE OR REPLACE VIEW memory.cern.view AS
SELECT 
  main.event, 
  main.run,
  main.luminosityBlock,
  main.HLT_IsoMu24_eta2p,
  main.HLT_IsoMu24,
  main.HLT_IsoMu17_eta2p1_LooseIsoPFTau20,
  main.PV_npvs,
  main.PV_x,
  main.PV_y,
  main.PV_z,
  main.MET_pt,
  main.MET_phi,
  main.MET_sumet,
  main.MET_significance,
  main.MET_CovXX,
  main.MET_CovXY,
  main.MET_CovYY,
  main.nTau,
  main.nPhoton,
  main.nElectron,
  Electron AS Electrons,
  main.nMuon, 
  Muon AS Muons,
  main.nJet,
  Jet AS Jets
FROM memory.cern.run2012b_singlemu_small AS main
FULL JOIN jet_table AS j on main.event = j.event
FULL JOIN electron_table AS e ON main.event = e.event
FULL JOIN muon_table AS m ON main.event = m.event;