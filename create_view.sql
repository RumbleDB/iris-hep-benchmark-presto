CREATE OR REPLACE VIEW memory.cern.view AS
SELECT *,
  ARRAY[(
    SELECT CAST(
      ROW(
        Jet_pt[i], 
        Jet_eta[i], 
        Jet_phi[i], 
        Jet_mass[i], 
        Jet_puId[i], 
        Jet_btag[i]
      ) 
      AS ROW(
        pt DOUBLE, 
        eta DOUBLE, 
        phi DOUBLE, 
        mass DOUBLE, 
        puId BOOLEAN, 
        btag DOUBLE)
      )
    FROM UNNEST(Jet_pt) WITH ORDINALITY AS t (n, i)
  )] AS Jet,
  ARRAY[(
    SELECT CAST(
      ROW(
        Muon_pt[i], 
        Muon_eta[i], 
        Muon_phi[i], 
        Muon_mass[i], 
        Muon_charge[i], 
        Muon_pfRelIso03_all[i],
        Muon_pfRelIso04_all[i], 
        Muon_tightId[i], 
        Muon_softId[i], 
        Muon_dxy[i], 
        Muon_dxyErr[i], 
        Muon_dz[i],
        Muon_dzErr[i], 
        Muon_jetIdx[i], 
        Muon_genPartIdx[i]
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
        genPartIdx INTEGER)
      )    
    FROM UNNEST(Muon_pt) WITH ORDINALITY AS t (n, i)
  )] AS Muon,
  ARRAY[(
    SELECT CAST(
      ROW(
        Electron_pt[i], 
        Electron_eta[i], 
        Electron_phi[i],
        Electron_mass[i],
        Electron_charge[i],
        Electron_pfRelIso03_all[i],
        Electron_dxy[i],
        Electron_dxyErr[i],
        Electron_dz[i],
        Electron_dzErr[i],
        Electron_cutBasedId[i],
        Electron_pfId[i],
        Electron_jetIdx[i],
        Electron_genPartIdx[i]
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
        genPartIdx INTEGER)
      )
    FROM UNNEST(Electron_pt) WITH ORDINALITY AS t (n, i)
  )] AS Electron
FROM memory.cern.run2012b_singlemu_small;