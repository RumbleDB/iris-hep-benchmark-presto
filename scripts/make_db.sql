-- Create a schema which will hold our database
CREATE SCHEMA IF NOT EXISTS memory.cern;

-- Drop the table if it already exists
DROP TABLE IF EXISTS memory.cern.Run2012B_SingleMu_small;

-- Creates the `Run2012B_SingleMu_small` table
CREATE TABLE memory.cern.Run2012B_SingleMu_small (
    run                     INTEGER,
    luminosityBlock         BIGINT,
    event                   BIGINT,
    HLT_IsoMu24_eta2p1      BOOLEAN,
    HLT_IsoMu24             BOOLEAN,
    HLT_IsoMu17_eta2p1_LooseIsoPFTau20 BOOLEAN,
    PV_npvs                 INTEGER,
    PV_x                    REAL,
    PV_y                    REAL,
    PV_z                    REAL,
    nMuon                   BIGINT,
    Muon_pt                 ARRAY(REAL),
    Muon_eta                ARRAY(REAL),
    Muon_phi                ARRAY(REAL),
    Muon_mass               ARRAY(REAL),
    Muon_charge             ARRAY(INTEGER),
    Muon_pfRelIso03_all     ARRAY(REAL),
    Muon_pfRelIso04_all     ARRAY(REAL),
    Muon_tightId            ARRAY(BOOLEAN),
    Muon_softId             ARRAY(BOOLEAN),
    Muon_dxy                ARRAY(REAL),
    Muon_dxyErr             ARRAY(REAL),
    Muon_dz                 ARRAY(REAL),
    Muon_dzErr              ARRAY(REAL),
    Muon_jetIdx             ARRAY(INTEGER),
    Muon_genPartIdx         ARRAY(INTEGER),
    nElectron               BIGINT,
    Electron_pt             ARRAY(REAL),
    Electron_eta            ARRAY(REAL),
    Electron_phi            ARRAY(REAL),
    Electron_mass           ARRAY(REAL),
    Electron_charge         ARRAY(INTEGER),
    Electron_pfRelIso03_all ARRAY(REAL),
    Electron_dxy            ARRAY(REAL),
    Electron_dxyErr         ARRAY(REAL),
    Electron_dz             ARRAY(REAL),
    Electron_dzErr          ARRAY(REAL),
    Electron_cutBasedId     ARRAY(BOOLEAN),
    Electron_pfId           ARRAY(BOOLEAN),
    Electron_jetIdx         ARRAY(INTEGER),
    Electron_genPartIdx     ARRAY(INTEGER),
    nTau                    BIGINT,
    Tau_pt                  ARRAY(REAL),
    Tau_eta                 ARRAY(REAL),
    Tau_phi                 ARRAY(REAL),
    Tau_mass                ARRAY(REAL),
    Tau_charge              ARRAY(INTEGER),
    Tau_decayMode           ARRAY(INTEGER),
    Tau_relIso_all          ARRAY(REAL),
    Tau_jetIdx              ARRAY(INTEGER),
    Tau_genPartIdx          ARRAY(INTEGER),
    Tau_idDecayMode         ARRAY(BOOLEAN),
    Tau_idIsoRaw            ARRAY(REAL),
    Tau_idIsoVLoose         ARRAY(BOOLEAN),
    Tau_idIsoLoose          ARRAY(BOOLEAN),
    Tau_idIsoMedium         ARRAY(BOOLEAN),
    Tau_idIsoTight          ARRAY(BOOLEAN),
    Tau_idAntiEleLoose      ARRAY(BOOLEAN),
    Tau_idAntiEleMedium     ARRAY(BOOLEAN),
    Tau_idAntiEleTight      ARRAY(BOOLEAN),
    Tau_idAntiMuLoose       ARRAY(BOOLEAN),
    Tau_idAntiMuMedium      (BOOLEAN),
    Tau_idAntiMuTight       ARRAY(BOOLEAN),
    nPhoton                 BIGINT,
    Photon_pt               ARRAY(REAL),
    Photon_eta              ARRAY(REAL),
    Photon_phi              ARRAY(REAL),
    Photon_mass             ARRAY(REAL),
    Photon_charge           ARRAY(INTEGER),
    Photon_pfRelIso03_all   ARRAY(REAL),
    Photon_jetIdx           ARRAY(INTEGER),
    Photon_genPartIdx       ARRAY(INTEGER),
    MET_pt                  REAL,
    MET_phi                 REAL,
    MET_sumet               REAL,
    MET_significance        REAL,
    MET_CovXX               REAL,
    MET_CovXY               REAL,
    MET_CovYY               REAL,
    nJet                    BIGINT,
    Jet_pt                  ARRAY(REAL),
    Jet_eta                 ARRAY(REAL),
    Jet_phi                 ARRAY(REAL),
    Jet_mass                ARRAY(REAL),
    Jet_puId                ARRAY(BOOLEAN),
    Jet_btag                ARRAY(REAL)
);