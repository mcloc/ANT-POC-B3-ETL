

DROP TABLE B3Log.ativos_maior_liquidez;
DROP INDEX B3Log.b3signallogger_relogio_idx1 CASCADE;
DROP INDEX B3Log.ativos_maior_liquidez_pk CASCADE;
DROP INDEX B3Log.b3historical_idx CASCADE;
DROP INDEX B3Log.b3signallogger_vencimento_idx CASCADE;
DROP INDEX B3Log.b3historical_pk CASCADE;

CREATE INDEX b3signallogger_pk
 ON B3Log.B3SignalLogger
 ( id ASC );

CREATE INDEX b3historical_pk
 ON B3Log.B3Historical
 ( id );

CREATE INDEX intellect_processment_routines_pk
 ON Intellect.processment_routines
 ( id );

 CREATE INDEX b3historical_bova11_idx 
 ON B3Log.B3Historical 
 (codneg)
 WHERE (codneg = 'BOVA11');

 CREATE INDEX b3historical_bova11_idx2
 ON B3Log.B3Historical 
 (TOTNEG)
 WHERE (codneg = 'BOVA11');

 
 CREATE INDEX b3historical_vale3_idx 
 ON B3Log.B3Historical 
 (codneg)
 WHERE (codneg = 'VALE3');

 CREATE INDEX b3historical_vale3_idx2
 ON B3Log.B3Historical 
 (TOTNEG)
 WHERE (codneg = 'VALE31');
 
-- CLUSTER b3historical_bova11_idx ON B3Log.B3Historical;   -- CAN NOT CLUSTER PARTIAL INDEX

CREATE INDEX b3historical_totneg_idx
 ON B3Log.B3Historical
 ( TOTNEG DESC );
CLUSTER b3historical_totneg_idx ON B3Historical;

REINDEX TABLE B3Log.B3SignalLogger

--REINDEX INDEX b3signallogger_relogio_idx1
CREATE INDEX b3signallogger_relogio_idx1
 ON B3Log.B3SignalLogger USING BTREE
 ( relogio DESC );
  CLUSTER b3signallogger_relogio_idx1 ON B3Log.B3SignalLogger;

--REINDEX INDEX b3historical_voltot_idx
CREATE INDEX b3historical_voltot_idx
 ON B3Log.B3Historical USING BTREE
 ( VOLTOT DESC );
 CLUSTER b3historical_totneg_idx ON B3Historical;

--REINDEX INDEX b3historical_voltot_idx
CREATE INDEX b3signallogger_asset_idx
 ON B3Log.B3SignalLogger USING BTREE
 ( asset ASC );
CLUSTER b3signallogger_asset_idx ON B3Log.B3SignalLogger;

CREATE INDEX b3signallogger_asset_strike_vencimento_idx
 ON B3Log.B3SignalLogger USING BTREE
 ( asset, strike, vencimento DESC );

CLUSTER b3signallogger_asset_strike_vencimento_idx ON B3SignalLogger;

CREATE INDEX b3signallogger_asset_vencimemnto_idx
 ON B3Log.B3SignalLogger USING BTREE
 ( asset, vencimento DESC );

CLUSTER b3signallogger_asset_vencimemnto_idx ON B3SignalLogger;

CREATE INDEX b3historical_codbdi_idx
 ON B3Log.B3Historical USING BTREE
 ( CODBDI );

CLUSTER b3historical_codbdi_idx ON B3Historical;

CREATE INDEX b3historical_codneg_idx
 ON B3Log.B3Historical USING BTREE
 ( CODNEG ASC );

CLUSTER b3historical_codneg_idx ON B3Historical;

CREATE INDEX b3historical_codneg_datvenc_idx
 ON B3Log.B3Historical USING BTREE
 ( CODNEG ASC, DATVEN DESC );

CLUSTER b3historical_codneg_datvenc_idx ON B3Historical;

CREATE INDEX b3historical_data_pregao_idx
 ON B3Log.B3Historical USING BTREE
 ( DATA_PREGAO DESC );

CLUSTER b3historical_data_pregao_idx ON B3Historical;

CREATE INDEX b3historical_especi_idx
 ON B3Log.B3Historical USING BTREE
 ( ESPECI );

CLUSTER b3historical_especi_idx ON B3Historical;

CREATE INDEX b3historical_nomeres_idx
 ON B3Log.B3Historical USING BTREE
 ( NOMRES ASC );

CLUSTER b3historical_nomeres_idx ON B3Historical;

CREATE INDEX b3signallogger_strike_vecimento_idx
 ON B3Log.B3SignalLogger USING BTREE
 ( strike DESC, vencimento DESC );

CLUSTER b3signallogger_strike_vecimento_idx ON B3SignalLogger;

CREATE INDEX b3signallogger_strike_idx
 ON B3Log.B3SignalLogger USING BTREE
 ( strike );

CLUSTER b3signallogger_strike_idx ON B3SignalLogger;

CREATE INDEX b3historical_bova11_idx
 ON B3Log.B3Historical USING BTREE
 ( TOTNEG );

CLUSTER b3historical_bova11_idx ON B3Historical;

CREATE INDEX b3historical_tpmerc_idx
 ON B3Log.B3Historical USING BTREE
 ( TPMERC ASC );

CLUSTER b3historical_tpmerc_idx ON B3Historical;

CREATE INDEX b3signallogger_vencimento_idx
 ON B3Log.B3SignalLogger USING BTREE
 ( vencimento ASC );

CLUSTER b3signallogger_vencimento_idx ON B3SignalLogger;

CREATE INDEX b3historical_bova11_idx
 ON B3Log.B3Historical USING BTREE
 ( codneg = 'BOVA11' ASC );
CLUSTER b3historical_bova11_idx ON B3Log.B3Historical;




CREATE INDEX b3ativo_opcao_idx
 ON B3Log.B3AitvosOpcoes USING BTREE
 ( ativo ASC, opcao_ativo ASC );

CLUSTER b3ativo_opcao_idx ON B3Log.B3AitvosOpcoes;


