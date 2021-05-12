
-- **************   ZERAR TABELAS E INDICES B3SignalLogger ***************************
-- APAGAR REGISTROS CARREGADOS ( C U I D A D O )
--DELETE FROM B3Log.B3SignalLogger ;
TRUNCATE B3Log.B3SignalLogger RESTART IDENTITY;
--ZERAR SEQUENCE AUTO INCREMENT
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE IF EXISTS B3Log.b3signallogger_id;
CREATE SEQUENCE B3Log.b3signallogger_id;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN id SET DEFAULT nextval('B3Log.b3signallogger_id');
ALTER SEQUENCE B3Log.b3signallogger_id OWNED BY B3Log.B3SignalLogger.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT

-- **************   ZERAR TABELAS E INDICES B3SHistorical ***************************
-- APAGAR REGISTROS B3Historico ( C U I D A D O )
DELETE FROM B3Log.b3Historical ;
--ZERAR SEQUENCE AUTO INCREMENT B3Historico
ALTER TABLE ONLY B3Log.b3Historical ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE IF EXISTS B3Log.b3historical_id;
CREATE SEQUENCE B3Log.b3historical_id;
ALTER TABLE ONLY B3Log.b3Historical ALTER COLUMN id SET DEFAULT nextval('B3Log.b3historical_id');
ALTER SEQUENCE B3Log.b3historical_id OWNED BY B3Log.b3Historical.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT


-- **************   ZERAR TABELAS E INDICES B3SignalLoggerLevel1 ***************************
-- APAGAR REGISTROS B3Historico ( C U I D A D O )
DELETE FROM B3Log.B3SignalLoggerLevel1 ;
--ZERAR SEQUENCE AUTO INCREMENT B3Historico
ALTER TABLE ONLY B3Log.B3SignalLoggerLevel1 ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE IF EXISTS B3Log.b3signalloggerLevel1_id;
CREATE SEQUENCE B3Log.b3signalloggerLevel1_id;
ALTER TABLE ONLY B3Log.B3SignalLoggerLevel1 ALTER COLUMN id SET DEFAULT nextval('B3Log.b3signalloggerLevel1_id');
ALTER SEQUENCE B3Log.b3signalloggerLevel1_id OWNED BY B3Log.B3SignalLoggerLevel1.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT





-- **************   ZERAR TABELAS E INDICES B3SignalLoggerLevel2Negocios ***************************
-- APAGAR REGISTROS B3Historico ( C U I D A D O )
DELETE FROM B3Log.B3SignalLoggerLevel2Negocios ;
--ZERAR SEQUENCE AUTO INCREMENT B3SignalLoggerLevel2Negocios
ALTER TABLE ONLY B3Log.B3SignalLoggerLevel2Negocios ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE IF EXISTS B3Log.B3SignalLoggerLevel2Negocios_id;
CREATE SEQUENCE B3Log.B3SignalLoggerLevel2Negocios_id;
ALTER TABLE ONLY B3Log.B3SignalLoggerLevel2Negocios ALTER COLUMN id SET DEFAULT nextval('B3Log.B3SignalLoggerLevel2Negocios_id');
ALTER SEQUENCE B3Log.B3SignalLoggerLevel2Negocios_id OWNED BY B3Log.B3SignalLoggerLevel2Negocios.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT








-- ------------------------------------------- NEW DDL ver 0.2 Intellect/ANTOption -------------------------------------------




-- **************   ZERAR TABELAS E INDICES Intellect.processment_rotines ***************************
-- APAGAR REGISTROS Intellect.processment_rotines ( C U I D A D O )
DELETE FROM Intellect.processment_rotines ;
--ZERAR SEQUENCE AUTO INCREMENT Intellect.processment_rotines
ALTER TABLE ONLY Intellect.processment_rotines ALTER COLUMN id SET DEFAULT 0;
DROP  SEQUENCE  IF EXISTS Intellect.seq_processment_rotines_id;
CREATE SEQUENCE Intellect.seq_processment_rotines_id;
ALTER TABLE ONLY Intellect.processment_rotines ALTER COLUMN id SET DEFAULT nextval('Intellect.seq_processment_rotines_id');
ALTER SEQUENCE Intellect.seq_processment_rotines_id OWNED BY Intellect.processment_rotines.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT


-- **************   ZERAR TABELAS E INDICES Intellect.processment_execution ***************************
-- APAGAR REGISTROS Intellect.processment_execution ( C U I D A D O )
DELETE FROM Intellect.processment_execution ;
--ZERAR SEQUENCE AUTO INCREMENT Intellect.processment_execution
ALTER TABLE ONLY Intellect.processment_execution ALTER COLUMN id SET DEFAULT 0;
DROP  SEQUENCE  IF EXISTS Intellect.seq_processment_execution_id;
CREATE SEQUENCE Intellect.seq_processment_execution_id;
ALTER TABLE ONLY Intellect.processment_execution ALTER COLUMN id SET DEFAULT nextval('Intellect.seq_processment_execution_id');
ALTER SEQUENCE Intellect.seq_processment_execution_id OWNED BY Intellect.processment_execution.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT




-- **************   ZERAR TABELAS E INDICES Intellect.processment_execution_audit ***************************
-- APAGAR REGISTROS Intellect.processment_execution_audit ( C U I D A D O )
DELETE FROM Intellect.processment_execution_audit ;
--ZERAR SEQUENCE AUTO INCREMENT Intellect.processment_execution_audit
ALTER TABLE ONLY Intellect.processment_execution_audit ALTER COLUMN id SET DEFAULT 0;
DROP  SEQUENCE  IF EXISTS Intellect.seq_processment_execution_audit_id;
CREATE SEQUENCE Intellect.seq_processment_execution_audit_id;
ALTER TABLE ONLY Intellect.processment_execution_audit ALTER COLUMN id SET DEFAULT nextval('Intellect.seq_processment_execution_audit_id');
ALTER SEQUENCE Intellect.seq_processment_execution_audit_id OWNED BY Intellect.processment_execution_audit.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT



-- **************   ZERAR TABELAS E INDICES Intellect.processment_errors ***************************
-- APAGAR REGISTROS Intellect.processment_errors ( C U I D A D O )
DELETE FROM Intellect.processment_errors ;
--ZERAR SEQUENCE AUTO INCREMENT Intellect.processment_rotines
ALTER TABLE ONLY Intellect.processment_errors ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE IF EXISTS Intellect.seq_processment_errors_id;
CREATE SEQUENCE Intellect.seq_processment_errors_id;
ALTER TABLE ONLY Intellect.processment_errors ALTER COLUMN id SET DEFAULT nextval('Intellect.seq_processment_errors_id');
ALTER SEQUENCE Intellect.seq_processment_errors_id OWNED BY Intellect.processment_errors.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT



-- **************   ZERAR TABELAS E INDICES Intellect.processment_errors_log ***************************
-- APAGAR REGISTROS Intellect.processment_errors_log ( C U I D A D O )
DELETE FROM Intellect.processment_errors_log ;
--ZERAR SEQUENCE AUTO INCREMENT Intellect.processment_rotines
ALTER TABLE ONLY Intellect.processment_errors_log ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE IF EXISTS Intellect.seq_processment_errors_log_id;
CREATE SEQUENCE Intellect.seq_processment_errors_log_id;
ALTER TABLE ONLY Intellect.processment_errors_log ALTER COLUMN id SET DEFAULT nextval('Intellect.seq_processment_errors_log_id');
ALTER SEQUENCE Intellect.seq_processment_errors_log_id OWNED BY Intellect.processment_errors_log.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT



-- **************   ZERAR TABELAS E INDICES Intellect.csv_load_lot ***************************
-- APAGAR REGISTROS Intellect.csv_load_lot ( C U I D A D O )
DELETE FROM Intellect.csv_load_lot ;
--ZERAR SEQUENCE AUTO INCREMENT Intellect.processment_rotines
ALTER TABLE ONLY Intellect.csv_load_lot ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE IF EXISTS Intellect.seq_csv_load_lot_id;
CREATE SEQUENCE Intellect.seq_csv_load_lot_id;
ALTER TABLE ONLY Intellect.csv_load_lot ALTER COLUMN id SET DEFAULT nextval('Intellect.seq_csv_load_lot_id');
ALTER SEQUENCE Intellect.seq_csv_load_lot_id OWNED BY Intellect.csv_load_lot.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT



-- **************   ZERAR TABELAS E INDICES Intellect.csv_load_registry ***************************
-- APAGAR REGISTROS Intellect.csv_load_registry ( C U I D A D O )
DELETE FROM Intellect.csv_load_registry ;
--ZERAR SEQUENCE AUTO INCREMENT Intellect.processment_rotines
ALTER TABLE ONLY Intellect.csv_load_registry ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE IF EXISTS Intellect.seq_csv_load_registry_id;
CREATE SEQUENCE Intellect.seq_csv_load_registry_id;
ALTER TABLE ONLY Intellect.csv_load_registry ALTER COLUMN id SET DEFAULT nextval('Intellect.seq_csv_load_registry_id');
ALTER SEQUENCE Intellect.seq_csv_load_registry_id OWNED BY Intellect.csv_load_registry.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT




-- **************   ZERAR TABELAS E INDICES Intellect.hot_table ***************************
-- APAGAR REGISTROS Intellect.hot_table ( C U I D A D O )
DELETE FROM Intellect.hot_table ;
--ZERAR SEQUENCE AUTO INCREMENT Intellect.hot_table
ALTER TABLE ONLY Intellect.hot_table ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE IF EXISTS Intellect.seq_hot_table_id;
CREATE SEQUENCE Intellect.seq_hot_table_id;
ALTER TABLE ONLY Intellect.hot_table ALTER COLUMN id SET DEFAULT nextval('Intellect.seq_hot_table_id');
ALTER SEQUENCE Intellect.seq_hot_table_id OWNED BY Intellect.hot_table.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT

