
--CREATE TABLE B3SignalLogger
--DROP TABLE B3Log.B3SignalLogger cascade
-- nextval('b3signallogger_id') NOT NULL,
CREATE TABLE B3Log.B3SignalLogger (
                id BIGINT  NOT NULL,
                asset VARCHAR(11) NOT NULL,
                data DATE NOT NULL,
                hora TIME NOT NULL,
                ultimo NUMERIC(8,2) NOT NULL,
                strike NUMERIC(8,2) NOT NULL,
                negocios INTEGER NOT NULL,
                quantidade INTEGER NOT NULL,
                volume NUMERIC(18,2) NOT NULL,
                oferta_compra NUMERIC(8,2) NOT NULL,
                oferta_venda NUMERIC(8,2) NOT NULL,
                VOC INTEGER NOT NULL,
                VOV INTEGER NOT NULL,
                vencimento DATE NOT NULL,
                validade DATE NOT NULL,
                contratos_abertos BIGINT NOT NULL,
                estado_atual VARCHAR(80) NOT NULL,
                relogio TIMESTAMP NOT NULL,
                CONSTRAINT b3signallogger_pk PRIMARY KEY (id)
);


DROP TABLE B3Log.B3Historical;
CREATE TABLE B3Log.B3Historical (
                id BIGINT NOT NULL,
                TIPREG NUMERIC(2) NOT NULL,
                DATA_PREGAO DATE NOT NULL,
                TPMERC NUMERIC(3) NOT NULL,
                CODBDI CHAR(2) NOT NULL,
                PRAZOT CHAR(3) NOT NULL,
                CODNEG VARCHAR(12) NOT NULL,
                PREMAX NUMERIC(11,2) NOT NULL,
                PREABE NUMERIC(11,2) NOT NULL,
                PREMIN NUMERIC(11,2) NOT NULL,
                PTOEXE NUMERIC(13,6) NOT NULL,
                MODREF CHAR(4) NOT NULL,
                PREOFV NUMERIC(11,2) NOT NULL,
                PREULT NUMERIC(11,2) NOT NULL,
                DISMES NUMERIC(3) NOT NULL,
                CODISI VARCHAR(12) NOT NULL,
                PREMED NUMERIC(11,2) NOT NULL,
                FATCOT NUMERIC(7) NOT NULL,
                PREOFC NUMERIC(11,2) NOT NULL,
                INDOPC NUMERIC(1) NOT NULL,
                QUATOT VARCHAR(18) NOT NULL,
                VOLTOT NUMERIC(16,2) NOT NULL,
                PREEXE NUMERIC(11,2) NOT NULL,
                DATVEN NUMERIC(8) NOT NULL,
                TOTNEG NUMERIC(5) NOT NULL,
                NOMRES VARCHAR(12) NOT NULL,
                ESPECI VARCHAR(10) NOT NULL,
                CONSTRAINT b3historical_pk PRIMARY KEY (id)
);


DROP TABLE B3Log.B3SignalLoggerLevel1;

CREATE TABLE B3Log.B3SignalLoggerLevel1 (
		id BIGINT  NOT NULL,
		asset VARCHAR(11) NOT NULL,
		data DATE NOT NULL,
		hora TIME NOT NULL,
		ultimo NUMERIC(8,2) NOT NULL,
		strike NUMERIC(8,2) NOT NULL,
		valor_ativo NUMERIC(8,2) NOT NULL,
		valor_medio_negocios NUMERIC(8,2) NOT NULL,
		negocios INTEGER NOT NULL,
		quantidade INTEGER NOT NULL,
		volume NUMERIC(18,2) NOT NULL,
		oferta_compra NUMERIC(8,2) NOT NULL,
		oferta_venda NUMERIC(8,2) NOT NULL,
		log_devirada1semana NUMERIC(43,40) DEFAULT  NULL,
		preco_blackshoes1semanas NUMERIC(8,2) DEFAULT  NULL,
		log_devirada2semana NUMERIC(43,40) DEFAULT  NULL,
		preco_blackshoes2semanas NUMERIC(8,2) DEFAULT  NULL,
		log_devirada3semana NUMERIC(43,40) DEFAULT  NULL,
		preco_blackshoes3semanas NUMERIC(8,2) DEFAULT  NULL,
		log_devirada3meses NUMERIC(43,40) DEFAULT  NULL,
		preco_blackshoes3meses NUMERIC(8,2) DEFAULT  NULL,
		log_devirada6meses NUMERIC(43,40) DEFAULT  NULL,
		preco_blackshoes6meses NUMERIC(8,2) DEFAULT  NULL,
		log_devirada12meses NUMERIC(43,40) DEFAULT  NULL,
		preco_blackshoes12meses NUMERIC(8,2) DEFAULT  NULL,
		log_devirada18meses NUMERIC(43,40) DEFAULT  NULL,
		preco_blackshoes18meses NUMERIC(8,2) DEFAULT  NULL,
		log_devirada24meses NUMERIC(43,40) DEFAULT  NULL,
		preco_blackshoes24meses NUMERIC(8,2) DEFAULT  NULL,
		log_devirada36meses NUMERIC(43,40) DEFAULT  NULL,
		preco_blackshoes36meses NUMERIC(8,2) DEFAULT  NULL,
		VOC INTEGER NOT NULL,
		VOV INTEGER NOT NULL,
		vencimento DATE NOT NULL,
		validade DATE NOT NULL,
		contratos_abertos BIGINT NOT NULL,
		estado_atual VARCHAR(80) NOT NULL,
		relogio TIMESTAMP NOT NULL,
CONSTRAINT b3signalloggerlevel1_pk PRIMARY KEY (id)
);


DROP TABLE B3Log.B3AtivosOpcoes;

CREATE TABLE B3Log.B3AtivosOpcoes (
                ativo VARCHAR(11) NOT NULL,
                substr_opcao_ativo VARCHAR(11) NOT NULL,
                opcao_ativo VARCHAR(11) NULL,
                CONSTRAINT b3aitvosopcoes_pk PRIMARY KEY (ativo,opcao_ativo)
);

select * from B3Log.B3AtivosOpcoes 


DROP TABLE   B3Log.B3SignalLoggerLevel2Negocios;

CREATE TABLE B3Log.B3SignalLoggerLevel2Negocios (
		id BIGINT  NOT NULL,
		asset VARCHAR(11) NOT NULL,
		data DATE NOT NULL,
		hora TIME NOT NULL,
		ultimo NUMERIC(8,2) NOT NULL,
		strike NUMERIC(8,2) NOT NULL,
		valor_ativo NUMERIC(8,2) NOT NULL,
		valor_medio_negocios NUMERIC(8,2) NOT NULL,
		negocios INTEGER NOT NULL,
		quantidade INTEGER NOT NULL,
		volume NUMERIC(18,2) NOT NULL,
		media_dia_negocios INTEGER DEFAULT  NULL,
		media_dia_quantidade INTEGER DEFAULT  NULL,
		media_dia_volume NUMERIC(18,2) DEFAULT NULL,
		oferta_compra NUMERIC(8,2) NOT NULL,
		oferta_venda NUMERIC(8,2) NOT NULL,
		VOC INTEGER NOT NULL,
		VOV INTEGER NOT NULL,
		vencimento DATE NOT NULL,
		validade DATE NOT NULL,
		contratos_abertos BIGINT NOT NULL,
		estado_atual VARCHAR(80) NOT NULL,
		relogio_last_change TIMESTAMP NOT NULL,
CONSTRAINT b3signalloggerlevel2negocios_idx PRIMARY KEY (id)
);



-- ------------------------------------------- NEW DDL ver 0.2 Intellect/ANTOption -------------------------------------------



--DROP TABLE IF EXISTS Intellect.processment_rotines;
CREATE TABLE Intellect.processment_rotines (
		id BIGINT  NOT NULL,
		name VARCHAR(70) NOT NULL,
		description VARCHAR(255) NOT NULL,
		processment_seq NUMERIC(3,0) NOT NULL,
		processment_group VARCHAR(70) NOT NULL,
		processment_mode VARCHAR(70) NOT NULL,
		new_thread bool NOT NULL,
		thread_name VARCHAR(80) NULL,
		active_status bool NOT NULL,
		created_at TIMESTAMP NOT NULL,
CONSTRAINT pk_intellect_processment_rotines_idx PRIMARY KEY (id)
);




--DROP TABLE IF EXISTS  Intellect.processment_execution 
CREATE TABLE Intellect.processment_execution (
		id BIGINT  NOT NULL,
		processment_mode VARCHAR(70) NOT NULL,
		status NUMERIC(2,0) NOT NULL, -- 0 executing, 1 executed success, -1 error
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
CONSTRAINT pk_intellect_processment_execution_idx PRIMARY KEY (id)
);


-- TODO: aqui vai uma trigger no DB pra fazer insert no audit toda vez que tiver insert/update 
-- na processment_execution
--DROP TABLE IF EXISTS  Intellect.processment_execution_audit;
CREATE TABLE Intellect.processment_execution_audit (
		id BIGINT  NOT NULL,
		name VARCHAR(70) NOT NULL,
		description VARCHAR(255) NOT NULL,
		processment_id BIGINT NOT NULL,
		processment_seq NUMERIC(3,0) NOT NULL,
		processment_group VARCHAR(70) NOT NULL,
		processment_mode VARCHAR(70) NOT NULL,
		new_thread bool NOT NULL,
		status NUMERIC(2,0) NOT NULL, -- 0 executing, 1 executed success, -1 error
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
CONSTRAINT pk_intellect_processment_execution_audit_idx PRIMARY KEY (id)
);


--DROP TABLE IF EXISTS  Intellect.processment_errors;
CREATE TABLE Intellect.processment_errors (
		id BIGINT  NOT NULL,
		name VARCHAR(70) NOT NULL,
		description VARCHAR(255) NOT NULL,
		error_code NUMERIC(6,0) NOT NULL,
		processment_group VARCHAR(70) NOT NULL,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
CONSTRAINT pk_intellect_processment_errors_idx PRIMARY KEY (id)
);



--DROP TABLE IF EXISTS  Intellect.processment_errors_log;
CREATE TABLE Intellect.processment_errors_log (
		id BIGINT  NOT NULL,
		name VARCHAR(70) NOT NULL,
		processment_errors_id BIGINT NOT NULL,
		processment_execution_id BIGINT NULL,
		processment_group VARCHAR(70) NOT NULL,
		processment_mode VARCHAR(70) NOT NULL,
		java_class VARCHAR(255) NULL,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
CONSTRAINT pk_intellect_processment_errors_log_idx PRIMARY KEY (id),
CONSTRAINT fk_processment_errors_log_errors_id FOREIGN KEY(processment_errors_id) 
	REFERENCES Intellect.processment_errors(id),
CONSTRAINT fk_processment_errors_log_processment_execution_id FOREIGN KEY(processment_execution_id) 
	REFERENCES Intellect.processment_execution(id)
);


--DROP TABLE IF EXISTS Intellect.csv_load_lot
CREATE TABLE Intellect.csv_load_lot (
		id BIGINT  NOT NULL,
		lot_name VARCHAR(70) NOT NULL,
		load_path VARCHAR(255) NOT NULL,
		processment_execution_id BIGINT NOT NULL,
		status NUMERIC(2,0) NOT NULL,	
		files_loaded NUMERIC(8,0) NOT NULL DEFAULT 0,
		files_error_not_loaded NUMERIC(8,0) NOT NULL DEFAULT 0,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
CONSTRAINT pk_csv_load_lot_idx PRIMARY KEY (id),
CONSTRAINT fk_csv_load_lot_processment_execution_id FOREIGN KEY(processment_execution_id) 
	  REFERENCES Intellect.processment_execution(id)
);
/*
 STATUS_LOADING = 0;                                            
 STATUS_ARCHIVING = 10;                                         
 STATUS_FINISHED_NOERRORS_ARCHIVED = 30;                        
 STATUS_FINISHED_WITHERRORS_ARCHIVED = -30;                     
 STATUS_FINISHED_WITHERRORS_NOTARCHIVED = -31;                  
 STATUS_ERROR_NOTFINISHED_NOTARCHIVED = -2;                     
 STATUS_ERROR_NOTFINISHED_ARCHIVED_BY_CLEANUP_PROCESS = -50;    
 STATUS_ERROR_ARCHIVED = -1;                                    
*/

--DROP TABLE IF EXISTS Intellect.csv_load_registry
CREATE TABLE Intellect.csv_load_registry (
		id BIGINT  NOT NULL,
		lot_name VARCHAR(70) NOT NULL,
		lot_id BIGINT  NOT NULL,
		file_name VARCHAR(70) NOT NULL,
		load_path VARCHAR(255) NOT NULL,
		processment_execution_id BIGINT NOT NULL,
		status NUMERIC(2,0) NOT NULL,	-- 0 loading, 1 loaded success, -1 error not loaded, 2 archived
		error_msg TEXT NULL,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
CONSTRAINT pk_csv_load_registry_idx PRIMARY KEY (id),
CONSTRAINT fk_csv_load_registry_processment_execution_id FOREIGN KEY(processment_execution_id) 
	  REFERENCES Intellect.processment_execution(id),
CONSTRAINT fk_csv_load_registry_load_lot_id FOREIGN KEY(lot_id) 
	  REFERENCES Intellect.csv_load_lot(id)
);


CREATE TABLE Intellect.hot_table (
		id BIGINT  NOT NULL,
		asset VARCHAR(11) NOT NULL,
		data DATE NOT NULL,
		hora TIME NOT NULL,
		ultimo NUMERIC(8,2) NOT NULL,
		strike NUMERIC(8,2) NOT NULL,
		valor_ativo NUMERIC(8,2) NOT NULL,
		valor_medio_negocios NUMERIC(8,2) NOT NULL,
		negocios INTEGER NOT NULL,
		quantidade INTEGER NOT NULL,
		volume NUMERIC(18,2) NOT NULL,
		media_dia_negocios INTEGER DEFAULT  NULL,
		media_dia_quantidade INTEGER DEFAULT  NULL,
		media_dia_volume NUMERIC(18,2) DEFAULT NULL,
		oferta_compra NUMERIC(8,2) NOT NULL,
		oferta_venda NUMERIC(8,2) NOT NULL,
		VOC INTEGER NOT NULL,
		VOV INTEGER NOT NULL,
		vencimento DATE NOT NULL,
		validade DATE NOT NULL,
		contratos_abertos BIGINT NOT NULL,
		estado_atual VARCHAR(80) NOT NULL,
		relogio_last_change TIMESTAMP NOT NULL,
CONSTRAINT pk_intellect_hot_table_idx PRIMARY KEY (id)
);