CREATE TABLE b3signalloggerlevel1 (
	id bigserial NOT NULL DEFAULT nextval('b3log.b3signalloggerlevel1_id'::regclass),
	asset character varying(11) NOT NULL,
	data date NOT NULL,
	hora time NOT NULL,
	ultimo numeric(8, 2) NOT NULL,
	strike numeric(8, 2) NOT NULL,
	valor_ativo numeric(8, 2) NOT NULL,
	negocios integer NOT NULL,
	quantidade integer NOT NULL,
	volume numeric(18, 2) NOT NULL,
	oferta_compra numeric(8, 2) NOT NULL,
	oferta_venda numeric(8, 2) NOT NULL,
	log_devirada1semana numeric(43, 40) DEFAULT NULL::numeric,
	preco_blackshoes1semanas numeric(8, 2) DEFAULT NULL::numeric,
	log_devirada2semana numeric(43, 40) DEFAULT NULL::numeric,
	preco_blackshoes2semanas numeric(8, 2) DEFAULT NULL::numeric,
	log_devirada3semana numeric(43, 40) DEFAULT NULL::numeric,
	preco_blackshoes3semanas numeric(8, 2) DEFAULT NULL::numeric,
	log_devirada3meses numeric(43, 40) DEFAULT NULL::numeric,
	preco_blackshoes3meses numeric(8, 2) DEFAULT NULL::numeric,
	log_devirada6meses numeric(43, 40) DEFAULT NULL::numeric,
	preco_blackshoes6meses numeric(8, 2) DEFAULT NULL::numeric,
	log_devirada12meses numeric(43, 40) DEFAULT NULL::numeric,
	preco_blackshoes12meses numeric(8, 2) DEFAULT NULL::numeric,
	log_devirada18meses numeric(43, 40) DEFAULT NULL::numeric,
	preco_blackshoes18meses numeric(8, 2) DEFAULT NULL::numeric,
	log_devirada24meses numeric(43, 40) DEFAULT NULL::numeric,
	preco_blackshoes24meses numeric(8, 2) DEFAULT NULL::numeric,
	log_devirada36meses numeric(43, 40) DEFAULT NULL::numeric,
	preco_blackshoes36meses numeric(8, 2) DEFAULT NULL::numeric,
	voc integer NOT NULL,
	vov integer NOT NULL,
	vencimento date NOT NULL,
	validade date NOT NULL,
	contratos_abertos bigint NOT NULL,
	estado_atual character varying(80) NOT NULL,
	relogio timestamp NOT NULL
);
ALTER TABLE b3signalloggerlevel1 ADD CONSTRAINT b3signalloggerlevel1_pk PRIMARY KEY(id);
CREATE TABLE b3historical (
	id bigserial NOT NULL DEFAULT nextval('b3log.b3historical_id'::regclass),
	tipreg numeric(2, 0) NOT NULL,
	data_pregao date NOT NULL,
	tpmerc numeric(3, 0) NOT NULL,
	codbdi character(2) NOT NULL,
	prazot character(3) NOT NULL,
	codneg character varying(12) NOT NULL,
	premax numeric(11, 2) NOT NULL,
	preabe numeric(11, 2) NOT NULL,
	premin numeric(11, 2) NOT NULL,
	ptoexe numeric(13, 6) NOT NULL,
	modref character(4) NOT NULL,
	preofv numeric(11, 2) NOT NULL,
	preult numeric(11, 2) NOT NULL,
	dismes numeric(3, 0) NOT NULL,
	codisi character varying(12) NOT NULL,
	premed numeric(11, 2) NOT NULL,
	fatcot numeric(7, 0) NOT NULL,
	preofc numeric(11, 2) NOT NULL,
	indopc numeric(1, 0) NOT NULL,
	quatot character varying(18) NOT NULL,
	voltot numeric(16, 2) NOT NULL,
	preexe numeric(11, 2) NOT NULL,
	datven numeric(8, 0) NOT NULL,
	totneg numeric(5, 0) NOT NULL,
	nomres character varying(12) NOT NULL,
	especi character varying(10) NOT NULL
);
ALTER TABLE b3historical ADD CONSTRAINT b3historical_pk PRIMARY KEY(id);
CREATE INDEX b3historical_idx ON b3historical (tpmerc);
CREATE INDEX b3historical_bova11_idx ON b3historical (codneg);
CREATE INDEX b3historical_bova11_idx2 ON b3historical (totneg);
CREATE INDEX b3historical_vale3_idx ON b3historical (codneg);
CREATE INDEX b3historical_vale3_idx2 ON b3historical (totneg);
CREATE TABLE b3signallogger (
	id bigserial NOT NULL DEFAULT nextval('b3log.b3signallogger_id'::regclass),
	asset character varying(11) NOT NULL,
	data date NOT NULL,
	hora time NOT NULL,
	ultimo numeric(8, 2) NOT NULL,
	strike numeric(8, 2) NOT NULL,
	negocios integer NOT NULL,
	quantidade integer NOT NULL,
	volume numeric(18, 2) NOT NULL,
	oferta_compra numeric(8, 2) NOT NULL,
	oferta_venda numeric(8, 2) NOT NULL,
	voc integer NOT NULL,
	vov integer NOT NULL,
	vencimento date NOT NULL,
	validade date NOT NULL,
	contratos_abertos bigint NOT NULL,
	estado_atual character varying(80) NOT NULL,
	relogio timestamp NOT NULL
);
ALTER TABLE b3signallogger ADD CONSTRAINT b3signallogger_pk PRIMARY KEY(id);
CREATE INDEX b3signallogger_vencimento_idx ON b3signallogger (vencimento);
CREATE INDEX b3signallogger_relogio_idx1 ON b3signallogger (relogio);
CREATE TABLE b3logsignalunique (
	id bigserial NOT NULL DEFAULT nextval('b3log.b3logsignalunique_id_seq'::regclass),
	data date,
	hora time,
	asset character varying(12),
	valor_ativo numeric(8, 2),
	preco_opcao numeric(8, 2),
	strike numeric(8, 2),
	oferta_compra numeric(8, 2),
	oferta_venda numeric(8, 2),
	vencimento date,
	desvio_padrao_3mses numeric(42, 40),
	desvio_padrao_6mses numeric(42, 40),
	desvio_padrao_12mses numeric(42, 40),
	desvio_padrao_18mses numeric(42, 40),
	desvio_padrao_24mses numeric(42, 40),
	desvio_padrao_36mses numeric(42, 40),
	preco_blackshoes numeric(8, 2),
	relogio timestamp
);
ALTER TABLE b3logsignalunique ADD CONSTRAINT b3logsignalunique_pkey PRIMARY KEY(id);
CREATE TABLE volatilidade_historica (
	codneg character varying(12),
	data_pregao date,
	preult numeric(11, 2),
	anterior numeric(131089, 0),
	variacao numeric(131089, 0),
	log_variacao numeric(131089, 0),
	update_at timestamp with time zone
);
CREATE TABLE ativos_maior_liquidez (
	codneg character varying(12),
	volume_total numeric(131089, 0),
	total_negociacoes numeric(131089, 0),
	total_dias_negociados bigint
);
