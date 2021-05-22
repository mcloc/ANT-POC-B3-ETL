select volume/negocios volume_dividido_qtd, * from B3Log.B3SignalLoggerLevel1  where asset like 'BOVA%' 
order by volume desc
limit 1000

select * from B3Log.B3SignalLoggerLevel1 
where estado_atual= 'Aberto' 
AND relogio::date 
BETWEEN '2021-02-02 10:01:00.0' AND '2021-02-02 10:02:00.0'
AND asset = 'VALEB972'
order by relogio Asc limit 1000


select  ativo from B3Log.B3AtivosOpcoes group by ativo

select * from B3Log.B3SignalLoggerLevel1 a 
where estado_atual= 'Aberto' 
AND a.asset like  substring('BOVA11', '[A-Z]+')||'%' AND a.strike != 0 AND relogio >= TO_TIMESTAMP('2021-01-27 00:00:00','YYYY-MM-DD HH24:MI:SS') 
AND a.relogio <= TO_TIMESTAMP('2021-01-27 23:59:5','YYYY-MM-DD HH24:MI:SS') 
ORDER BY a.relogio ASC


select * from B3Log.B3SignalLoggerLevel1 order by relogio desc limit 1000

select * from B3Log.B3SignalLoggerLevel2Negocios 
where estado_atual= 'Aberto' 
AND relogio_last_change::date 
BETWEEN '2021-01-26 00:01:00.0' AND '2021-01-26 23:02:00.0'
AND asset = 'VALEB952'
order by volume DESC
limit 1000


select * from B3Log.B3SignalLoggerLevel2Negocios 
where estado_atual= 'Aberto' 
AND relogio_last_change::date 
BETWEEN '2021-01-26 00:01:00.0' AND '2021-01-26 23:02:00.0'
AND asset = 'VALEB952'
order by relogio_last_change DESC
limit 1000


select sum(volume) from B3Log.B3SignalLoggerLevel2Negocios 
where estado_atual= 'Aberto' 
AND relogio_last_change::date 
BETWEEN '2021-01-26 10:01:00.0' AND '2021-01-26 19:52:00.0'
AND asset = 'VALEB972'
group by asset, volume
HAVING SUM(volume) > 4000000
order by volume DESC
limit 1000


select to_char(date(relogio),'YYYY-MM-DD'), count(1) from B3Log.B3SignalLogger
group by 1

--delete  from B3Log.B3SignalLogger where relogio > '2021-01-04 00:00:00.0' and relogio < '2021-01-04 23:00:00.0'


SELECT asset,count(1) as total_volume_acima_4Mega
FROM B3Log.B3SignalLoggerLevel2Negocios  
where estado_atual= 'Aberto' 
AND relogio_last_change::date 
BETWEEN '2021-01-26 10:01:00.0' AND '2021-01-26 19:52:00.0'
AND asset = 'VALEB972'
group by ROLLUP(asset )
HAVING SUM(volume) > 4000000
order by asset;

SELECT asset,sum(volume) /1000000 volume, sum(negocios) as total
FROM B3Log.B3SignalLoggerLevel2Negocios  
where estado_atual= 'Aberto' 
AND relogio_last_change::date 
BETWEEN '2021-01-26 10:01:00.0' AND '2021-01-26 19:52:00.0'
AND asset = 'VALEB972'
group by ROLLUP(asset, negocios )
HAVING SUM(volume) < 4000000
order by volume desc, total DESC;



WITH table1 (asset, volume, total) as (
	SELECT asset,sum(volume) /1000000 volume, sum(negocios) as total
	FROM B3Log.B3SignalLoggerLevel2Negocios  
	where estado_atual= 'Aberto' 
	AND relogio_last_change::date 
	BETWEEN '2021-01-26 10:01:00.0' AND '2021-01-26 19:52:00.0'
	AND asset = 'VALEB972'
	group by ROLLUP(asset, negocios )
	HAVING SUM(volume) < 4000000
	order by volume desc, total DESC
) SELECT asset, sum(volume) total_volume, sum(total) total_neogocios from table1 group by asset;


select estado_atual from B3Log.B3SignalLoggerLevel1 group by estado_atual



select diff_volume/diff_quantidade as vol_dividido_qtd,  * from B3Log.B3SignalLoggerLevel2Negocios a
where  a.asset  = 'VALEB952' AND a.strike != 0 
AND a.relogio_last_change >= TO_TIMESTAMP('2021-01-26 13:32:57.0','YYYY-MM-DD HH24:MI:SS') 
AND a.relogio_last_change <= TO_TIMESTAMP('2021-01-26 13:35:50.0','YYYY-MM-DD HH24:MI:SS') 
ORDER BY a.relogio_last_change ASC



select * from B3Log.B3SignalLoggerLevel2Negocios a
where  a.asset like  substring('VALEB952', '[A-Z]+')||'%' AND a.strike != 0 
AND a.relogio_last_change >= TO_TIMESTAMP('2021-01-26 13:32:57.0','YYYY-MM-DD HH24:MI:SS') 
AND a.relogio_last_change <= TO_TIMESTAMP('2021-01-26 13:35:50.0','YYYY-MM-DD HH24:MI:SS') 
ORDER BY a.relogio_last_change ASC

select * from B3Log.B3SignalLoggerLevel2Negocios a
where  a.asset like  substring('BOVA11', '[A-Z]+')||'%' AND a.strike != 0 
AND a.relogio_last_change >= TO_TIMESTAMP('2021-01-26 13:33:57.0','YYYY-MM-DD HH24:MI:SS') 
AND a.relogio_last_change <= TO_TIMESTAMP('2021-01-26 13:35:50.0','YYYY-MM-DD HH24:MI:SS') ORDER BY a.relogio_last_change ASC


select  a.data, a.hora, a.asset, b.ultimo valor_ativo, a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.validade, a.estado_atual, a.relogio,  
 a.VOC, a.VOV, a.contratos_abertos,  a.negocios, a.quantidade, a.volume 
 from B3Log.b3signallogger a 			LEFT JOIN B3Log.b3signallogger b ON a.relogio=b.relogio AND b.asset = 'BOVA11' 
			where  a.asset like  substring('BOVA11', '[A-Z]+')||'%' AND a.strike != 0 
			AND a.relogio >= TO_TIMESTAMP('2021-01-26 00:00:00','YYYY-MM-DD HH24:MI:SS') 
			AND a.relogio <= TO_TIMESTAMP('2021-01-26 23:59:5','YYYY-MM-DD HH24:MI:SS') ORDER BY a.relogio ASC


------------------------------------ B4 2021-03-17 --------------------------------------------------------



--CREATE SCHEMA B3Log e da auth pro user ANTOption
CREATE SCHEMA B3Log AUTHORIZATION "ANTOption";
--FIM CREATE SCHEMA B3Log e da auth pro user ANTOption


--CREATE TABLE B3SignalLogger
--DROP TABLE B3Log.B3SignalLogger
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



CREATE TABLE b3log.B3SignalLoggerLevel1 (
		id BIGINT  NOT NULL,
		asset VARCHAR(11) NOT NULL,
		data DATE NOT NULL,
		hora TIME NOT NULL,
		ultimo NUMERIC(8,2) NOT NULL,
		strike NUMERIC(8,2) NOT NULL,
		valor_ativo NUMERIC(8,2) NOT NULL,
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






-- FIM DO CREATE TABLE


-- PARA DAR CARGA NO CSV COM COMANDO "COPY' USUARIO TEM DE TER ESSA ROLE
GRANT pg_read_server_files TO ANTOption;


-- **************   ZERAR TABELAS E INDICES B3SignalLogger ***************************
-- APAGAR REGISTROS CARREGADOS ( C U I D A D O )
DELETE FROM B3Log.B3SignalLogger ;
--ZERAR SEQUENCE AUTO INCREMENT
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE B3Log.b3signallogger_id;
CREATE SEQUENCE B3Log.b3signallogger_id;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN id SET DEFAULT nextval('B3Log.b3signallogger_id');
ALTER SEQUENCE B3Log.b3signallogger_id OWNED BY B3Log.B3SignalLogger.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT

select * from B3Log.B3SignalLogger order by relogio ASC;


-- **************   ZERAR TABELAS E INDICES B3SHistorical ***************************
-- APAGAR REGISTROS B3Historico ( C U I D A D O )
DELETE FROM B3Log.b3Historical ;
--ZERAR SEQUENCE AUTO INCREMENT B3Historico
ALTER TABLE ONLY B3Log.b3Historical ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE B3Log.b3historical_id;
CREATE SEQUENCE B3Log.b3historical_id;
ALTER TABLE ONLY B3Log.b3Historical ALTER COLUMN id SET DEFAULT nextval('B3Log.b3historical_id');
ALTER SEQUENCE B3Log.b3historical_id OWNED BY B3Log.b3Historical.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT


-- **************   ZERAR TABELAS E INDICES B3SignalLoggerLevel1 ***************************
-- APAGAR REGISTROS B3Historico ( C U I D A D O )
DELETE FROM B3Log.B3SignalLoggerLevel1 ;
--ZERAR SEQUENCE AUTO INCREMENT B3Historico
ALTER TABLE ONLY B3Log.B3SignalLoggerLevel1 ALTER COLUMN id SET DEFAULT 0;
DROP SEQUENCE B3Log.b3signalloggerLevel1_id;
CREATE SEQUENCE B3Log.b3signalloggerLevel1_id;
ALTER TABLE ONLY B3Log.B3SignalLoggerLevel1 ALTER COLUMN id SET DEFAULT nextval('B3Log.b3signalloggerLevel1_id');
ALTER SEQUENCE B3Log.b3signalloggerLevel1_id OWNED BY B3Log.B3SignalLoggerLevel1.id;
--FIM DO ZERAR SEQUENCE AUTO INCREMENT

select * from B3Log.b3signalloggerlevel1 
where asset = 'VALEB912'
order by relogio ASC,  asset ASC, data ASC, hora ASC;

select asset, count(1) count_registros, volume, ultimo from B3Log.b3signalloggerlevel1 
where asset = 'VALEB912'
group by volume, ultimo, asset
limit 100;


select count(1) from B3Log.b3signalloggerlevel1 

-- **************   ALGTER TABLES TEMPORARIOS, O QUE ESTA AQUI JAH ESTA ATUALIZADO NOS CREATE TABLE  ***************************
--ALTERACOES DE MODELAGEM, SOMENTE PARA HISTORICO ;DE AÇÔES FEITAS
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN relogio TYPE TIMESTAMP, ALTER COLUMN relogio SET NOT NULL;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN negocios TYPE INTEGER, ALTER COLUMN negocios SET NOT NULL;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN volume  TYPE float ;
ALTER TABLE ONLY ANTOption.b3log.B3SignalLogger ALTER COLUMN oferta_compra TYPE REAL, ALTER COLUMN oferta_compra SET NOT NULL;
ALTER TABLE ONLY ANTOption.b3log.B3SignalLogger ALTER COLUMN oferta_venda TYPE REAL, ALTER COLUMN oferta_venda SET NOT NULL;
ALTER TABLE ONLY ANTOption.b3log.B3SignalLogger ALTER COLUMN ultimo TYPE REAL, ALTER COLUMN ultimo SET NOT NULL;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN VOC TYPE INTEGER, ALTER COLUMN VOC SET NOT NULL;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN VOV TYPE INTEGER, ALTER COLUMN VOC SET NOT NULL;
--FIM ALTERACOES DE MODELAGEM, SOMENTE PARA HISTORICO


ALTER TABLE ONLY B3Log.B3Historical ALTER COLUMN PREMED TYPE NUMERIC(11,2), ALTER COLUMN PREMED SET NOT NULL;
ALTER TABLE ONLY B3Log.B3Historical ALTER COLUMN PTOEXE TYPE NUMERIC(13,6), ALTER COLUMN PTOEXE SET NOT NULL
ALTER TABLE ONLY B3Log.B3Historical ALTER COLUMN DATA_PREGAO TYPE DATE, ALTER COLUMN DATA_PREGAO SET NOT NULL;

-- ALTER TABLE PARA NUMERICS COM PRECISION E SCALE
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN oferta_compra TYPE NUMERIC(8,2), ALTER COLUMN oferta_compra SET NOT NULL;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN oferta_venda TYPE NUMERIC(8,2), ALTER COLUMN oferta_venda SET NOT NULL;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN strike TYPE NUMERIC(8,2), ALTER COLUMN strike SET NOT NULL;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN ultimo TYPE NUMERIC(8,2), ALTER COLUMN ultimo SET NOT NULL;
ALTER TABLE ONLY B3Log.B3SignalLogger ALTER COLUMN volume TYPE NUMERIC(18,2), ALTER COLUMN volume SET NOT NULL;
-- FIM ALTER TABLE PARA NUMERICS COM PRECISION E SCALE
-- **************   F I M    -  ALGTER TABLES TEMPORARIOS, O QUE ESTA AQUI JAH ESTA ATUALIZADO NOS CREATE TABLE  ***************************





-- **************   LOAD CSV SE ENCONTRA EM OUTRO file.sql o loadLog.sql  ***************************
-- LOAD CSV Total1-UTF8.csv
COPY  B3Log.B3SignalLogger  ( 
asset,
data,
hora,
ultimo,
strike,
negocios,
quantidade,
volume,
oferta_compra,
oferta_venda,
VOC,
VOV,
vencimento,
validade,
contratos_abertos,
estado_atual,
relogio
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/
DELIMITER ';'  CSV HEADER;
--) FROM  '/home/mcloc/devel/ANT/OpBot/ANT-Option-resources/datasets/'


-- BOVA11 LOG 29-01-2021.csv'
-- BOVA11 LOG 28-01-2021.csv'
-- BOVA11 LOG 27-01-2021.csv'
-- BOVA11 LOG 26-01-2021.csv'

-- BOVA11 LOG 12-02-2021.csv'
-- BOVA11 LOG 11-02-2021.csv'
-- BOVA11 LOG 10-02-2021.csv'
-- BOVA11 LOG 09-02-2021.csv'
-- BOVA11 LOG 08-02-2021.csv'
-- BOVA11 LOG 05-02-2021.csv'
-- BOVA11 LOG 04-02-2021.csv'
-- BOVA11 LOG 03-02-2021.csv'
-- BOVA11 LOG 02-02-2021.csv'

-- FIM DO LOAD CSV Total1-UTF8.csv
-- **************  F I M  ----  LOAD CSV SE ENCONTRA EM OUTRO file.sql o loadLog.sql  ***************************







-- **************  QUERIES DE USO COMUM - buffer.sql  ***************************
--queries
select max(relogio) from B3Log.B3SignalLogger ;
select count(1) from B3Log.B3SignalLogger;

select * from B3Log.B3SignalLogger;

select asset, data, hora, ultimo, strike, negocios, vencimento, relogio from b3log.b3signallogger order by relogio limit 1000

SELECT * FROM B3Log.B3SignalLogger ORDER By relogio ASC LIMIT 100000 OFFSET 600000
SELECT asset FROM B3Log.B3SignalLogger group by asset order by 1




-- **************   queries de B3 Historical ***************************
select count(1) from B3Log.B3historical
-- TOTAL HISTORICO ATIVOS 1.446.541 registros

select * from B3Log.B3Historical where voltot > 0 limit 1000

select codneg, count(1) from B3Log.B3Historical group by (1);

select codneg, sum(voltot), count(1) from B3Log.B3Historical  where voltot > 0 group by (codneg) order by 2 DESC,3 DESC, codneg;

select codneg, sum(totneg), count(1) from B3Log.B3Historical  where voltot > 0 group by (codneg) order by 2 DESC,3 DESC, codneg;

select codneg, sum(voltot), count(1) from B3Log.B3Historical  where voltot > 0 group by (codneg) order by 2,3, codneg;

-- M A X  data_pregao 
select max(data_pregao) from B3Log.B3historical ;
--2021-02-12
-- M I N  data_pregao 
select min(data_pregao) from B3Log.B3historical ;
--2020-01-02

select count(1) from B3Log.B3historical where ptoexe > 0
select * from B3Log.B3historical where ptoexe > 0 order by data_pregao desc

select data_pregao, codneg, PREABE , PREMAX , PREMIN , PREMED , PREULT , PREOFC, PREOFV, PREEXE, PTOEXE from B3Log.B3Historical 
where codneg = 'VALE3' AND data_pregao < '2020-01-30' order by 1 DESC

select data_pregao, codneg, PREULT from B3Log.B3Historical 
where codneg = 'VALE3' AND data_pregao < '2020-03-30' order by 1 DESC

select data_pregao, codneg, PREULT from B3Log.B3Historical 
where codneg = 'VALE3' 
AND preult = 0 

SELECT MAX(data_pregao) FROM B3Log.B3Historical 

select CODNEG from B3Log.B3Historical group by 1 order by 1
select asset, strike, vencimento, relogio, count(1) from B3Log.b3signallogger where relogio = '2021-04-02 10:56:00' group by asset, strike, vencimento,relogio


-- ativos no log
select asset, substring(asset FROM  '[A-Z]+') from B3Log.B3SignalLogger  
WHERE strike = 0
group by asset
order by asset 



-- opcoes no log
select a.asset, ultimo from B3Log.B3SignalLogger  a 
LEFT JOIN B3Log.B3SignalLogger  b ON a.asset like 
WHERE a.strike != 0 
group by asset
order by asset 


select * from B3Log.B3SignalLogger order by relogio DESC

select * from B3Log.B3SignalLogger 
where relogio1
order by relogio DESC

-- **************   queries de B3 Logger ***************************
-- COUNT TOTAL REGISTROS
select count(1) from B3Log.B3SignalLogger 
-- até log dia 05 de FEV carregado em 08 de fev total: 6941215
-- até log dia 12 de FEV carregado em 27 de mar total: 10796383


-- M A X  relogio 
select max(relogio) from B3Log.B3SignalLogger ;
--2021-12-02 13:03:00.0 ???? mes 12? pau do arquivo 0302 pra frente
--2021-02-02 18:02:00.0

select count(1), relogio from B3Log.B3SignalLogger
where relogio > '2021-02-05'
group by ROLLUP(relogio )
order by relogio desc

select * FROM B3Log.B3SignalLogger
where asset like 'PETR%' and asset != 'PETR4'




select * FROM B3Log.B3SignalLogger
order by id desc limit 100



select * FROM B3Log.B3SignalLogger
where relogio >= '2021-02-15'
and asset = 'WINFUT'


select count(1) FROM B3Log.B3SignalLogger
where relogio >= '2021-02-15'



--DELETE FROM B3Log.B3SignalLogger
where relogio >= '2021-02-15' and relogio < '2021-05-02' 

--delete from B3Log.B3SignalLogger
where relogio > '2021-02-02'

-- M I N  relogio 
select min(relogio) from B3Log.B3SignalLogger ;
--2021-01-26 09:45:01.0

SELECT relogio::date
    FROM B3Log.B3SignalLogger 
    WHERE  relogio::date 
      BETWEEN '2021-01-26 09:45:01.0' AND '2021-01-26 18:29:59.0'
    GROUP BY relogio::date inteval 50 MI




-- **************  QUERIES DE ANALISE GROUP BY ETC - migrará para outro file.sql o analise.sql  ***************************
SELECT asset, count(*), max(relogio::date),
       min(relogio::date)
FROM B3Log.B3SignalLogger 
GROUP BY (asset,to_char(relogio, 'HH:MI DD/MONTH/YYYY'))
ORDER BY (to_char(relogio, 'HH:MI DD/MONTH/YYYY')) ASC LIMIT 100000 OFFSET 200000

SELECT asset, count(*), max(relogio::date),
       min(relogio::date)
FROM B3Log.B3SignalLogger 
GROUP BY (asset,to_char(relogio, 'HH:MI DD/MONTH/YYYY'))
ORDER BY (to_char(relogio, 'HH:MI DD/MONTH/YYYY')) ASC LIMIT 100000 OFFSET 200000

SELECT min(relogio)as START_TIME, max(relogio) as END_TIME 
	FROM B3Log.B3SignalLogger
	GROUP BY (min(relogio))
	ORDER BY relogio ASC
	LIMIT 100000 OFFSET 200000;


SELECT asset, min(relogio)as START_TIME, max(relogio) as END_TIME 
	FROM B3Log.B3SignalLogger
	GROUP BY (asset,relogio)
	ORDER BY relogio ASC
	LIMIT 100000 OFFSET 200000;


select asset 
FROM B3Log.B3SignalLogger
GROUP BY (asset)

/* ASSETS
BOVA11
BOVAB106
BOVAB110
BOVAB112
BOVAB114
BOVAB116
BOVAB118
BOVAB120
BOVAB122
BOVAB124
BOVAC110
BOVAC112
BOVAC114
BOVAC116
BOVAC118
BOVAC12
BOVAC122
BOVAC124
BOVAC126
BOVAC128
VALE3
VALEB912
VALEB920
VALEB932
VALEB937
VALEB947
VALEB952
VALEB972
VALEO837
VALEO842
VALEO847
VALEO857
*/




-- INCONSISTENCIAS:
-- 1: BOVAB100;44531; FALTOU DATA NA SEGUNDA COLUNA, REMOVIDO 12604 REGISTROS
-- 2; BOVAB124;19/01/2021;0.686944444;0.   TODOS ATIVOS COM ESTE float NO LUGAR DA HORA (centenas de milhares deles)


----------------------------------------------------- after April 2021 ----------------------------------


select * from intellect.processment_execution order by 1 desc 

select * from Intellect.csv_load_registry 
where status < 1  
--and lot_id = 28 
order by lot_name desc 

select * from Intellect.csv_load_lot 
--where status <= -1 
order by lot_name 

update Intellect.csv_load_lot set status = 300 where id = 1

select * from Intellect.processment_errors_log

select * from Intellect.processment_rotines where active_status = true
hot_


truncate Intellect.csv_load_registry RESTART IDENTITY;
truncate Intellect.csv_load_lot RESTART IDENTITY CASCADE;
truncate Intellect.processment_execution RESTART IDENTITY CASCADE;
truncate b3log.b3ativosopcoes RESTART IDENTITY CASCADE;


 select 'BOVA11' as ativo, 'BOVA' as substr_ativo, asset as opcao_ativo 
from B3Log.B3SignalLoggerRaw  
WHERE lot_id = 1 AND (strike != 0 AND asset like 'BOVA%')
group by 1,2,3
order by 1,3

INSERT INTO Intellect.hot_table_derivatives(data,hora,asset,valor_ativo,ultimo,strike,oferta_compra,oferta_venda,vencimento,validade,estado_atual,relogio_last_change,VOC, VOV, contratos_abertos, valor_medio_by_negocios, valor_medio_by_quantidade, negocios, quantidade, volume, lot_name, lot_id) 
VALUES('2021-05-06 -03', '16:42:41-03', 'PETRR246', 23.42, 3.3, 26.46, 3.01, 3.64, '2021-06-18 -03', '2021-06-18 -03', 'Aberto', '2021-05-06 16:42:45-03', 11000, 10000, 39800, 2315682.5, 3.3, 4, 2805000, 9262730.0,'RTD_20210506','1'::numeric)


select * from intellect.hot_table_derivatives

select count(1) from intellect.hot_table_derivatives

select * from intellect.hot_tab:le_assets

select count(1) from intellect.hot_table_assets

select * from B3Log.b3signalloggerRaw
limit 100

select count(1) from B3Log.b3signalloggerRaw

select min(id) from B3Log.b3signalloggerRaw

select max(id) from B3Log.b3signalloggerRaw


delete from  B3Log.b3signalloggerRaw where asset NOT LIKE substring('PETR', '[A-Z]+')||'%'

select relogio from B3Log.b3signalloggerRaw where id = 500000

select asset from B3Log.b3signalloggerRaw where id < 500000
group by 1

--delete from B3Log.b3signalloggerRaw where id > 500000


select * from b3log.b3ativosopcoes; 

--delete from b3log.b3ativosopcoes;


SELECT relname, oid, relfilenode FROM pg_class WHERE relname = 'B3SignalLogger';

SELECT * FROM pg_class WHERE relname  like 'b3%';

SELECT * FROM pg_class WHERE relnamespace = 16394

SELECT * FROM pg_class WHERE oid  = 16392



select  a.data, a.hora, a.asset, b.ultimo valor_ativo, a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.validade, a.estado_atual, a.relogio,   a.VOC, a.VOV, a.contratos_abertos,  a.negocios, a.quantidade, a.volume  from B3Log.b3signalloggerraw a 			LEFT JOIN B3Log.b3signalloggerraw b ON a.relogio=b.relogio AND b.asset = 'PETR4' 			where  a.asset like  substring('PETR4', '[A-Z]+')||'%' AND a.strike != 0 	and a.lot_id = 1and b.lot_id = 1ORDER BY a.relogio ASC

select  count(1) from B3Log.b3signalloggerraw a 			LEFT JOIN B3Log.b3signalloggerraw b ON a.relogio=b.relogio AND b.asset = 'PETR4' 			where  a.asset like  substring('PETR4', '[A-Z]+')||'%' AND a.strike != 0 	and a.lot_id = 1and b.lot_id = 1 



WITH ativos as (
select asset as ativo, substring(asset, '[A-Z]+') as opcao from B3Log.B3SignalLogger  
		WHERE strike = 0
		group by asset
		order by asset
)
SELECT b.ativo, a.asset FROM B3Log.B3SignalLogger a
LEFT JOIN ativos b on b.opcao =  substring(a.asset, '[A-Z]+')
WHERE strike != 0 
group by b.ativo, a.asset order by ativo, asset;



SELECT a.asset, b.asset, substring(a.asset, '[A-Z]+') as opcao FROM B3Log.B3SignalLogger a
LEFT JOIN B3Log.B3SignalLogger b  on b.strike = 0 AND b.asset like substring(a.asset, '[A-Z]+')||'%'
WHERE a.strike != 0 
group by a.asset, opcao, b.asset order by b.asset, a.asset
limit 1000;

INSERT INTO Intellect.processment_rotines (name, description, active_status, processment_seq, processment_group, processment_mode, new_thread, thread_name, created_at) VALUES 
('handler_start', 'ETL Level-1 populate assets', true, 21, 'etl', 'batch_process', false,null,   now())


update Intellect.csv_load_lot set status = 30 where id = 1	