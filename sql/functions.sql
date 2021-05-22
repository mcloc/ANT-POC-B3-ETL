/**
 *   FUNCTION _SQL_get_ativos_maior_liquidez(_total_ativos int) 
 *	@autho: MC LOC
 *	@date: 2021-02-21
 */

 
CREATE OR REPLACE FUNCTION _SQL_get_ativos_maior_liquidez(_total_ativos int) 
RETURNS void AS 
$$
DECLARE
	stmt TEXT;
BEGIN
	IF _SQL_check_table_exists('B3Log','ativos_maior_liquidez') THEN
		EXECUTE 'DROP TABLE B3Log.ativos_maior_liquidez'; 
	END IF;
	IF (SELECT count(*) FROM pg_prepared_statements WHERE name like 'prepared_qry_maior_liquidez') > 0 THEN
	    DEALLOCATE prepared_qry_maior_liquidez;
	END IF;
	PREPARE prepared_qry_maior_liquidez(int) AS 
		SELECT codneg, sum(voltot)as volume_total, sum(totneg) as total_negociacoes, count(1) total_dias_negociados
		FROM B3Log.B3Historical  
		WHERE voltot > 0 
		group by (codneg) 
		order by 2 DESC,3 DESC, 4 DESC, codneg ASC
		limit $1;
	stmt = format('CREATE TABLE B3Log.ativos_maior_liquidez AS EXECUTE prepared_qry_maior_liquidez(%L);',_total_ativos);
	EXECUTE stmt;
	DEALLOCATE prepared_qry_maior_liquidez;
END
$$
LANGUAGE 'plpgsql' VOLATILE

--TEST  3 sec perde 
select _SQL_get_ativos_maior_liquidez(30);
select * from B3Log.ativos_maior_liquidez;

/*****F I M   FUNCTION _SQL_get_ativos_maior_liquidez(_total_ativos int) ************/



--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

 

/**
 *   FUNCTION _SQL_check_table_exists(_schema varchar, _table_name varchar) 
 *	@autho: MC LOC
 *	@date: 2021-02-21
 */
 
CREATE OR REPLACE FUNCTION _SQL_check_table_exists(_schema varchar, _table_name varchar) 
RETURNS bool AS 
$$
BEGIN
   IF EXISTS (
      SELECT  -- list can be empty
      FROM   pg_catalog.pg_class c
      JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE  n.nspname = LOWER(_schema)
      AND    n.nspname NOT LIKE 'pg_%'  -- exclude system schemas!
      AND    c.relname = LOWER(_table_name)
      AND    c.relkind = 'r')           -- you probably need this
   THEN
	RAISE NOTICE '%.% exists',_schema,_table_name ;
	RETURN TRUE;
   ELSE
	RAISE NOTICE '%.% NOT exists',_schema,_table_name ;
	RETURN FALSE;
   END IF;
END
$$
LANGUAGE 'plpgsql' VOLATILE

--TEST
select _SQL_check_table_exists('B3Log', 'volatilidade_historica');

/*****F I M   FUNCTION _SQL_check_table_exists(_schema varchar, _table_name varchar) ************/



--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------



/**
 *   FUNCTION _SQL_populateVolatilidadeHistorica(_asset varchar(12),  _dt_inicio date, _dt_fim date) 
 *	@autho: MC LOC
 *	@date: 2021-02-21
 */
--DROP FUNCTION _SQL_populateVolatilidadeHistorica(_asset varchar(12),  _dt_inicio date, _dt_fim date) 

CREATE OR REPLACE FUNCTION _SQL_populateVolatilidadeHistorica(_asset varchar(12),  _dt_inicio date, _dt_fim date) 
RETURNS bool AS 
$$
DECLARE
	stmt TEXT;
BEGIN
	RAISE NOTICE '_asset=%, _dt_inicio=%, _dt_fim=%',_asset, _dt_inicio, _dt_fim;
	IF (SELECT count(*) FROM pg_prepared_statements WHERE name like 'prepared_qry_volatilidade_historica') > 0 THEN
	    DEALLOCATE prepared_qry_volatilidade_historica;
	END IF;
	PREPARE prepared_qry_volatilidade_historica(varchar(12), date , date) AS 
		WITH VARIACAO  AS (
			SELECT 
				CODNEG, DATA_PREGAO, PREULT
			FROM B3Log.B3Historical 
			where CODNEG = $1 AND data_pregao >= $2 AND data_pregao <= $3 order by DATA_PREGAO  DESC,CODNEG
		) 
		SELECT
			CODNEG, DATA_PREGAO, PREULT,
			LAG(PREULT,-1) OVER (
				ORDER BY DATA_PREGAO DESC
			) ANTERIOR,
			PREULT/(LAG(PREULT,-1) OVER (
				ORDER BY DATA_PREGAO DESC
			)) as VARIACAO,
			log(
				PREULT/(LAG(PREULT,-1) OVER (
					ORDER BY DATA_PREGAO DESC
				)) 
			) as LOG_VARIACAO,
			now() as update_at
		FROM
			VARIACAO ;
	IF (select _SQL_check_table_exists('B3Log', 'volatilidade_historica'))  THEN
		EXECUTE 'DROP TABLE B3Log.volatilidade_historica'; 
	END IF;
	stmt = format('CREATE TABLE B3Log.volatilidade_historica AS EXECUTE prepared_qry_volatilidade_historica(%L, %L, %L) WITH DATA ;', _asset, _dt_inicio, _dt_fim);
	RAISE NOTICE 'stmt: %', stmt;
	EXECUTE stmt;
	RETURN TRUE;
	DEALLOCATE prepared_qry_volatilidade_historica;
END;
$$
LANGUAGE 'plpgsql' VOLATILE


--TEST
select _SQL_populateVolatilidadeHistorica('VALE3', '2020-01-01', '2021-03-30');
select * from B3Log.volatilidade_historica ;
select stddev(log_variacao) from B3Log.volatilidade_historica ;

--TEST
select _SQL_populateVolatilidadeHistorica('BOVA11', '2020-01-01', '2021-03-30');
select * from B3Log.volatilidade_historica ;
--select stddev(log_variacao) from B3Log.volatilidade_historica ;
/***** F I M   FUNCTION _SQL_populateVolatilidadeHistorica(_asset varchar(12),  _dt_inicio date, _dt_fim date)  ************/



--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------


/**
 *   FUNCTION _SQL_return_opcoes_com_valor_ativo_sync(_asset varchar(12),  _dt_inicio date, _dt_fim date) 
 *	@autho: MC LOC
 *	@date: 2021-02-21
 */
--DROP FUNCTION _SQL_return_opcoes_com_valor_ativo_sync(_asset varchar(12),  _dt_inicio date, _dt_fim date) 
DROP  FUNCTION _SQL_return_opcoes_com_valor_ativo_sync(_asset varchar(12),  _dt_inicio varchar(22), _dt_fim varchar(22)) 

CREATE OR REPLACE FUNCTION _SQL_return_opcoes_com_valor_ativo_sync(_asset varchar(12),  _dt_inicio varchar(22), _dt_fim varchar(22)) 
RETURNS void AS 
$$
DECLARE
	_stmt TEXT;
	/*__data date; 
	hora time; 
	asset varchar(12); 
	valor_ativo  NuMERIC(8,2); 
	preco_opcao  NuMERIC(8,2); 
	strike NuMERIC(8,2); 
	oferta_compra NuMERIC(8,2); 
	oferta_venda NuMERIC(8,2); 
	vencimento date; 
	relogio timestamp;*/
	raw_row record;
	row_number int := 0 ;
	raw_log_rows CURSOR(__dt_inicio varchar(22), __dt_fim varchar(22)) FOR
		select  a.data, a.hora, a.asset, b.ultimo valor_ativo, a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.relogio  from B3Log.b3signallogger a 
			LEFT JOIN B3Log.b3signallogger b ON a.relogio=b.relogio AND b.asset = 'VALE3' 
			where  a.asset != 'BOVA11' AND a.asset like 'VALE%' AND a.strike != 0 
			AND a.relogio >= TO_TIMESTAMP(__dt_inicio,'YYYY-MM-DD HH24:MI:SS') 
			AND a.relogio <= TO_TIMESTAMP(__dt_fim,'YYYY-MM-DD HH24:MI:SS') 
			ORDER BY a.relogio ASC, a.data ASC, a.hora ASC ;
BEGIN
	IF (select _SQL_check_table_exists('B3Log', 'B3LogSignalUnique')) THEN
		DROP TABLE B3Log.B3LogSignalUnique;
	END IF;
	EXECUTE 'CREATE TABLE B3Log.B3LogSignalUnique(
		id BIGSERIAL PRIMARY KEY,
		data date,
		hora time, 
		asset varchar(12), 
		valor_ativo  NuMERIC(8,2), 
		preco_opcao  NuMERIC(8,2), 
		strike NuMERIC(8,2), 
		oferta_compra NuMERIC(8,2), 
		oferta_venda NuMERIC(8,2), 
		vencimento date, 
		desvio_padrao_3mses NUMERIC(42,40) NULL, 
		desvio_padrao_6mses NUMERIC(42,40) NULL,
		desvio_padrao_12mses NUMERIC(42,40) NULL,
		desvio_padrao_18mses NUMERIC(42,40) NULL,
		desvio_padrao_24mses NUMERIC(42,40) NULL,
		desvio_padrao_36mses NUMERIC(42,40) NULL,
		preco_blackshoes NUMERIC(8,2) NULL,
		relogio timestamp
	);';
	open raw_log_rows(_dt_inicio, _dt_fim);
	LOOP
		fetch raw_log_rows into raw_row;
		IF NOT FOUND THEN EXIT; END IF;
		row_number := row_number+1;
		IF NOT EXISTS (SELECT * FROM B3Log.B3LogSignalUnique WHERE
			data = raw_row.data AND
			hora = raw_row.hora AND
			asset =  raw_row.asset AND
			valor_ativo = raw_row.valor_ativo AND
			preco_opcao = raw_row.preco_opcao AND
			strike = raw_row.strike AND
			oferta_compra =  raw_row.oferta_compra AND
			oferta_venda = raw_row.oferta_venda AND
			vencimento = raw_row.vencimento) THEN
			row_number := row_number+1;
			RAISE NOTICE  'counter %', row_number;
			RAISE NOTICE  '% % % % % % % % % %', 
				raw_row.data,
				raw_row.hora,
				raw_row.asset,
				raw_row.valor_ativo,
				raw_row.preco_opcao,
				raw_row.strike,
				raw_row.oferta_compra,
				raw_row.oferta_venda,
				raw_row.vencimento,
				raw_row.relogio;
			INSERT INTO B3Log.B3LogSignalUnique (
				data,
				hora,
				asset,
				valor_ativo,
				preco_opcao,
				strike,
				oferta_compra,
				oferta_venda,
				vencimento,
				relogio
			) 
			VALUES (
				raw_row.data,
				raw_row.hora,
				raw_row.asset,
				raw_row.valor_ativo,
				raw_row.preco_opcao,
				raw_row.strike,
				raw_row.oferta_compra,
				raw_row.oferta_venda,
				raw_row.vencimento,
				raw_row.relogio
			); 		
		END IF;
		--RAISE NOTICE  'counter %', row_number;
	END LOOP;
END;
$$
LANGUAGE 'plpgsql' VOLATILE

--TEST
select _SQL_return_opcoes_com_valor_ativo_sync('VALE3', '2021-01-26 00:00:00', '2021-01-26 23:59:59');


select hora,
asset,
valor_ativo,
preco_opcao,
strike,
oferta_compra,
oferta_venda,
vencimento,
relogio from B3Log.B3LogSignalUnique order by asset ASC,relogio ASC, data DESC, hora DESC limit 1000;

Copy (
select data, 
hora,
asset,
valor_ativo,
preco_opcao,
strike,
oferta_compra,
oferta_venda,
vencimento,
relogio from B3Log.B3LogSignalUnique order by asset ASC,relogio ASC, data DESC, hora DESC) 
To '/tmp/vale3_derivativos_26-01-2021.csv' With CSV DELIMITER ';' HEADER;





select hora,
asset,
valor_ativo,
preco_opcao,
strike,
oferta_compra,
oferta_venda,
vencimento,
relogio from B3Log.B3LogSignalUnique order by relogio ASC, data DESC, hora DESC limit 100;

select min(relogio) from B3Log.B3LogSignalUnique;
--2021-01-26 09:45:01.0

select max(relogio) from B3Log.B3LogSignalUnique;
--2021-01-26 18:16:03.0

select asset, count(1) from B3Log.B3LogSignalUnique group by asset order by 2,1;

/*data,
hora,
asset,
valor_ativo,
preco_opcao,
strike,
oferta_compra,
oferta_venda,
vencimento,
relogio
*/


--------------------------------------------------------------------------------------------------------
--select  count(1) -- 326757
select  a.data, a.hora, a.asset, b.ultimo valor_ativo, a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.relogio   
from B3Log.b3signallogger a 
LEFT JOIN B3Log.b3signallogger b ON a.relogio=b.relogio AND b.asset = 'VALE3' 
where  a.asset != 'BOVA11' AND a.asset like 'VALE%' AND a.strike != 0 
AND a.relogio >= TO_TIMESTAMP('2021-01-26 09:45:01.0','YYYY-MM-DD HH24:MI:SS') 
AND a.relogio <= TO_TIMESTAMP('2021-01-26 18:16:03.0','YYYY-MM-DD HH24:MI:SS') 
limit 100

select count(1) from B3Log.b3logsignalunique
--42492


select * from B3Log.b3signallogger where validade is null




--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------


/**
 *   FUNCTION _SQL_populate_ativo_opcoes() 
 *	@autho: MC LOC
 *	@date: 2021-03-05
 */
DROP  FUNCTION _SQL_populate_ativo_opcoes() 

CREATE OR REPLACE FUNCTION _SQL_populate_ativo_opcoes() 
RETURNS void AS 
$$
DECLARE
	insert_stmt TEXT;
	row record;
	row_number int := 0 ;
	ativos_substr_rows CURSOR FOR
		-- ativos no log
		select asset, substring(asset, '[A-Z]+') from B3Log.B3SignalLogger  
		WHERE strike = 0
		group by asset
		order by asset;
BEGIN
	open ativos_substr_rows;
	/*IF (select _SQL_check_table_exists('B3Log', 'B3AitvosOpcoes')) THEN
		DROP TABLE B3Log.B3AitvosOpcoes;
	END IF;*/
	LOOP
		fetch ativos_substr_rows into row;
		IF NOT FOUND THEN EXIT; END IF;
		row_number := row_number+1;
		RAISE NOTICE  'asset % -> opcao substr %', row.asset, row.substring;
		EXECUTE 'INSERT INTO B3Log.B3AtivosOpcoes (ativo, opcao_ativo) ' ||
			'SELECT ' ||
			quote_literal(row.asset) || '  as ativo, ' ||
			'asset as opcao_ativo '||
			'FROM B3Log.B3SignalLogger  ' ||
			'WHERE strike != 0 AND asset LIKE ( '|| 
			quote_literal(row.substring||'%') || ') ' ||
			'group by asset order by ativo, asset; ';
		RAISE NOTICE  'total de ativos encontrados %', row_number;
	END LOOP;
	EXECUTE 'INSERT INTO B3Log.B3AtivosOpcoes (ativo, opcao_ativo) ' ||
			'SELECT asset  as ativo, null as opcao_ativo ' ||
			'FROM B3Log.B3SignalLogger  ' ||
			'WHERE strike == 0 ' ||
			'group by asset order by ativo ';
END;
$$
LANGUAGE 'plpgsql' VOLATILE

--TEST
select _SQL_populate_ativo_opcoes();

select ativo from B3Log.B3AtivosOpcoes
group by 1

--delete from B3Log.B3AtivosOpcoes;

select ativo, count(1) from B3Log.B3AtivosOpcoes group by ativo;





--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------




/**
 *   FUNCTION _black_shoes_get_d1(_asset varchar(12), volatilidade_historica NUMERIC(42,39),  strike NUMERIC(8,2), 
 * 	dt_vencimento date, risco NUMERIC(8,5), preco_asset NuMERIC(8,2))
 *	@autho: MC LOC
 *	@date: 2021-02-21
 */
--DROP FUNCTION _black_shoes_get_d1(_asset varchar(12), volatilidade_historica NUMERIC(42,39),  strike NUMERIC(8,2), _dt_vencimento date, risco NUMERIC(8,5), preco_asset NuMERIC(8,2))

CREATE OR REPLACE FUNCTION _black_shoes_get_d1(_asset varchar(12), volatilidade_historica NUMERIC(42,39),  strike NUMERIC(8,2), 
	_dt_vencimento date, risco NUMERIC(8,5), preco_asset NuMERIC(8,2)) 
RETURNS bool AS 
$$
DECLARE
	--stmt TEXT;
BEGIN
	RAISE NOTICE '_asset=%, _dt_inicio=%, _dt_fim=%',_asset, _dt_inicio, _dt_fim;
END;
$$
LANGUAGE 'plpgsql' VOLATILE
/***** F I M   FUNCTION _black_shoes_get_d1(_asset varchar(12), volatilidade_historica NUMERIC(42,39),  strike NUMERIC(8,2), 
	_dt_vencimento date, risco NUMERIC(8,5), preco_asset NuMERIC(8,2)) ************/



--TEST
select _black_shoes_get_d1(_asset varchar(12), volatilidade_historica NUMERIC(42,39),  strike NUMERIC(8,2), 
	_dt_vencimento date, SELLIC, B3SignalLogger.ultimo)


select * from B3Log.b3signallogger where asset = 'VALE3' limit 100

select * from B3Log.b3signallogger where asset != 'VALE3' AND asset != 'BOVA11' limit 100

-- PEGA TODAS OPCOES DO ATIVO VALE3
select data, hora, asset, ultimo as preco_opcao, strike, oferta_compra, oferta_venda, vencimento, relogio 
from B3Log.b3signallogger 
--LEFT JOIN B3Log.b3signallogger 
where asset != 'VALE3' AND asset != 'BOVA11'  AND asset like 'VALE%'
ORDER BY relogio DESC
LIMIT 100

-- PEGA TODOS PREÇOS DO ATIVO VALE3
select data, hora, asset, ultimo as preco_opcao, strike, oferta_compra, oferta_venda, vencimento, relogio 
from B3Log.b3signallogger 
--LEFT JOIN B3Log.b3signallogger 
where asset = 'VALE3' AND strike = 0
ORDER BY relogio DESC
LIMIT 100


-- PEGA TODOS PREÇOS DO ATIVO VALE3
select data, hora, asset, ultimo as preco_opcao, strike, oferta_compra, oferta_venda, vencimento, relogio 
from B3Log.b3signallogger 
--LEFT JOIN B3Log.b3signallogger 
where  asset != 'BOVA11' AND asset like 'VALE3' --AND strike != 0
AND relogio = '2021-01-26 09:45:02.0'
--AND relogio = '2021-01-26 11:56'
--ORDER BY data ASC, hora ASC
ORDER BY relogio ASC, data ASC, hora ASC
LIMIT 100

select a.data, a.hora, a.asset, b.ultimo valor_ativo, a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.relogio 
from B3Log.b3signallogger a
LEFT JOIN B3Log.b3signallogger b ON a.relogio=b.relogio AND b.asset = 'VALE3'
where  a.asset != 'BOVA11' AND a.asset like 'VALE%' AND a.strike != 0
--AND a.relogio = '2021-01-26 09:45:02.0'
AND a.relogio = '2021-01-26 11:56'CREATE OR REPLACE FUNCTION test_func(filepath text, xcol text, fillval text)
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE sql text;
BEGIN
  EXECUTE format($$ALTER TABLE my_table ALTER COLUMN %s SET DEFAULT '%s';$$, xcol, fillval);

  SELECT format($$COPY my_table(%s) FROM '%s' WITH (FORMAT CSV, DELIMITER '|', NULL 'NULL', HEADER true);$$
        , string_agg(quote_ident(attname), ','), filepath)
  INTO sql
  FROM   pg_attribute
  WHERE attrelid = 'my_table'::regclass
      AND attname != 'xtra_col'
      AND attnum > 0;
  EXECUTE sql;

  EXECUTE format($$ALTER TABLE my_table ALTER COLUMN %s DROP DEFAULT;$$, xcol);
END;
$func$;

SELECT test_func('/workdir/some_file.txt', 'xtra_col', 'b');

--ORDER BY data ASC, hora ASC
ORDER BY b.asset, a.relogio ASC, a.data ASC, a.hora ASC


-- ******************************************  QUERY PARA CRIACAO DE TABELA NORMALIZADA ********************************
--select count(1)
--select a.data, a.hora, a.asset,  a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.relogio 
select a.data, a.hora, a.asset, b.ultimo valor_ativo, a.ultimo as preco_opcao, a.strike, a.oferta_compra, a.oferta_venda, a.vencimento, a.relogio 
from B3Log.b3signallogger a
LEFT JOIN B3Log.b3signallogger b ON a.relogio=b.relogio AND b.asset = 'VALE3'
where  a.asset != 'BOVA11' AND a.asset like 'VALE%' AND a.strike != 0
--where  a.asset != 'BOVA11' AND a.asset like 'VALE3' --AND strike != 0
AND a.relogio >= '2021-01-26 09:45:02.0'
AND a.relogio <= '2021-01-26 11:56'
ORDER BY a.relogio ASC, a.data ASC, a.hora ASC
LIMIT 100
-- ******************************************  QUERY PARA CRIACAO DE TABELA NORMALIZADA ********************************

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

.
.
.
.
..
.
.
.
.
.
.
.
..
.






-- **************************** checkVolatilidadeHistoricaExists *********************************
CREATE OR REPLACE FUNCTION checkVolatilidadeHistoricaExists(varchar(12)) 
RETURNS TABLE(asset varchar(12), preco_blck_shoes numeric(11,2)) AS $$
DECLARE
	_asset ALIAS FOR $1;
	_count_vol_hist int;
BEGIN
	 _count_vol_hist := (SELECT COUNT(1) FROM B3Log.volatilidade_historica WHERE CODNEG = _asset);
	 IF _count_vol_hist = 0 THEN
	 	RAISE NOTICE 'SEM REGISTROS "volatilidade_historica %": hidratando tabela', _asset;
	 ELSE
	 	RAISE NOTICE 'TOTAL REGISTROS em "volatilidade_historica para %": %', _asset, _count_vol_hist;
	 END IF;
      RAISE NOTICE 'Black and Shoes for ASSET: %', _asset;
      RETURN QUERY SELECT $1, 12.00;
END;
$$
LANGUAGE 'plpgsql' VOLATILE
-- ************ F I M **************** checkVolatilidadeHistoricaExists *********************************

select
*
from checkVolatilidadeHistoricaExists('BOVA11')




-- **************************** populateVolatilidadeHistorica *********************************
CREATE OR REPLACE FUNCTION populateVolatilidadeHistorica(varchar(12)) 
RETURNS TABLE(asset varchar(12), preco_blck_shoes numeric(11,2)) AS $$
DECLARE
	_asset ALIAS FOR $1;
	_count_vol_hist int;
	_table_vol_hist_exsits bool;
BEGIN
	_table_vol_hist_exsits := (SELECT EXISTS (
	   SELECT FROM information_schema.tables 
	   WHERE  table_schema = 'B3Log'
	   AND    table_name   = 'volatilidade_historica'
	   ));
	 IF _table_vol_hist_exsits =  TRUE THEN
	 	_count_vol_hist := (SELECT COUNT(1) FROM B3Log.volatilidade_historica WHERE CODNEG = _asset);
		 IF _count_vol_hist > 0 THEN
		 	RAISE INFO 'volatilidade_historica for ASSET: % - % total registros', _asset, _count_vol_hist;
		 END IF;
	 END IF;
     RAISE INFO 'Black and Shoes for ASSET: %', _asset;
     RETURN QUERY SELECT $1, 12.00;
END;
$$
LANGUAGE 'plpgsql' VOLATILE
-- ************ F I M **************** populateVolatilidadeHistorica *********************************

DROP FUNCTION _SQL_populateVolatilidadeHistorica(varchar(12),date,date) ;

CREATE OR REPLACE FUNCTION _SQL_populateVolatilidadeHistorica(_asset varchar(12),  _dt_inicio date, _dt_fim date) 
RETURNS bool AS 
$$
DECLARE
	stmt TEXT;
BEGIN
	RAISE NOTICE '_asset=%, _dt_inicio=%, _dt_fim=%',_asset, _dt_inicio, _dt_fim;
	IF (SELECT count(*) FROM pg_prepared_statements WHERE name ilike 'volatilidade') > 0 THEN
	    DEALLOCATE volatilidade;
	END IF;
	PREPARE volatilidade(varchar(12), date , date) AS 
		WITH VARIACAO  AS (
			SELECT 
				CODNEG, DATA_PREGAO, PREULT
			FROM B3Log.B3Historical 
			where CODNEG = $1 AND data_pregao >= $2 AND data_pregao <= $3 order by DATA_PREGAO  DESC,CODNEG
		) 
		SELECT
			CODNEG, DATA_PREGAO, PREULT,
			LAG(PREULT,-1) OVER (
				ORDER BY DATA_PREGAO DESC
			) ANTERIOR,
			PREULT/(LAG(PREULT,-1) OVER (
				ORDER BY DATA_PREGAO DESC
			)) as VARIACAO,
			log(
				PREULT/(LAG(PREULT,-1) OVER (
					ORDER BY DATA_PREGAO DESC
				)) 
			) as LOG_VARIACAO,
			now() as update_at
		FROM
			VARIACAO ;
	stmt = format('CREATE TABLE B3Log.volatilidade_historica AS EXECUTE volatilidade(%L, %L, %L);', _asset, _dt_inicio, _dt_fim);
	EXECUTE stmt;
	RETURN TRUE;
	DEALLOCATE volatilidade;
END;
$$
LANGUAGE 'plpgsql' VOLATILE

select * from 	 B3Log.B3Historical 
where CODNEG = 'BOVA11'

select _SQL_populateVolatilidadeHistorica('VALE3', '2020-01-01', '2021-03-30')


select * from B3Log.volatilidade_historica

-- ************ F I M **************** loadFromCSV *********************************



/**
 *   FUNCTION load_from_csv(_asset varchar(12), volatilidade_historica NUMERIC(42,39),  strike NUMERIC(8,2), 
 * 	dt_vencimento date, risco NUMERIC(8,5), preco_asset NuMERIC(8,2))
 *	@autho: MC LOC
 *	@date: 2021-05-15
 */
CREATE OR REPLACE FUNCTION load_from_csv(filepath text, lot_name VARCHAR(70), lot_id BIGINT)
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE sql text;
BEGIN
  EXECUTE format($$ALTER TABLE B3Log.B3SignalLoggerRaw ALTER COLUMN %s SET DEFAULT '%s';$$, 'lot_name', lot_name);
  EXECUTE format($$ALTER TABLE B3Log.B3SignalLoggerRaw ALTER COLUMN %s SET DEFAULT '%s';$$, 'lot_id', lot_id);
  SELECT format($$COPY B3Log.B3SignalLoggerRaw(%s) FROM '%s' WITH (FORMAT csv, HEADER true, DELIMITER ',');$$
        , string_agg(quote_ident('lot_name'), ','), string_agg(quote_ident('lot_id'), ',') filepath)
  INTO sql
  FROM   pg_attribute
  WHERE attrelid = 'B3Log.B3SignalLoggerRaw'::regclass
      AND attname != 'lot_name'
      AND attnum > 0;
  EXECUTE sql;
END;
$func$;

SELECT test_func('/workdir/some_file.txt', 'xtra_col', 'b');

 
-- ************ F I M **************** loadFromCSV *********************************














select current_schemas(false)

  SELECT  c.*
      FROM   pg_catalog.pg_class c
      JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE  n.nspname = 'B3Log'CREATE OR REPLACE FUNCTION test_func(filepath text, xcol text, fillval text)
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE sql text;
BEGIN
  EXECUTE format($$ALTER TABLE my_table ALTER COLUMN %s SET DEFAULT '%s';$$, xcol, fillval);

  SELECT format($$COPY my_table(%s) FROM '%s' WITH (FORMAT CSV, DELIMITER '|', NULL 'NULL', HEADER true);$$
        , string_agg(quote_ident(attname), ','), filepath)
  INTO sql
  FROM   pg_attribute
  WHERE attrelid = 'my_table'::regclass
      AND attname != 'xtra_col'
      AND attnum > 0;
  EXECUTE sql;

  EXECUTE format($$ALTER TABLE my_table ALTER COLUMN %s DROP DEFAULT;$$, xcol);
END;
$func$;

SELECT test_func('/workdir/some_file.txt', 'xtra_col', 'b');

      AND    n.nspname NOT LIKE 'pg_%'  -- exclude system schemas!
      AND    c.relname = 'volatilidade_historica'
      AND    c.relkind = 'r';         -- you probably need this

-- *************************************************************

PREPARE volatilidade(varchar(12), date) AS 
	WITH VARIACAO  AS (
		SELECT 
			CODNEG, DATA_PREGAO, PREULT
		FROM B3Log.B3Historical 
		where codneg = $1 AND data_pregao < $2 order by CODNEG,DATA_PREGAO  DESC
	) 
	SELECT
	CODNEG, DATA_PREGAO, PREULT,
		LAG(PREULT,-1) OVER (
			ORDER BY DATA_PREGAO DESC
		) ANTERIOR,
		PREULT/(LAG(PREULT,-11) OVER (
			ORDER BY DATA_PREGAO DESC
		)) as VARIACAO,
		log(
			PREULT/(LAG(PREULT,-11) OVER (
				ORDER BY DATA_PREGAO DESC
			)) 
		) as LOG_VARIACAO
	FROM
		VARIACAO ;
	
	DROP TABLE B3Log.volatilidade_historica 
	
	CREATE TABLE B3Log.volatilidade_historica AS
	EXECUTE volatilidade('VALE3', '2021-03-30');

SELECT to_regclass('B3Log.ativos_em_foco');
SELECT 'B3Log.ativos_em_foco'::regclass


PREPARE volatilidade(varchar(12), date) AS 
WITH VARIACAO  AS (
	SELECT 
		CODNEG, DATA_PREGAO, PREULT
	FROM B3Log.B3Historical 
	where codneg = $1 AND data_pregao < $2 order by CODNEG,DATA_PREGAO  DESC
) 
SELECT
CODNEG, DATA_PREGAO, PREULT,
	LAG(PREULT,-1) OVER (
		ORDER BY DATA_PREGAO DESC
	) ANTERIOR,
	PREULT/(LAG(PREULT,-11) OVER (
		ORDER BY DATA_PREGAO DESC
	)) as VARIACAO,
	log(
		PREULT/(LAG(PREULT,-11) OVER (
			ORDER BY DATA_PREGAO DESC
		)) 
	) as LOG_VARIACAO,
	now()
FROM
	VARIACAO ;

--DROP TABLE B3Log.volatilidade_historica 

CREATE TABLE B3Log.volatilidade_historica AS
EXECUTE volatilidade('VALE3', '2021-03-30');









PREPARE volatilidade(varchar(12), date) AS 
WITH VARIACAO  AS (
	SELECT 
		CODNEG, DATA_PREGAO, PREULT
	FROM B3Log.B3Historical 
	where codneg = $1 AND data_pregao < $2 order by 1,2  DESC
) 
SELECT
CODNEG, DATA_PREGAO, PREULT,
	LAG(PREULT,-1) OVER (
		ORDER BY DATA_PREGAO DESC
	) ANTERIOR,
	PREULT/(LAG(PREULT,-11) OVER (
		ORDER BY DATA_PREGAO DESC
	)) as VARIACAO,
	log(
		PREULT/(LAG(PREULT,-11) OVER (
			ORDER BY DATA_PREGAO DESC
		)) 
	) as LOG_VARIACAO
FROM
	VARIACAO ;

--DROP TABLE B3Log.volatilidade_historica 

CREATE TABLE B3Log.volatilidade_historica AS
EXECUTE volatilidade('VALE3', '2021-03-30');


WITH vol_hist as (
	EXECUTE volatilidade('VALE3', '2021-03-30');
)
INSERT INTO B3Log.volatilidade_historica SELECT * FROM vol_hist