-- **************  QUERIES DE ANALISE - analise.sql  ***************************


--burrro stddev(coluna) from lista de log(N) karalho

-- create tmp table from SELECT
--CREATE TEMP TABLE tbl AS
--SELECT * FROM tbl WHERE ... ;


select which, min(val), max(val), stddev(val), avg(val)
from t, lateral
     (values ('col1', col1), ('col2', col2), . . . 
     ) v(which, val)
group by which;





CREATE TEMP TABLE B3Log.tmp_volatilidade (
		CODNEG VARCHAR(12) NOT NULL,
		DATA_PREGAO DATE NOT NULL,
		PREULT NUMERIC(11,2) NOT NULL,
   		VARIACAO NUMERIC(25,19) DEFAULT NULL,
   		LOG_VARIACAO NUMERIC(25,19) DEFAULT NULL
	) AS select codneg, data_pregao, PREULT  PREULT from B3Log.B3Historical 
		where codneg = 'VALE3' AND data_pregao < '2020-03-30' order by 1 DESC;

WITH VARIACAO  AS (
	SELECT 
		CODNEG, DATA_PREGAO, PREULT
	FROM B3Log.B3Historical 
	where codneg = 'VALE3' AND data_pregao < '2020-03-30' order by 1,2  DESC
) 
SELECT
CODNEG, DATA_PREGAO, PREULT,
	LAG(PREULT,1) OVER (
		ORDER BY DATA_PREGAO DESC
	) ANTERIOR,
	PREULT/(LAG(PREULT,1) OVER (
		ORDER BY DATA_PREGAO DESC
	)) as VARIACAO,
	log(
		PREULT/(LAG(PREULT,1) OVER (
			ORDER BY DATA_PREGAO DESC
		)) 
	) as LOG_VARIACAO
FROM
	VARIACAO ;


-- **************************************** PREPARE STATEMENT AND CREATE TABLE VOLATILIDADE *********************
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

--DELETE FROM B3Log.volatilidade_historica
-- *************  F I M ******************* PREPARE STATEMENT AND CREATE TABLE VOLATILIDADE *********************

SELECT * FROM B3Log.volatilidade_historica
-- **************************************** DESVIO PADRAO DA VOLATILIDADE HISTORICA         *********************
select codneg,stddev(log_variacao) from B3Log.volatilidade_historica group by codneg
-- *************** F I M ************************* DESVIO PADRAO DA VOLATILIDADE HISTORICA         *********************

select count(1) FROM B3Log.volatilidade_historica;


PREPARE total_dias_neogciados(varchar(12)) as (
	select count(1) FROM B3Log.volatilidade_historica where codneg = $1
)
EXECUTE total_dias_neogciados('VALE3');
--278

-- ******************* B L A C K    A N D    S H O E S ******************************
select stddev(log_variacao) * sqrt(278) as desvio_padrao from B3Log.volatilidade_historica group by codneg

SELECT  *  FROM B3lOG.b3signallogger WHERE asset like 'VALE%' AND   strike != 0 group by 1 order by 1 limit 100;

SELECT  asset, data, count(1)  FROM B3lOG.b3signallogger WHERE asset = 'VALE3'  group by asset,data order by data  DESC  limit 100

-- 1 preço ativok
-- 2 strike
-- 3 volatilidade historica
-- 4 Risko (selic + cds)
-- 5 tempo em anos para o vencimento
/********************************************************************
::::: PARA CALL ::::::

BlackShores Preço Opção = PRECO_ATIVO * DIST_NORMAL(d1) - (STRIKE * exp(-rISK * TEMPO) * DIST_NORMAL(d2)

ONDE 

d1 =  	(LOG ( PRECO_ATIVO / STRIKE) + ((RISKO + VOLATILIDADE²)/2) * TEMPO ) / (VOLATILIDADE 8 sqrt(TEMPO)

D2 = d1 - (VOLATILIDADE * sqrt(TEMPO)


::::: PARA PUT::::::

BlackShores Preço Opção = (STRIKE * exp(-rISK * TEMPO) * DIST_NORMAL(-d2) - PRECO_ATIVO * DIST_NORMA(-d1)



********************************************************************/





CREATE OR REPLACE FUNCTION B3Log.blackShoes(_asset varchar(12))
	RETURNS TABLE (asset varchar(12), preco_blckshoes numerica(11,2))
	LANGUAGE plpgsql AS
	$func$
	DECLARE
	   _sensors text := 'col1::text, col2::text';  -- cast each col to text
	   _type    text := 'foo';
	BEGIN
	   RETURN QUERY EXECUTE '
	      SELECT datahora, ' || _sensors || '
	      FROM   ' || quote_ident(_type) || '
	      WHERE  id = $1
	      ORDER  BY datahora'
   USING  _id;

END
$func$;






	
  $BODY$
     DECLARE
        --cursor
        reg myScheme.telephone%ROWTYPE;
  BEGIN
           --Loop on all table phones
        FOR reg in
                   SELECT phone.number
                   FROM myScheme.telephone phone
           LOOP
              RETURN NEXT reg;
        END LOOP;
        RETURN;
     END;
     $BODY$
    LANGUAGE plpgsql VOLATILE;




-- ******** F  I  M  *********** B L A C K    A N D    S H O E S ******************************



SELECT DISTINCT CODNEG FROM B3Log.B3Historical group by CODNEG order by 1
SELECT CODNEG FROM B3Log.B3Historical group by CODNEG order by 1
SELECT CODNEG, count(1) FROM B3Log.B3Historical group by CODNEG order by 1
-- NO PUEDE AGREGAR AGREGACAO CLARO -- SELECT sum(count(1)) FROM B3Log.B3Historical group by CODNEG order by 1

SELECT CODNEG,count(CODNEG) as total
FROM B3Log.B3Historical 
group by ROLLUP(CODNEG )
order by CODNEG;

WITH ativos as (
SELECT DISTINCT CODNEG FROM B3Log.B3Historical group by CODNEG order by CODNEG
)
SELECT CODNEG FROM ativos --ROLLUP((money,requests))



-- **************************************** PREPARE STATEMENT AND CREATE TABLE MAIOR LIQUIDEZ *********************
DEALLOCATE ativos30_maior_liquidez;

PREPARE ativos30_maior_liquidez(int) AS 
	SELECT codneg, sum(voltot)as volume_total, sum(totneg) as total_negociacoes, count(1) total_dias_negociados
	FROM B3Log.B3Historical  
	WHERE voltot > 0 
	group by (codneg) 
	order by 2 DESC,3 DESC, 4 DESC, codneg ASC
	limit $1;

CREATE TABLE ativos30mais_liquidos AS
EXECUTE ativos30_maior_liquidez(40);

select * from ativos30mais_liquidos


select * from ativos30mais_liquidos
-- ******************** F I M ************* PREPARE STATEMENT AND CREATE TABLE MAIOR LIQUIDEZ *********************






-- ******************************** FUNCTION TO EXECUTE  volatilidade FOREACH 30 mais liquidez ativos *********************
CREATE OR REPLACE FUNCTION getVolatilidade30Mais() RETURNS SETOF foo AS
$BODY$
DECLARE
    r foo%rowtype;
BEGIN
  	EXECUTE ativos30_maior_liquidez(30);
	LOOP
     -- can do some processing here
     RETURN NEXT r; -- return current row of SELECT
	END LOOP;
	RETURN;
END
$BODY$
LANGUAGE 'plpgsql' ;
-- *************** F I M ******* FUNCTION TO EXECUTE  volatilidade FOREACH 30 mais liquidez ativos *********************


-- hummmmmm Linha em Coluna,,,, tem de ir pro PL e loop
select which, min(val), max(val), stddev(val), avg(val)
from t, lateral
     (values ('col1', col1), ('col2', col2), . . . 
     ) v(which, val)
group by which;
	

/*
CREATE FUNCTION sum(int[]) RETURNS int8 AS $$
DECLARE
  s int8 := 0;
  x int;
BEGIN
  FOREACH x IN ARRAY $1
  LOOP
    s := s + x;
  END LOOP;
  RETURN s;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION scan_rows(int[]) RETURNS void AS $$
DECLARE
  x int[];
BEGIN
  FOREACH x SLICE 1 IN ARRAY $1
  LOOP
    RAISE NOTICE 'row = %', x;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT scan_rows(ARRAY[[1,2,3],[4,5,6],[7,8,9],[10,11,12]]);

NOTICE:  row = {1,2,3}
NOTICE:  row = {4,5,6}
NOTICE:  row = {7,8,9}
NOTICE:  row = {10,11,12}



[ <<label>> ]
[ DECLARE
    declarations ]
BEGIN
    statements
EXCEPTION
    WHEN condition [ OR condition ... ] THEN
        handler_statements
    [ WHEN condition [ OR condition ... ] THEN
          handler_statements
      ... ]
END;

INSERT INTO mytab(firstname, lastname) VALUES('Tom', 'Jones');
BEGIN
    UPDATE mytab SET firstname = 'Joe' WHERE lastname = 'Jones';
    x := x + 1;
    y := x / 0;
EXCEPTION
    WHEN division_by_zero THEN
        RAISE NOTICE 'caught division_by_zero';
        RETURN x;
END;


*************************************************
CREATE TABLE db (a INT PRIMARY KEY, b TEXT);

CREATE FUNCTION merge_db(key INT, data TEXT) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE db SET b = data WHERE a = key;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO db(a,b) VALUES (key, data);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

SELECT merge_db(1, 'david');
SELECT merge_db(1, 'dennis');
*****************************************************************


DECLARE
  text_var1 text;
  text_var2 text;
  text_var3 text;
BEGIN
  -- some processing which might cause an exception
  ...
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS text_var1 = MESSAGE_TEXT,
                          text_var2 = PG_EXCEPTION_DETAIL,
                          text_var3 = PG_EXCEPTION_HINT;
END;


]*/


-- *****************************************************************
-- Create table VOLATILIDADE
-- create variation temp table with D1/D2 D2/D3 etc
-- then get the log of each variation
-- the create Standard DEviation of all periods
CREATE OR REPLACE FUNCTION compute_volatilidade (_CODNEG varchar)
  RETURNS void AS
$func$
BEGIN    
	CREATE TEMP TABLE tmp_volatilidade (
		CODNEG VARCHAR(12) NOT NULL,
		DATA_PREGAO DATE NOT NULL,
		PREULT NUMERIC(11,2) NOT NULL,
   		VARIACAO NUMERIC(25,19) DEFAULT NULL,
   		LOG_VARIACAO NUMERIC(25,19) DEFAULT NULL,
   	
	)ON COMMIT DROP AS
	select codneg, data_pregao, PREULT  PREULT from B3Log.B3Historical 
		where codneg = 'VALE3' AND data_pregao < '2020-03-30' order by 1 DESC

		

   DELETE FROM pref_scores p
   USING  tmp_gids t
   WHERE  p.gid = t.gid;

   DELETE FROM pref_games p
   USING  tmp_gids t
   WHERE  p.gid = t.gid;

   -- more deletes ...    
END
$func$ LANGUAGE plpgsql;

