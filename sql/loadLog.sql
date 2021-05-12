-- Heacy Machinery Industries Ltda.
-- B3 Analisys
-- @author: MC LOC




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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 26-01-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 27-01-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 28-01-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 29-01-2021.csv'
DELIMITER ';'  CSV HEADER;


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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 02-02-2021.csv'
DELIMITER ';'  CSV HEADER;



-- os files abaixo estao com relogio bixado mm/dd/yyyy
/*
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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 03-02-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 04-02-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 05-02-2021.csv'
DELIMITER ';'  CSV HEADER;


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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 08-02-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 09-02-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 10-02-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 11-02-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 12-02-2021.csv'
DELIMITER ';'  CSV HEADER;

-- FIM DO LOAD CSV Total1-UTF8.csv*/


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
) FROM  '/home/mcloc/Documents/ANT-resources/communications/tmp/BOVA11 LOG 11-02-2021.csv'
DELIMITER ';'  CSV HEADER;

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
) FROM  '/home/mcloc/Downloads/13/RTD_20210401/RTD_20210401150143.txt'
DELIMITER ','  CSV HEADER;