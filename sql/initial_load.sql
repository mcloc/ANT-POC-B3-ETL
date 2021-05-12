-- ALL STRUCTURE DATA 

-- delete from Intellect.processment_rotines 
--delete from Iupdate Intellect.processment_rotines  set active_status = false;ntellect.processment_rotines
INSERT INTO Intellect.processment_rotines (name, description, active_status, processment_seq, processment_group, processment_mode, new_thread, thread_name, created_at) VALUES 
('startup', 'Initial controller startup rotine', true, 01, 'core', 'batch_process', false, null, now()),
('handler_start', 'Start CSV handler rotine', true, 10, 'csv', 'batch_process', true, 'CSV_HANDLER', now()),
('db_reindex_table_raw', 'DB Management reindex raw B3Log table before ETL rotine', true, 13, 'db_management', 'batch_process', false, null,  now()),
('etl0_sanity_check', 'ETL Level-0 perform INFO sanity check on data received rotine', true, 20, 'etl', 'batch_process', false,null,   now()),
('etl1_populate_assets', 'ETL Level-1 get all real assets and populate assets table rotine', true, 30, 'etl', 'batch_process', false, null,  now()),
('etl1_normalization', 'ETL Level-1 normalization rotine', true, 31, 'etl', 'batch_process', false, null,   now()),
('reindex_table_normalized', 'DB Management reindex normalized B3Log table after ETL normalization rotine', true, 32, 'db_management', 'batch_process',  false, null, now()),
('etl2_preco_medio', 'ETL Level-2 calculate preco medio (volume/negocios rotine)', true, 40, 'etl', 'batch_process', false, null,  now()),
('etl2_black_scholes', 'ETL Level-2 calculate Black and Scholes rotine', true, 41, 'etl', 'batch_process', false, null,  now()),
('db_purge_raw_table', 'DB Management purge RAW Load TABLE rotine', true, 900, 'db_management', 'batch_process',  false,null,  now()),
('db_reindex_all_tables', 'DB Management reindex ALL TABLES rotine', true, 910, 'db_management', 'batch_process', false, null,  now()),
--
('startup', 'Initial controller startup rotine', true, 00, 'core', 'realtime',  false, null, now()),
('csv_wating', 'Waiting CSV files rotine', true, 10, 'csv', 'realtime',  false, 'CSV_HANDLER', now()),
('csv_load_start', 'Load CSV files rotine', true, 11, 'csv', 'realtime',  false, null, now()),
('csv_archive_start', 'Archive CSV files rotine', true, 12, 'csv', 'realtime',  false, null, now()),
('db_reindex_table_raw', 'DB Management reindex raw B3Log table before ETL rotine', true, 13, 'db_management', 'realtime',  false, null, now()),
('etl0_sanity_check', 'ETL Level-0 perform INFO sanity check on data received rotine', true, 20, 'etl', 'realtime', false, null,  now()),
('etl1_populate_assets', 'ETL Level-1 get all real assets and populate assets table rotine', true, 30, 'etl', 'realtime',  false, null, now()),
('etl1_normalization', 'ETL Level-1 normalization rotine', true, 31, 'etl', 'realtime',  false,null,  now()),
('reindex_table_normalized', 'DB Management reindex normalized B3Log table after ETL normalization rotine', true, 32, 'db_management', 'realtime',  false, null, now()),
('etl2_preco_medio', 'ETL Level-2 calculate preco medio (volume/negocios rotine)', true, 40, 'etl', 'realtime',  false, null, now()),
('etl2_black_scholes', 'ETL Level-2 calculate Black and Scholes rotine', true, 41, 'etl', 'realtime',  false, null, now()),
('db_purge_raw_table', 'DB Management purge RAW Load TABLE rotine', true, 900, 'db_management', 'realtime',  false, null, now()),
('db_reindex_all_tables', 'DB Management reindex ALL TABLES rotine', true, 910, 'db_management', 'realtime',  false, null, now());
--  select * from Intellect.processment_rotines 


--delete from Intellect.processment_errors
INSERT INTO Intellect.processment_errors (name, description,error_code, processment_group, created_at, updated_at) VALUES 
('startup_error', 'Initial controller startup error', 001, 'core', now(), now()),
('runtime_error', 'Initial controller startup error', 002, 'core', now(), now()),
('get_machinestate_error', 'Error on localize last machinestate', 010, 'core', now(), now()),
('recover_machinestate_error', 'Error on recovery from last machinestate', 020, 'core', now(), now()),
('csv_load_path_error', 'Error on locate csv_load path', 100, 'csv',  now(), now()),
('csv_load_permission_error', 'Error on permission on file or direcoty csv_load path', 101, 'csv',now(), now()),
('csv_integrety_data_error', 'CSV Data Integreity error', 102, 'csv', now(), now()),
('csv_archive_path_error', 'Error on locate csv_archive path', 103, 'csv', now(), now()),
('csv_archive_permission_error', 'Error on permission on file or direcoty csv_archive path', 104, 'csv', now(), now()),
('csv_archive_zip_error', 'Error on zipping data on direcoty csv_archive path', 105, 'csv', now(), now()),
('db_connect_error', 'Error connectiong to database', 200, 'db_management', now(), now()),
('db_exception_error', 'Database exception error', 201, 'db_management', now(), now()),
('etl_sanity_dropstep_error', 'ETL dropstep on time sanity check error', 300, 'etl', now(), now()),
('etl_sanity_info_error', 'ETL Infomation on loaded DATA sanity check error', 301, 'etl', now(), now()),
('etl_populate_assets_error', 'ETL error on populate/infere Assets to load', 302, 'etl', now(), now()),
('etl_normalization_error', 'ETL error on normalize data loaded', 303, 'etl', now(), now()),
('etl_preco_medio_error', 'ETL error on calculate preco_medio', 304, 'etl', now(), now()),
('etl_blackscholes_error', 'ETL error on calculate black and scholes price', 305, 'etl', now(), now());
--  select * from Intellect.processment_errors  order by error_code

update Intellect.processment_rotines  set active_status = false;

update Intellect.processment_rotines  set active_status = true;

update Intellect.processment_rotines  set active_status = true where processment_seq = 10;

select * from Intellect.processment_rotines order by id 
