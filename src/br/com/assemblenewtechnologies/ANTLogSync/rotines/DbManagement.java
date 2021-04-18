package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbManagement extends AbstractRotine  {
	private static Logger LOGGER = LoggerFactory.getLogger(DbManagement.class);
	
	public void db_reindex_table_raw() throws Exception{
		LOGGER.info("[DB_MANAGEMENT] db_reindex_table_raw...");
	}
	public void reindex_table_normalized() throws Exception{
		LOGGER.info("[DB_MANAGEMENT] reindex_table_normalized...");
	}	
	public void db_purge_raw_table() throws Exception{
		LOGGER.info("[DB_MANAGEMENT] db_purge_raw_table...");
	}
	public void db_reindex_all_tables() throws Exception{
		LOGGER.info("[DB_MANAGEMENT] db_reindex_all_tables...");
	}
	@Override
	public void handler_start() throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void handler_finish() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	
}
