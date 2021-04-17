package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbManagement  {
	private static Logger LOGGER = LoggerFactory.getLogger(DbManagement.class);
	
	public void db_reindex_table_raw() throws Exception{
		LOGGER.info("[DB_MANAGEMENT] db_reindex_table_raw...");
	}
	public void db_purge_raw_table() throws Exception{
		LOGGER.info("[DB_MANAGEMENT] db_purge_raw_table...");
	}
	public void db_reindex_all_tables() throws Exception{
		LOGGER.info("[DB_MANAGEMENT] db_reindex_all_tables...");
	}
}
