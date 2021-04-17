package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Etl extends Rotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Etl.class);
	
	public void etl0_sanity_check() throws Exception{
		LOGGER.info("[ETL] etl0_sanity_check...");
	}
	public void etl1_populate_assets() throws Exception{
		LOGGER.info("[ETL]etl1_populate_assets ...");
	}
	public void etl1_normalization() throws Exception{
		LOGGER.info("[ETL] etl1_normalization...");
	}
	public void etl2_preco_medio() throws Exception{
		LOGGER.info("[ETL] etl2_preco_medio...");
	}
	public void etl2_black_scholes() throws Exception{
		LOGGER.info("[ETL]etl2_black_scholes ...");
	}
}
