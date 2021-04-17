package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Csv extends Rotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Csv.class);
	
	public void csv_wating() throws Exception{
		LOGGER.info("[CSV] csv_wating...");
	}
	public void csv_load_start() throws Exception{
		LOGGER.info("[CSV] csv_load_start...");
	}
	public void csv_archive_start() throws Exception{
		LOGGER.info("[CSV] csv_archive_start...");
	}
}
