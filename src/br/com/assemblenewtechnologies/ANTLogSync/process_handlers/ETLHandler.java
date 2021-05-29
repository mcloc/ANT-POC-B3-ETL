package br.com.assemblenewtechnologies.ANTLogSync.process_handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.rotines.Etl;

public class ETLHandler implements Runnable {

	private static Logger LOGGER = LoggerFactory.getLogger(ETLHandler.class);
	private Etl etl_rotine;

	public ETLHandler(Etl etl) throws Exception {
		etl_rotine = etl;
	}

	@Override
	public void run() {
		try {
			LOGGER.info("[ETL Thread] Starting...");
			while (true) {
				try {
					etl_rotine.setStartTime();
					etl_rotine.etl1_populate_assets();
					etl_rotine.etl1_normalization();
					etl_rotine.setExecuting(false);
				} catch (Exception e) {
					LOGGER.error("[ETL Thread] Error");
					LOGGER.error(e.getMessage());
					DBConnectionHelper.closeETLConn();
					e.printStackTrace();
				}
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}

	}

}
