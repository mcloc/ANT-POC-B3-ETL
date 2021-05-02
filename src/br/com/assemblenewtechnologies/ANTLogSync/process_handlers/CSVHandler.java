package br.com.assemblenewtechnologies.ANTLogSync.process_handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.rotines.Csv;

public class CSVHandler implements Runnable {

	private static Logger LOGGER = LoggerFactory.getLogger(CSVHandler.class);
	private Csv csv_rotine;

	public CSVHandler(Csv csv) {
		csv_rotine = csv;
	}

	@Override
	public void run() {
		try {
			LOGGER.info("[CSV Thread] Starting...");
			while (true) {
				try {
					csv_rotine.setStartTime();
					csv_rotine.csv_check_for_files();
					csv_rotine.setExecuting(false);
				} catch (Exception e) {
					LOGGER.error("[CSV Thread] Error");
					LOGGER.error(e.getMessage());
					e.printStackTrace();
				}
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}

	}

}
