package br.com.assemblenewtechnologies.ANTLogSync.process_handlers;

import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.model.CsvLoadLot;
import br.com.assemblenewtechnologies.ANTLogSync.model.CsvLoadRegistry;
import br.com.assemblenewtechnologies.ANTLogSync.rotines.Csv;

public class CSVHandler implements Runnable {

	private static Logger LOGGER = LoggerFactory.getLogger(CSVHandler.class);
	private Csv csv_rotine;

	public CSVHandler(Csv csv) throws Exception {
		csv_rotine = csv;
		try {
			if (csv_rotine.getConnection() == null || csv_rotine.getConnection().isClosed()) {
				csv_rotine.setConnection(DBConnectionHelper.getNewConn());
				csv_rotine.getConnection().setAutoCommit(true);
				CsvLoadLot.set_connection(csv_rotine.getConnection());
				CsvLoadRegistry.set_connection(csv_rotine.getConnection());
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
			throw e;
		}

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
