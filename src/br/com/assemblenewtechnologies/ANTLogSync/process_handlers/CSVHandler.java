package br.com.assemblenewtechnologies.ANTLogSync.process_handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.controller.MainController;
import br.com.assemblenewtechnologies.ANTLogSync.rotines.Csv;

public class CSVHandler implements Runnable {

	private static Logger LOGGER = LoggerFactory.getLogger(MainController.class);
	private static GlobalProperties globalProperties = new GlobalProperties();
	private Csv csv_rotine;

	public CSVHandler(Csv csv) {
		csv_rotine = csv;
	}

	@Override
	public void run() {
		try {
			LOGGER.info("CSV Thread Running");
			while (true) {
				try {
					csv_rotine.csv_check_for_files();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Thread.sleep(2000);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
