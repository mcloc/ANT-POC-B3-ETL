package br.com.assemblenewtechnologies.ANTLogSync.controller;

import java.sql.Connection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnection;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentRotine;

public class MainController {
	private static Logger LOGGER = LoggerFactory.getLogger(MainController.class);
	private static long start_time;
	private static long end_time;
	private static long timer1;
	private static long timer2;
	private static long timer3;
	private static long timer4;
	private static long timer5;
	private static ControllerData controller_data;
	private static Map<Integer, ProcessmentRotine> processment_map;
	
	
	public static void main(String[] args) {
		start_time = System.currentTimeMillis();
		LOGGER.info("Initializing ANTController...");
		
		try {
			controller_data = new ControllerData();
			controller_data.closeConnection();
			processment_map = controller_data.getProcessment_rotines();
			LOGGER.info("Processments:");
			for (Integer processment_seq: processment_map.keySet()) {
				LOGGER.info("processment_seq: " + processment_seq);
				LOGGER.info("processment_name: " + processment_map.get(processment_seq).getName());
				LOGGER.info("processment_group: " + processment_map.get(processment_seq).getProcessment_group());
				LOGGER.info("processment_mode: " + processment_map.get(processment_seq).getProcessment_mode());
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
		
		
		
		end_time = System.currentTimeMillis() - start_time;
		LOGGER.info("ANTController execution time: "  + end_time);
	}

}
