package br.com.assemblenewtechnologies.ANTLogSync.controller;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentError;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentRotine;

public class MainController {
	private static Logger LOGGER = LoggerFactory.getLogger(MainController.class);
	private static long start_time;
	private static long end_time;
//	private static long timer1;
//	private static long timer2;
//	private static long timer3;
//	private static long timer4;
//	private static long timer5;
	private static ControllerData controller_data;
	private static Map<Integer, ProcessmentRotine> processment_map;
	private static Map<Integer, ProcessmentError> errors_map;
	
	
	public static void main(String[] args) throws Exception {
		start_time = System.currentTimeMillis();
		LOGGER.info("Initializing ANTController...");
		
		try {
			controller_data = new ControllerData();
			processment_map = controller_data.getProcessment_rotines();
			errors_map = controller_data.getErrors();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			throw new Exception(e);
		}
		
		while(true) {
			try {
				process();
				Thread.sleep(1000);
			} catch (Exception e) {
				LOGGER.error("ANTController processment error");
				LOGGER.error(e.getMessage());
				break;
			}
		}
		
		
		end_time = System.currentTimeMillis() - start_time;
		LOGGER.info("ANTController execution time: "  + end_time);
	}


	private static void process() throws Exception{
		for (Integer processment_seq: processment_map.keySet()) {
			LOGGER.info(processment_seq +": "+ processment_map.get(processment_seq).getName() + " - " + processment_map.get(processment_seq).getProcessment_group());
		}
//		LOGGER.info("Errors:");
//		for (Integer error_code: errors_map.keySet()) {
//			LOGGER.info("error_code: " + error_code);
//			LOGGER.info("error_name: " + errors_map.get(error_code).getName());
//			LOGGER.info("error_description: " + errors_map.get(error_code).getDescription());
//		}
		
	}

}
