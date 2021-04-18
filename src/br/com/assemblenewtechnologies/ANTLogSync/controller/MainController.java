package br.com.assemblenewtechnologies.ANTLogSync.controller;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.Main;
import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.constants.ErrorCodes;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentError;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentErrorLog;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentRotine;
import br.com.assemblenewtechnologies.ANTLogSync.rotines.RotineInterface;

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
	private static boolean need_to_update = false;
	
	private static int actual_processment = 0;

	public static void main(String[] args) throws Exception {
		start_time = System.currentTimeMillis();
		LOGGER.info("Initializing ANTController...");

		// Singleton DBConnection, load Singleton
		try {
			DBConnectionHelper.getInstance();
		} catch (Exception e1) {
			LOGGER.error(e1.getMessage());
			throw new Exception(e1); // no DB we need to throw Exeception runtime Error
		}

		try {
			controller_data = new ControllerData();
			processment_map = controller_data.getProcessment_rotines();
			errors_map = controller_data.getErrors();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			ProcessmentErrorLog.logError(ErrorCodes.RUNTIME_ERROR, GlobalProperties.getInstance().getProcessmentMode(), null,
					MainController.class.getName());
			end_time = (System.currentTimeMillis() - start_time) / 60;
			LOGGER.info("ANTController execution time: " + end_time + " minutes");
			throw new Exception(e); // no Controller Data need to throw Exception RUNTIME ERROR
		}
		
		//TODO: register execution_log

		/**
		 * PROCESSMENT WHILE(1)
		 * CRASH RECOVERY AND SIGNAL LISTENERS
		 */
		while (true) {
			try {
				//TODO: CHECK FOR INCOMPLETE PROCESSMENTS - RECOVERY
				//TODO: SIGNAL LISTENER THREADS
				
				
				process(); // PROCESS ROUTINE EXECUTION MAP
				Thread.sleep(1000);

				//UPDATE THE CONTROLLER MAIN EXEUCTION MAP AND ROTINES
				if (need_to_update)
					update();

			} catch (Exception e) {
				LOGGER.error("ANTController processment error");
				LOGGER.error(e.getMessage());
				ProcessmentErrorLog.logError(ErrorCodes.RUNTIME_ERROR, GlobalProperties.getInstance().getProcessmentMode(), null,
						MainController.class.getName());
				break; //RUNTIME ERROR
			}
		}
		//TODO: exit() rotine, send signals to all modules and watchdog
		//TODO: register end of execution_log
		
		end_time = System.currentTimeMillis() - start_time;
		LOGGER.info("ANTController execution time: " + end_time);
	}

	// check: this will change machine state behavior - must be used with care
	private static void update() throws Exception {
		try {
			controller_data.reload();
			processment_map = controller_data.getProcessment_rotines();
			errors_map = controller_data.getErrors();
		} catch (Exception e) {
			LOGGER.error("ANTController update() contrller_data error: ");
			ProcessmentErrorLog.logError(ErrorCodes.RUNTIME_ERROR, GlobalProperties.getInstance().getProcessmentMode(), null,
					MainController.class.getName());
			LOGGER.error(e.getMessage(), e);
			throw new Exception("Error on updating MainController execution rotines map");
		}

	}

	/**
	 * FIXME: this method should call routine handlers. A refactoring is needed so
	 * processsment_rotines should be HandlersRotines and not EXECUTION rotines
	 * wich should be inside the Handlers. Another refactoring should be that
	 * threads which has already started should not be started again
	 * a Static MAP of Threads on execution must be implemented
	 * and this processs() method should handle them
	 * @throws Exception
	 */
	private static void process() throws Exception {
		
		//For each rotine on processment_rotine table order by processment_seq
		//TODO: make distinct execution for distinc routines_group, p.e. DB_MANAGEMENT or processment_seq > X
		//may run on other threads or momments for now we don't have this distinction
		//FIXME: make execution distinct of groups of rotines
		for (Integer processment_seq : processment_map.keySet()) {
			actual_processment = processment_seq;
			String class_name = getClassName(processment_map.get(processment_seq).getProcessment_group());
			class_name = "br.com.assemblenewtechnologies.ANTLogSync.rotines." + class_name;
			Class<?> c;
			try {
				c = Class.forName(class_name);
				Method method = c.getDeclaredMethod(processment_map.get(processment_seq).getName());
				Constructor<?> l_constructor = c.getConstructor();
				RotineInterface _rotine = (RotineInterface) l_constructor.newInstance();
				method.invoke(_rotine);
			} catch (ClassNotFoundException e) {
				LOGGER.error("ANTController class not found error: " + class_name);
				LOGGER.error(e.getMessage());
			} catch (NoSuchMethodException e) {
				LOGGER.error(
						"ANTController NoSuchMethodException error: " + processment_map.get(processment_seq).getName());
				LOGGER.error(e.getMessage());
			} catch (SecurityException e) {
				LOGGER.error("ANTController processment error");
				LOGGER.error(e.getMessage());
			} catch (InstantiationException e) {
				LOGGER.error("ANTController error to instantiate class: " + class_name);
				LOGGER.error(e.getMessage());
			} catch (IllegalAccessException e) {
				LOGGER.error("ANTController processment error");
				LOGGER.error(e.getMessage());
			} catch (IllegalArgumentException e) {
				LOGGER.error("ANTController processment error");
				LOGGER.error(e.getMessage());
			} catch (InvocationTargetException e) {
				LOGGER.error("ANTController InvocationTargetException error: " + class_name + "."
						+ processment_map.get(processment_seq).getName());
				LOGGER.error(e.getMessage(), e);
			}
		}
	}

	private static String getClassName(String name) {
		String[] parts = name.split("_");

		if (parts.length == 1) {
			return parts[0].substring(0, 1).toUpperCase() + parts[0].substring(1);
		}

		String className = "";
		for (int i = 0; i < parts.length; i++) {
			className += parts[i].substring(0, 1).toUpperCase() + parts[i].substring(1);
		}
		return className;
	}

}
