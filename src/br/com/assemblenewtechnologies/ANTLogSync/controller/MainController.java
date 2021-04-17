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
import br.com.assemblenewtechnologies.ANTLogSync.rotines.Rotine;

public class MainController {
	private static Logger LOGGER = LoggerFactory.getLogger(MainController.class);
	private static GlobalProperties globalProperties = new GlobalProperties();
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

	public static void main(String[] args) throws Exception {
		start_time = System.currentTimeMillis();
		LOGGER.info("Initializing ANTController...");

		// Singleton DBConnection, load Singleton
		try {
			DBConnectionHelper.getInstance();
		} catch (Exception e1) {
			LOGGER.error(e1.getMessage());
			throw new Exception(e1);
		}

		try {
			controller_data = new ControllerData();
			processment_map = controller_data.getProcessment_rotines();
			errors_map = controller_data.getErrors();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			ProcessmentErrorLog.logError(ErrorCodes.RUNTIME_ERROR, globalProperties.getProcessmentMode(), null,
					MainController.class.getName());
			DBConnectionHelper.close();
			end_time = (System.currentTimeMillis() - start_time) / 60;
			LOGGER.info("ANTController execution time: " + end_time + " minutes");
			throw new Exception(e);
		}

		while (true) {
			try {
				process();
				Thread.sleep(1000);

				if (need_to_update)
					update();

			} catch (Exception e) {
				LOGGER.error("ANTController processment error");
				LOGGER.error(e.getMessage());
				ProcessmentErrorLog.logError(ErrorCodes.RUNTIME_ERROR, globalProperties.getProcessmentMode(), null,
						MainController.class.getName());
				break;
			}
		}

		DBConnectionHelper.close();
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
			ProcessmentErrorLog.logError(ErrorCodes.RUNTIME_ERROR, globalProperties.getProcessmentMode(), null,
					MainController.class.getName());
			LOGGER.error(e.getMessage(), e);
		}

	}

	private static void process() throws Exception {
		for (Integer processment_seq : processment_map.keySet()) {
			String class_name = getClassName(processment_map.get(processment_seq).getProcessment_group());
			class_name = "br.com.assemblenewtechnologies.ANTLogSync.rotines." + class_name;
			LOGGER.info("Processment Sequence: " + processment_seq);
			Class<?> c;
			try {
				c = Class.forName(class_name);
				Method method = c.getDeclaredMethod(processment_map.get(processment_seq).getName());
				Constructor<?> l_constructor = c.getConstructor();
				Rotine _rotine = (Rotine) l_constructor.newInstance();
				method.invoke(_rotine);
			} catch (ClassNotFoundException e) {
				LOGGER.error("ANTController class not found error: " + class_name);
				LOGGER.error(e.getMessage());
				ProcessmentErrorLog.logError(ErrorCodes.RUNTIME_ERROR, globalProperties.getProcessmentMode(), null,
						MainController.class.getName());
			} catch (NoSuchMethodException e) {
				LOGGER.error(
						"ANTController NoSuchMethodException error: " + processment_map.get(processment_seq).getName());
				LOGGER.error(e.getMessage());
				ProcessmentErrorLog.logError(ErrorCodes.RUNTIME_ERROR, globalProperties.getProcessmentMode(), null,
						MainController.class.getName());
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
