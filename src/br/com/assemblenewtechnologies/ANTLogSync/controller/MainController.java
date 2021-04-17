package br.com.assemblenewtechnologies.ANTLogSync.controller;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentError;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentRotine;
import br.com.assemblenewtechnologies.ANTLogSync.rotines.Rotine;

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


	private static void process(){
		for (Integer processment_seq: processment_map.keySet()) {
			String class_name = getClassName(processment_map.get(processment_seq).getProcessment_group());
			class_name = "br.com.assemblenewtechnologies.ANTLogSync.rotines."+class_name;
			LOGGER.info("Processment Sequence: "+ processment_seq);
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
			} catch (NoSuchMethodException e) {
				LOGGER.error("ANTController NoSuchMethodException error: "+processment_map.get(processment_seq).getName() );
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
				LOGGER.error("ANTController InvocationTargetException error: " + class_name + "." +processment_map.get(processment_seq).getName() );
				LOGGER.error(e.getMessage());
			}

		}
	}


	private static String getClassName(String name) {
		String[] parts = name.split("_");
		
		if(parts.length == 1) {
			return parts[0].substring(0, 1).toUpperCase() + parts[0].substring(1);
		}
		
		String className = "";
		for(int i = 0; i < parts.length;i++) {
			className += parts[i].substring(0, 1).toUpperCase() + parts[i].substring(1);
		}
		return className;
	}

}
