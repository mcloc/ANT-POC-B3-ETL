package br.com.assemblenewtechnologies.ANTLogSync;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnection;

public class Main {
	private final static String RTD_DIRETCTORY = "/home/mcloc/Downloads/12";
	private static Logger LOGGER = LoggerFactory.getLogger(Main.class);
	static GlobalProperties globalProperties = new GlobalProperties();
	private static JDBCConnection jdbcConnection;
	private static long start_time;

	public static void main(String[] args) {
		start_time = System.currentTimeMillis();
		LOGGER.info("Initializing ANTLogSync...");
		LOGGER.info("Directory to search RTD files: ." + RTD_DIRETCTORY);
		File dir = new File(RTD_DIRETCTORY);
		showFiles(dir.listFiles());

	}
	
	public static void showFiles(File[] files) {
        for (File file : files) {
            if (file.isDirectory()) {
                LOGGER.info("Directory: " + file.getAbsolutePath());
                showFiles(file.listFiles()); // Calls same method again.
            } else {
                LOGGER.info("File: " + file.getAbsolutePath());
            }
        }
    }

}
