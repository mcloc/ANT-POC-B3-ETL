package br.com.assemblenewtechnologies.ANTLogSync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnection;

public class Main {
	private final static String RTD_DIRETCTORY = "/home/mcloc/Downloads/12";
	private static Logger LOGGER = LoggerFactory.getLogger(Main.class);
	private static GlobalProperties globalProperties = new GlobalProperties();
	private static JDBCConnection jdbcConnection;
	private static Connection connection;
	private static long start_time;
	private static long timer1;
	private static long timer2;
	private static long timer3;
	private static long timer4;
	private static long timer5;
	
	private static int files_processed = 0;
	private static int directories_processed = 0;
	

	public static void main(String[] args) throws Exception {
		start_time = System.currentTimeMillis();
		LOGGER.info("Initializing ANTLogSync...");
		LOGGER.info("Directory to search RTD files: ." + RTD_DIRETCTORY);
		
		
		try {
			jdbcConnection = new JDBCConnection(globalProperties);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception("No database connection...");
		}

		connection = jdbcConnection.getConn();
		connection.setAutoCommit(true);
		
		start_time = System.currentTimeMillis();
		
		File dir = new File(RTD_DIRETCTORY);
		File[] list = dir.listFiles();
		Arrays.sort(list);
		processFiles(list);
		
		
		LOGGER.info("Total directories processed: " + directories_processed);
		LOGGER.info("Total files processed on all directories: " + files_processed);

	}
	
	private static void processFiles(File[] files) throws Exception {
        for (File file : files) {
            if (file.isDirectory()) {
            	directories_processed++;
                LOGGER.info("Processing Directory: " + file.getAbsolutePath());
                File[] _list = file.listFiles();
                Arrays.sort(_list);
                processFiles(_list); // Calls same method again.
            } else {
            	files_processed++;
                LOGGER.info("Loading File: " + file.getAbsolutePath());
                loadCSV(file.getAbsolutePath());
            }
        }
    }

	private static void loadCSV(String absolutePath) throws Exception {
		try {
		    long rowsInserted = new CopyManager((BaseConnection) connection)
		            .copyIn(
		                "COPY B3Log.B3SignalLogger "+
		            "( "
		            + "asset,"
		            + "data,"
		            + "hora,"
		            + "ultimo,"
		            + "strike,"
		            + "negocios,"
		            + "quantidade,"
		            + "volume,"
		            + "oferta_compra,"
		            + "oferta_venda,"
		            + "VOC,"
		            + "VOV,"
		            + "vencimento,"
		            + "validade,"
		            + "contratos_abertos,"
		            + "estado_atual,"
		            + "relogio"
		            + ") "
		            + "FROM STDIN (FORMAT csv, HEADER true, DELIMITER ',')", 
		                new BufferedReader(new FileReader(absolutePath))
		                );
		    LOGGER.info("File: " + absolutePath + " LOADED: " + rowsInserted + " rows");
		} catch (SQLException | IOException e) {
			LOGGER.error("Error Loading File: " + absolutePath);
			LOGGER.error(e.getMessage(), e);
			throw e;
		}

	}

}
