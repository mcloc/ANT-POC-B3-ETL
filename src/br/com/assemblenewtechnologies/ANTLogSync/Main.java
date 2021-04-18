package br.com.assemblenewtechnologies.ANTLogSync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.ZipUtils;
import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnector;

public class Main {
	private final static String RTD_DIRETCTORY = "/ANT-TOOLCHAIN/ANTOption/data_load/";
	private final static String ARCHIVE_DIRETCTORY = "/ANT-TOOLCHAIN/ANTOption/archive/";
	private final static String ARCHIVE_BUFFER_DIRETCTORY = "/ANT-TOOLCHAIN/ANTOption/archive_buffer/";
	private static Logger LOGGER = LoggerFactory.getLogger(Main.class);
	private static GlobalProperties globalProperties = new GlobalProperties();
	private static JDBCConnector jdbcConnection;
	private static Connection connection;
	private static long start_time;
	private static long timer1;
//	private static long timer2;
//	private static long timer3;
//	private static long timer4;
//	private static long timer5;

	private static int files_processed = 0;
	private static int directories_processed = 0;
	private static long rows_processed = 0;

	private static String current_directory;
	private static String file_separator;

	public static void main(String[] args) throws Exception {
		file_separator = java.nio.file.FileSystems.getDefault().getSeparator();
		start_time = System.currentTimeMillis();
		LOGGER.info("Initializing ANTLogSync...");
		LOGGER.info("Directory to search RTD files: ." + RTD_DIRETCTORY);

		try {
			jdbcConnection = new JDBCConnector(globalProperties);
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
		timer1 = System.currentTimeMillis();
		long total_time = (timer1 - start_time) / 1000;

		jdbcConnection.connClose();
		LOGGER.info("Total time to process: " + total_time + " seconds");
		LOGGER.info("Total directories processed: " + directories_processed);
		LOGGER.info("Total files processed on all directories: " + files_processed);
		LOGGER.info("Total records processed on all directories: " + rows_processed);
		LOGGER.info("Files per sec: " + (files_processed / total_time));
		LOGGER.info("Records per sec: " + (rows_processed / total_time));

	}

	private static void processFiles(File[] files) throws Exception {
		String last_directory = null;
		boolean in_root_dir = true;
		for (File file : files) {
			if (file.isDirectory()) {
				current_directory = file.getName();
				if (last_directory != null && last_directory != current_directory) {
					zipArchive(last_directory);
				}

				directories_processed++;
				LOGGER.info("Processing Directory: " + file.getAbsolutePath());
				File[] _list = file.listFiles();
				Arrays.sort(_list);
				processFiles(_list); // Calls same method again.
				last_directory = current_directory;
				in_root_dir = true;
			} else {
				in_root_dir = false;
				files_processed++;
//                LOGGER.info("Loading File: " + file.getAbsolutePath());
				loadCSV(file);
			}
			
			if(in_root_dir) {
				File index = new File(RTD_DIRETCTORY+file_separator+last_directory);
				if (index.exists())
					index.delete();
				zipArchive(last_directory);
			}
		}
	}

	private static void zipArchive(String last_directory) {
		String zip_archive_path = ARCHIVE_BUFFER_DIRETCTORY + last_directory;
		String zip_file = ARCHIVE_DIRETCTORY+ last_directory + ".zip";
		ZipUtils appZip = new ZipUtils();
		appZip.setOUTPUT_ZIP_FILE(zip_file);
		appZip.setSOURCE_FOLDER(zip_archive_path);
		appZip.generateFileList(new File(zip_archive_path));
		appZip.zipIt(zip_file);

	}

	private static void loadCSV(File file) throws Exception {
		try {
			long rowsInserted = new CopyManager((BaseConnection) connection).copyIn(
					"COPY B3Log.B3SignalLogger " + "( " + "asset," + "data," + "hora," + "ultimo," + "strike,"
							+ "negocios," + "quantidade," + "volume," + "oferta_compra," + "oferta_venda," + "VOC,"
							+ "VOV," + "vencimento," + "validade," + "contratos_abertos," + "estado_atual," + "relogio"
							+ ") " + "FROM STDIN (FORMAT csv, HEADER true, DELIMITER ',')",
					new BufferedReader(new FileReader(file.getAbsoluteFile())));
//		    LOGGER.info("File: " + absolutePath + " LOADED: " + rowsInserted + " rows");
			rows_processed += rowsInserted;
			archiveFile(file);
		} catch (SQLException e) {
			LOGGER.error("Database Error Loading File: " + file.getAbsoluteFile());
			LOGGER.error(e.getMessage());
//			LOGGER.error(e.getMessage(), e);
			archiveFile(file);
			// throw e;
		} catch (IOException e) {
			LOGGER.error("Error IO Loading File: " + file.getAbsoluteFile());
			LOGGER.error(e.getMessage());
		}

	}

	private static void archiveFile(File file) throws IOException {
		LOGGER.error("Archiving File: " + file.getAbsoluteFile());
		String archive_path = ARCHIVE_BUFFER_DIRETCTORY + file_separator + current_directory;
		File f = new File(archive_path);
		if (!f.exists()) {
			f.mkdir();
		}

		file.renameTo(
				new File(ARCHIVE_DIRETCTORY + file_separator + current_directory + file_separator + file.getName()));

		try {
			Files.move(Paths.get(file.getAbsolutePath()), Paths.get(archive_path + file_separator + file.getName()),
					StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			LOGGER.error("Error IO Archiving File: " + file.getAbsoluteFile());
			LOGGER.error(e.getMessage());
			throw e;
		}
	}

}
