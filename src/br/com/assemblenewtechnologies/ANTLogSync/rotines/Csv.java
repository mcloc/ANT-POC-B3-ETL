package br.com.assemblenewtechnologies.ANTLogSync.rotines;

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

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.Helpers.ZipUtils;
import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnection;

public class Csv extends Rotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Csv.class);
	private String RTD_DIRETCTORY;
	private String ARCHIVE_DIRETCTORY;
	private String ARCHIVE_BUFFER_DIRETCTORY;
	private GlobalProperties globalProperties = new GlobalProperties();
	private String current_directory;
	private int files_processed = 0;
	private int directories_processed = 0;
	private long rows_processed = 0;
	private JDBCConnection jdbcConnection;
	private Connection connection;
	private long start_time;
	private long timer1;

	public Csv() {
		RTD_DIRETCTORY = globalProperties.getRtdDiretctory();
		ARCHIVE_DIRETCTORY = globalProperties.getArchiveDiretctory();
		ARCHIVE_BUFFER_DIRETCTORY = globalProperties.getArchiveBufferDiretctory();
	}

	public void csv_wating() throws Exception {
		LOGGER.info("[CSV] csv_wating...");
	}

	public void csv_load_start() throws Exception {
		LOGGER.info("[CSV] csv_load_start...");
		start_time = System.currentTimeMillis();

		DBConnectionHelper connHelper;
		try {
			connHelper = DBConnectionHelper.getInstance();
			jdbcConnection = connHelper.getJdbcConnection();
			connection = jdbcConnection.getConn();
			connection.setAutoCommit(true);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			throw new Exception("No database connection...");
		}


		File dir = new File(RTD_DIRETCTORY);
		File[] files_list = dir.listFiles();
		
		if(files_list.length == 0) {
			LOGGER.info("[CSV] no CSV files found...");			
			connection.close();
			return;
		}
		
		Arrays.sort(files_list);
		processFiles(files_list);

		connection.close();
	}

	public void csv_archive_start() throws Exception {
		LOGGER.info("[CSV] csv_archive_start...");
	}

	private void processFiles(File[] files_list) throws Exception {
		String last_directory = null;
		boolean in_root_dir = true;
		for (File file : files_list) {
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

			if (in_root_dir) {
				File index = new File(RTD_DIRETCTORY + globalProperties.getFileSeparator() + last_directory);
				if (index.exists())
					index.delete();
				zipArchive(last_directory);
			}
			long timer1 = System.currentTimeMillis();
			long total_time = (timer1 - start_time) / 1000;
		}
	}

	private void loadCSV(File file) throws Exception {
		try {
			long rowsInserted = new CopyManager((BaseConnection) connection).copyIn(
					"COPY B3Log.B3SignalLogger " + "( " + "asset," + "data," + "hora," + "ultimo," + "strike,"
							+ "negocios," + "quantidade," + "volume," + "oferta_compra," + "oferta_venda," + "VOC,"
							+ "VOV," + "vencimento," + "validade," + "contratos_abertos," + "estado_atual," + "relogio"
							+ ") " + "FROM STDIN (FORMAT csv, HEADER true, DELIMITER ',')",
					new BufferedReader(new FileReader(file.getAbsoluteFile())));
		    LOGGER.info("File: " + file.getAbsoluteFile() + " LOADED: " + rowsInserted + " rows");
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

	private void archiveFile(File file) throws IOException {
		LOGGER.info("Archiving File: " + file.getAbsoluteFile());
		String archive_path = ARCHIVE_BUFFER_DIRETCTORY + globalProperties.getFileSeparator() + current_directory;
		File f = new File(archive_path);
		if (!f.exists()) {
			f.mkdir();
		}

		file.renameTo(new File(ARCHIVE_DIRETCTORY + globalProperties.getFileSeparator() + current_directory
				+ globalProperties.getFileSeparator() + file.getName()));

		try {
			Files.move(Paths.get(file.getAbsolutePath()),
					Paths.get(archive_path + globalProperties.getFileSeparator() + file.getName()),
					StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			LOGGER.error("Error IO Archiving File: " + file.getAbsoluteFile());
			LOGGER.error(e.getMessage());
			throw e;
		}
	}

	private void zipArchive(String last_directory) {
		String zip_archive_path = ARCHIVE_BUFFER_DIRETCTORY + last_directory;
		String zip_file = ARCHIVE_DIRETCTORY + last_directory + ".zip";
		ZipUtils appZip = new ZipUtils();
		appZip.setOUTPUT_ZIP_FILE(zip_file);
		appZip.setSOURCE_FOLDER(zip_archive_path);
		appZip.generateFileList(new File(zip_archive_path));
		appZip.zipIt(zip_file);

	}
}
