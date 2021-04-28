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
import java.util.LinkedHashMap;
import java.util.Map;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.Helpers.ZipUtils;
import br.com.assemblenewtechnologies.ANTLogSync.controller.MainController;
import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnector;
import br.com.assemblenewtechnologies.ANTLogSync.model.CsvLoadLot;
import br.com.assemblenewtechnologies.ANTLogSync.model.CsvLoadRegistry;
import br.com.assemblenewtechnologies.ANTLogSync.process_handlers.CSVHandler;

public class Csv extends AbstractRotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Csv.class);
	private String RTD_DIRETCTORY;
	private String ARCHIVE_DIRETCTORY;
	private String ARCHIVE_BUFFER_DIRETCTORY;
	private String current_directory;
	private int files_processed = 0;
	private int directories_processed = 0;
	private long rows_processed = 0;
	private JDBCConnector jdbcConnection;
	private Connection connection;
	private long start_time;
	private long timer1;
	private CSVHandler runnable;
	private Thread thread;
	private File[] files_list;
	private String thread_name = "CSV_HANDLER";

	private CsvLoadRegistry csv_db_registry;
	private CsvLoadLot csv_load_lot;

	public Csv() {
		RTD_DIRETCTORY = GlobalProperties.getInstance().getRtdDiretctory();
		ARCHIVE_DIRETCTORY = GlobalProperties.getInstance().getArchiveDiretctory();
		ARCHIVE_BUFFER_DIRETCTORY = GlobalProperties.getInstance().getArchiveBufferDiretctory();
	}

	@Override
	public void handler_start() throws Exception {
		runnable = new CSVHandler(this);
		thread = new Thread(runnable, thread_name);
		thread.start();
	}

	@Override
	public void handler_finish() throws Exception {
		thread.interrupt();
	}

	public void csv_check_for_files() throws Exception {
		LOGGER.debug("[CSV] checking for files...");

		File dir = new File(RTD_DIRETCTORY);
		files_list = dir.listFiles();

		if (files_list.length == 0) {
			LOGGER.debug("[CSV] no CSV files found...");
			return;
		}

		csv_load_start();
	}

	public void csv_load_start() throws Exception {
		rows_processed = 0;
		directories_processed = 0;
		files_processed = 0;

		LOGGER.info("[CSV] csv_load_start...");
		start_time = System.currentTimeMillis();

		connection = DBConnectionHelper.getNewConn();
		connection.setAutoCommit(true);

		Arrays.sort(files_list);
		processFiles(files_list);

		if (files_processed > 0) {
			long timer1 = System.currentTimeMillis();
			long total_time = (timer1 - start_time) / 1000;
			LOGGER.info("[CSV] total rows_processed inserted: " + rows_processed);
			LOGGER.info("[CSV] total directories processed: " + directories_processed);
			LOGGER.info("[CSV] total files processed: " + files_processed);
			LOGGER.info("[CSV] total time to process CSV lots: " + total_time + " seconds");
			LOGGER.info("[CSV] total files per sec: " + files_processed / total_time + " seconds");
			LOGGER.info("[CSV] total rows per sec: " + rows_processed / total_time + " seconds");
		}

		connection.close();
	}

	public void csv_archive_start() throws Exception {
		LOGGER.info("[CSV] csv_archive_start...");
	}

	private void processFiles(File[] files_list) throws Exception {
		String last_directory = null;
		boolean in_root_dir = false;
		int i = 0;
		for (File file : files_list) {
			if (file.isDirectory()) {
				current_directory = file.getName();

				//Crash Recovery
				if (CsvLoadLot.checkIfLotAlreadyProcessed(current_directory)) {
					csv_load_lot = CsvLoadLot.getLotByLotName(current_directory);
					LOGGER.info(" Resuming processment on Directory: " + current_directory+ " lot id: " + csv_load_lot.getId());
					//Crash Recovery Loading was finished just archive into another zip the contents
					//TODO: archive into another ZIP the contents
					if (csv_load_lot.getStatus() != CsvLoadLot.STATUS_LOADING) {
						LOGGER.warn("Directory: " + current_directory + " already processed, skipping...");
						String archive_path = ARCHIVE_BUFFER_DIRETCTORY
								+ GlobalProperties.getInstance().getFileSeparator() + current_directory;
						// FIXME: replace gives:
						// ERROR CSVHandler [CSV_HANDLER] /data_load/RTD_20210414 ->
						// /archive_buffer/RTD_20210414: Diretório não vazio
//					File index = new File(archive_path);
//					if (index.exists())
//						index.delete();
						file.renameTo(new File(archive_path));
						zipArchive(current_directory);
						continue;
					}
				} else {
					csv_load_lot = CsvLoadLot.registerCSVLot(current_directory,
							GlobalProperties.getInstance().getRtdDiretctory(), CsvLoadLot.STATUS_LOADING);
					LOGGER.info("Processing Directory: " + file.getAbsolutePath() + " lot id: " + csv_load_lot.getId());
				}

				// CHECK Processment Mode
				if (MainController.getProcessmentExecution().getProcessment_mode().equals("batch_process")
						&& current_directory.equals("LIVE_DATA")) {
					continue;
				}
				i++;

				if (last_directory != null && last_directory != current_directory) {
					File index = new File(
							RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator() + last_directory);
					if (index.exists())
						index.delete();
					zipArchive(last_directory);
				}

				directories_processed++;
				
				File[] _list = file.listFiles();
				Arrays.sort(_list);
				processFiles(_list); // Calls same method again.
				last_directory = current_directory;
				in_root_dir = true;
			} else {
				in_root_dir = false;
				files_processed++;
//                LOGGER.info("Loading File: " + file.getAbsolutePath());
				try {
					loadCSV(file);
				} catch (Exception e) {
					LOGGER.debug(e.getMessage());
//					throw e;
				}
			}

			if (i >= files_list.length) {
				File index = new File(
						RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator() + last_directory);
				if (index.exists())
					index.delete();
				zipArchive(last_directory);
			}
		}

	}

	private void loadCSV(File file) throws Exception {
		try {
			csv_db_registry = CsvLoadRegistry.registerCSV(current_directory, csv_load_lot.getId(), file.getName(),
					RTD_DIRETCTORY, CsvLoadRegistry.STATUS_LOADING);
			long rowsInserted = new CopyManager((BaseConnection) connection).copyIn(
					"COPY B3Log.B3SignalLogger " + "( " + "asset," + "data," + "hora," + "ultimo," + "strike,"
							+ "negocios," + "quantidade," + "volume," + "oferta_compra," + "oferta_venda," + "VOC,"
							+ "VOV," + "vencimento," + "validade," + "contratos_abertos," + "estado_atual," + "relogio"
							+ ") " + "FROM STDIN (FORMAT csv, HEADER true, DELIMITER ',')",
					new BufferedReader(new FileReader(file.getAbsoluteFile())));
			LOGGER.debug("File: " + file.getAbsoluteFile() + " LOADED: " + rowsInserted + " rows");
			if (rowsInserted > 0) {
				csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_LOADED);
				csv_load_lot.incrementFilesLoaded();
			}

			archiveFile(file, CsvLoadRegistry.STATUS_LOADED_ARCHIVED);
			rows_processed += rowsInserted;
		} catch (SQLException e) {
			LOGGER.debug("Database Error Loading File: " + file.getAbsoluteFile());
			LOGGER.debug(e.getMessage());
			csv_db_registry.setError_msg(e.getMessage());
			csv_load_lot.incrementFilesErrorNotLoaded();
			archiveFile(file, CsvLoadRegistry.STATUS_ERROR_ARCHIVED);
			
			throw new Exception(e.getMessage());
		} catch (IOException e) {
			csv_load_lot.incrementFilesErrorNotLoaded();
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_NOT_ARCHIVED);
			csv_db_registry.setError_msg(e.getMessage());
			csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_ERROR_NOT_ARCHIVED);
			LOGGER.debug("Error IO Loading File: " + file.getAbsoluteFile());
			LOGGER.debug(e.getMessage());
			throw new Exception(e.getMessage());
		} catch (Exception e) {
			csv_load_lot.incrementFilesErrorNotLoaded();
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_NOT_ARCHIVED);
			csv_db_registry.setError_msg(e.getMessage());
			csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_ERROR_NOT_ARCHIVED);
			LOGGER.debug("Error Loading File: " + file.getAbsoluteFile());
			LOGGER.debug(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	private void archiveFile(File file, int file_status) throws Exception {
		LOGGER.debug("Archiving File: " + file.getAbsoluteFile());
		String archive_path = ARCHIVE_BUFFER_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
				+ current_directory;
		File f = new File(archive_path);
		if (!f.exists()) {
			f.mkdir();
		}

//		file.renameTo(new File(ARCHIVE_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator() + current_directory
//				+ GlobalProperties.getInstance().getFileSeparator() + file.getName()));

		try {
			Files.move(Paths.get(file.getAbsolutePath()),
					Paths.get(archive_path + GlobalProperties.getInstance().getFileSeparator() + file.getName()),
					StandardCopyOption.REPLACE_EXISTING);
			csv_db_registry.changeStatus(file_status);
		} catch (IOException e) {
			LOGGER.error("Error IO Archiving File: " + file.getAbsoluteFile());
			LOGGER.error(e.getMessage());
			csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_ERROR_NOT_ARCHIVED);
			throw e;
		}
	}

	private void zipArchive(String last_directory) throws Exception {
		LOGGER.info("Total load errors on this lot: " + csv_load_lot.getFiles_error_not_loaded());
		String zip_archive_path = ARCHIVE_BUFFER_DIRETCTORY + last_directory;
		String zip_file = ARCHIVE_DIRETCTORY + last_directory + ".zip";
		ZipUtils appZip = new ZipUtils();
		appZip.setOUTPUT_ZIP_FILE(zip_file);
		appZip.setSOURCE_FOLDER(zip_archive_path);
		appZip.generateFileList(new File(zip_archive_path));
		try {
			appZip.zipIt(zip_file);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_NOT_ARCHIVED);
			throw new Exception(e);
		}

		if (csv_load_lot.getFiles_error_not_loaded() > 0 && csv_load_lot.getFiles_loaded() > 0) {
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_LOADED_WITH_ERRORS_ARCHIVED);
			return;
		}

		if (csv_load_lot.getFiles_loaded() > 0 && csv_load_lot.getFiles_error_not_loaded() == 0) {
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_LOADED_ARCHIVED);
			return;
		}

		csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_ARCHIVED);

		return;
	}

}
