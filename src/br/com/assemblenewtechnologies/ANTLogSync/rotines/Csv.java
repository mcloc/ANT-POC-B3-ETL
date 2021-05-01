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

import org.apache.commons.io.FileUtils;
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
		try {
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

			
		} catch (Exception e) {
			LOGGER.debug("csv_load_start() error");
			LOGGER.error(e.getMessage());
//			if (connection != null)
//				connection.close();
//			throw e;
		} finally {
			connection.close();
		}

	}

	public void csv_archive_start() throws Exception {
		LOGGER.info("[CSV] csv_archive_start...");
	}

	private void processFiles(File[] files_list) throws Exception {
		String last_directory = null;
		boolean in_root_dir = false;
		int i = 0;
		for (File _file_pointer : files_list) {
			if (_file_pointer.isDirectory()) {

				if (!_file_pointer.getName().startsWith("RTD")) {
					LOGGER.warn("Directory: " + _file_pointer.getName() + " has no RTD signature skipping");
					continue;
				}
				current_directory = _file_pointer.getName();

				// Crash Recovery
				if (CsvLoadLot.checkIfLotAlreadyProcessed(current_directory)) {
					csv_load_lot = CsvLoadLot.getLotByLotName(current_directory);
					LOGGER.info(" Resuming processment on Directory: " + current_directory + " lot id: "
							+ csv_load_lot.getId());
					// Crash Recovery Loading was finished just archive into another zip the
					// contents
					// TODO: archive into another ZIP the contents
					if (csv_load_lot.isFinished()) {
						LOGGER.warn("Directory: " + current_directory + " already processed, skipping...");
						moveDirectory2ArchiveBuffer(_file_pointer);
						zipArchive(current_directory);
						continue;
					}

				} else {
					csv_load_lot = CsvLoadLot.registerCSVLot(current_directory,
							GlobalProperties.getInstance().getRtdDiretctory(), CsvLoadLot.STATUS_LOADING);
					LOGGER.info("Processing Directory: " + _file_pointer.getAbsolutePath() + " lot id: "
							+ csv_load_lot.getId());
				}

				// CHECK Processment Mode
//				if (MainController.getProcessmentExecution().getProcessment_mode().equals("batch_process")) {
//					String archive_path = ARCHIVE_BUFFER_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
//							+ current_directory;
//					FileUtils.moveDirectory(file, new File(archive_path));
//					//DELETE from /data_load
//					if (file.exists()) {
//						LOGGER.info("Removing directory: " + file.getAbsolutePath());
//						FileUtils.deleteDirectory(file);
//					}
//					zipArchive(current_directory);
//					continue;
//				}
				i++;

				// FIXME: this condition should not happen since the last_directory should
				// already been removed on the match of RTD_FIM_DE_LOTE.txt
				// But if we do not find the RTD_FIM_DE_LOTE.txt we'll arxchive it and remove it
				// anyway
				// IF we got next Directory on DirList remove last one
				if (last_directory != null && last_directory != current_directory) {
					File _last_direcotry_fileobj = new File(
							RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator() + last_directory);

					// We delete last_directory in RTD_DIRETCTORY because it's suppose to be
					// already in ARCHIVE_BUFFER_DIRETCTORY
					if (_last_direcotry_fileobj.exists())
						FileUtils.deleteDirectory(_last_direcotry_fileobj);

					zipArchive(last_directory);
				}

				directories_processed++;

				File _log_data_dir = new File(RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
						+ current_directory + GlobalProperties.getInstance().getFileSeparator() + "LOG_DATA");

				if (!_log_data_dir.exists()) {
					LOGGER.error(
							"Directory LOG_DATA not found on :" + current_directory + " finishing and archiving lot");
					moveDirectory2ArchiveBuffer(_file_pointer);

//					LOGGER.info("Removing directory: " + current_directory);
//					FileUtils.deleteDirectory(_file_pointer);
					csv_load_lot.setFinished(true);

					zipArchive(_file_pointer.getName());
					in_root_dir = false;
					continue;
				}

				File[] _list = _log_data_dir.listFiles();
				
				if (_list.length == 0) {
					LOGGER.error("Directory LOG_DATA empty. Archiving and removing: " + _file_pointer.getName());
					moveDirectory2ArchiveBuffer(_file_pointer);
					csv_load_lot.setFinished(true);
					zipArchive(_file_pointer.getName());					
				}
				
				
				Arrays.sort(_list);
				processFiles(_list); // Calls same method again.

				// There should not be any files on LOG_DATA after processFiles() returns
				_list = _log_data_dir.listFiles();
				if (_list.length != 0) {
					LOGGER.error("Directory LOG_DATA not empty but processFiles(" + _log_data_dir.getName()
							+ ") has already returned");
					
					moveDirectory2ArchiveBuffer(_file_pointer);
					csv_load_lot.setFinished(true);
					zipArchive(_file_pointer.getName());					
				}

				last_directory = current_directory;
				in_root_dir = true;
				continue;
			} // END IF IS DIRECTORY

			/**
			 * FROM THIS POINT IT's a File
			 */
			if (_file_pointer.getName().equals("RTD_FIM_DE_LOTE.txt")) {
				
				csv_load_lot.setFinished(true);
				moveFile2ArchiveBuffer(_file_pointer, CsvLoadRegistry.STATUS_END_LOT);
				File index = new File(
						RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator() + current_directory);
				LOGGER.info("Removing directory: " + index.getAbsolutePath());
				
				FileUtils.deleteDirectory(index);
				zipArchive(current_directory);
				in_root_dir = false;
				continue;
			}

			in_root_dir = false;
			files_processed++;
			try {
				loadCSV(_file_pointer);
			} catch (Exception e) {
				LOGGER.debug(e.getMessage());
//					throw e;
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

			moveFile2ArchiveBuffer(file, CsvLoadRegistry.STATUS_LOADED_ARCHIVED);
			rows_processed += rowsInserted;
		} catch (SQLException e) {
			LOGGER.debug("Database Error Loading File: " + file.getAbsoluteFile());
			LOGGER.debug(e.getMessage());
			csv_db_registry.setError_msg(e.getMessage());
			csv_load_lot.incrementFilesErrorNotLoaded();
			moveFile2ArchiveBuffer(file, CsvLoadRegistry.STATUS_ERROR_ARCHIVED);

			throw new Exception(e.getMessage());
		} catch (IOException e) {
			csv_load_lot.incrementFilesErrorNotLoaded();
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_NOTFINISHED_NOTARCHIVED);
			csv_db_registry.setError_msg(e.getMessage());
			csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_ERROR_NOT_ARCHIVED);
			LOGGER.debug("Error IO Loading File: " + file.getAbsoluteFile());
			LOGGER.debug(e.getMessage());
			throw new Exception(e.getMessage());
		} catch (Exception e) {
			csv_load_lot.incrementFilesErrorNotLoaded();
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_NOTFINISHED_NOTARCHIVED);
			csv_db_registry.setError_msg(e.getMessage());
			csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_ERROR_NOT_ARCHIVED);
			LOGGER.debug("Error Loading File: " + file.getAbsoluteFile());
			LOGGER.debug(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	private void moveDirectory2ArchiveBuffer(File _file_pointer) throws Exception {
		if (_file_pointer.exists()) {
			String archive_path = ARCHIVE_BUFFER_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
					+ _file_pointer.getName();
			FileUtils.moveDirectory(_file_pointer, new File(archive_path));
			
			//FIXME: this should not happen since the dir already been moved above
//			if (_file_pointer.exists()) {
//				LOGGER.info("Removing directory: " + _file_pointer.getAbsolutePath());
//				FileUtils.deleteDirectory(_file_pointer);
//			}
			return;
		}

		throw new Exception("moveDirectory2ArchiveBuffer() Directory not exsits: " + _file_pointer.getAbsolutePath());
	}

	private void moveFile2ArchiveBuffer(File file, int file_status) throws Exception {
		LOGGER.debug("Archiving File: " + file.getAbsoluteFile());
		String archive_path = ARCHIVE_BUFFER_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
				+ current_directory;
		File f = new File(archive_path);
		if (!f.exists()) {
			f.mkdir();
		}

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

	private void zipArchive(String _directory) throws Exception {
		LOGGER.info("Total load errors on this lot: " + csv_load_lot.getFiles_error_not_loaded());
		String _archive_buffer_path = ARCHIVE_BUFFER_DIRETCTORY + _directory;
		String zip_file = ARCHIVE_DIRETCTORY + _directory + ".zip";
		ZipUtils appZip = new ZipUtils();
		appZip.setOUTPUT_ZIP_FILE(zip_file);
		appZip.setSOURCE_FOLDER(_archive_buffer_path);
		File _directory2zip_obj = new File(_archive_buffer_path);
		appZip.generateFileList(_directory2zip_obj);
		try {
			appZip.zipIt(zip_file);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			if (csv_load_lot.isFinished())
				csv_load_lot.changeStatus(CsvLoadLot.STATUS_FINISHED_WITHERRORS_NOTARCHIVED);
			else
				csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_NOTFINISHED_NOTARCHIVED);
			throw new Exception(e);
		}

		// DELETE ARCHIVE_BUFFER
		if (_directory2zip_obj.exists()) {
			LOGGER.info("Removing directory: " + _directory2zip_obj.getAbsolutePath());
			FileUtils.deleteDirectory(_directory2zip_obj);
		}	

		if (csv_load_lot.getFiles_error_not_loaded() > 0 && csv_load_lot.getFiles_loaded() > 0
				&& csv_load_lot.isFinished()) {
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_FINISHED_WITHERRORS_ARCHIVED);
			return;
		}

		if (csv_load_lot.getFiles_loaded() > 0 && csv_load_lot.getFiles_error_not_loaded() == 0
				&& csv_load_lot.isFinished()) {
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_FINISHED_NOERRORS_ARCHIVED);
			return;
		}

		csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_ARCHIVED);

		return;
	}

}
