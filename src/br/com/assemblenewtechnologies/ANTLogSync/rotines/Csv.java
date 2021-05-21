package br.com.assemblenewtechnologies.ANTLogSync.rotines;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.Helpers.ZipUtils;
import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnector;
import br.com.assemblenewtechnologies.ANTLogSync.model.CsvLoadLot;
import br.com.assemblenewtechnologies.ANTLogSync.model.CsvLoadRegistry;
import br.com.assemblenewtechnologies.ANTLogSync.process_handlers.CSVHandler;

public class Csv extends AbstractRotine {
	private static Logger LOGGER = LoggerFactory.getLogger(Csv.class);
	private String RTD_DIRETCTORY;
	private String ARCHIVE_DIRETCTORY;
	private String ARCHIVE_BUFFER_DIRETCTORY;
	private String current_lot_directory_name;
	private int files_processed = 0;
	private int directories_processed = 0;
	private long rows_processed = 0;
	private JDBCConnector jdbcConnection;
	private Connection connection;
	private long start_time;
	private long timer1;
	private CSVHandler runnable;
	private Thread thread;
	private File[] rtd_directory_list;
	private String thread_name = "CSV_HANDLER";

	private CsvLoadRegistry csv_db_registry;
	private CsvLoadLot csv_load_lot;

	private static int CSV_RETRY_LOT_TIMEDELAY = GlobalProperties.getInstance().getCSV_RETRY_LOT_TIMEDELAY();
	private static int CSV_WAIT2COPY_LOT_TIMEDELAY = GlobalProperties.getInstance().getCSV_WAIT2COPY_LOT_TIMEDELAY();
	private static int CSV_MAX_WAIT2COPY_LOT = GlobalProperties.getInstance().getCSV_MAX_WAIT2COPY_LOT();

	public Csv() {
		RTD_DIRETCTORY = GlobalProperties.getInstance().getRtdDiretctory();
		ARCHIVE_DIRETCTORY = GlobalProperties.getInstance().getArchiveDiretctory();
		ARCHIVE_BUFFER_DIRETCTORY = GlobalProperties.getInstance().getArchiveBufferDiretctory();

		/**
		 * Shutdown Hook to capture SIG KILL and CTRL-C interrupts
		 */
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					if (connection != null) {
						if (!connection.isClosed())
							connection.close();
					}
				} catch (Exception e) {
					LOGGER.error("CSV processment shutdown error");
					LOGGER.error(e.getMessage(), e);
				}
			}
		});
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

//	public synchronized void csv_check_for_files()  throws Exception {
	public void csv_check_for_files()  throws Exception {
		if (isExecuting()) {
			LOGGER.error("[CSV] checking for files already executing, returning");
			return;
		}

		setExecuting(true);

		LOGGER.debug("[CSV] checking for files...");

		File dir = new File(RTD_DIRETCTORY);
		rtd_directory_list = dir.listFiles();

		if (rtd_directory_list.length == 0) {
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
//		start_time = System.currentTimeMillis();
		try {

			Arrays.sort(rtd_directory_list);
			processRTD(rtd_directory_list);

			if (directories_processed > 0) {
				long timer1 = System.currentTimeMillis();
				long total_time = (timer1 - start_time) / 1000;
				LOGGER.info("[CSV] total time to process CSV lots: " + total_time + " seconds");
				LOGGER.info("[CSV] total directories processed: " + directories_processed);
				LOGGER.info("[CSV] total files processed: " + files_processed);
				LOGGER.info("[CSV] total files per sec: " + files_processed / total_time + " seconds");
				LOGGER.info("[CSV] total rows_processed inserted: " + rows_processed);
				LOGGER.info("[CSV] total rows per sec: " + rows_processed / total_time + " seconds");
			}
		} catch (Exception e) {
			LOGGER.info("[CSV]csv_load_start() error");
			LOGGER.error(e.getMessage());
			removeDefaultLotColumn();
			throw e;
		} finally {
			LOGGER.info("[CSV] csv_load_start() finished.");
		}

	}

	private void processRTD(File[] rtd_list)  {
		String last_directory = null;
		boolean in_root_dir = false;
		
		//FOREACH RTD DIRECTORY in 'data_load' dir
		for (File _file_pointer : rtd_list) {
			if (_file_pointer.isDirectory())
				try {
					processRTDDirectory(_file_pointer);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		} // END OF LOOP rtd_list

	}

	private void processRTDDirectory(File _file_pointer) throws IOException, Exception {
		if (!_file_pointer.getName().startsWith("RTD")) {
			LOGGER.warn("[CSV] Directory: " + _file_pointer.getName() + " has no RTD signature skipping");
			return;
		}
		current_lot_directory_name = _file_pointer.getName();

		// Crash Recovery
		if (CsvLoadLot.checkIfLotAlreadyProcessed(current_lot_directory_name)) {
			csv_load_lot = CsvLoadLot.getLotByLotName(current_lot_directory_name);
			LOGGER.info("[CSV] Resuming load processing on Directory: " + current_lot_directory_name + " lot id: "
					+ csv_load_lot.getId() + " actual status: " + csv_load_lot.getStatus());

			// IF IS FINISHED RETURN WIL NOT PROCESS AGAIN
			if (csv_load_lot.isFinished()) {
				File _log_data_dir = new File(RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
						+ current_lot_directory_name + GlobalProperties.getInstance().getFileSeparator() + "LOG_DATA");
				File[] _list = _log_data_dir.listFiles();
				int _last_size_list = _list.length;
				// Little hold to check if the lot directory still in copy process
				int _wait_copy_delay_counter = 0;
				while (_wait_copy_delay_counter < CSV_MAX_WAIT2COPY_LOT) {
					Thread.sleep(CSV_WAIT2COPY_LOT_TIMEDELAY);
					_log_data_dir = new File(RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
							+ current_lot_directory_name + GlobalProperties.getInstance().getFileSeparator()
							+ "LOG_DATA");
					_list = _log_data_dir.listFiles();
					if (_last_size_list != _list.length) {
						_last_size_list = _list.length;
						_wait_copy_delay_counter++;
						LOGGER.warn("[CSV] This lot is already finished. But files still beeing copied. \n"
								+ "Wating to Archive : " + current_lot_directory_name + " " + _wait_copy_delay_counter
								+ "/" + CSV_MAX_WAIT2COPY_LOT);
						continue;
					}
					break;
				}

				if (!GlobalProperties.getInstance().isCSV_ARCHIVE_ON()) {
					FileUtils.deleteDirectory(_file_pointer);
					LOGGER.info("[CSV] Deleting Directory: " + current_lot_directory_name + " lot id: "
							+ csv_load_lot.getId() + " actual status: " + csv_load_lot.getStatus());
					return;
				}

				LOGGER.warn("[CSV] This lot is already finished. Archiving : " + current_lot_directory_name
						+ " without loading.");
				moveDirLot2ArchiveBuffer(_file_pointer);
				zipArchive(current_lot_directory_name);
				return;
			}
		} else { // END OF IF ALREADY PROCESSED CREATE NEW csv_load_lot
			csv_load_lot = CsvLoadLot.registerCSVLot(current_lot_directory_name,
					GlobalProperties.getInstance().getRtdDiretctory(), CsvLoadLot.STATUS_LOADING, this.connection);

			LOGGER.info("[CSV] Processing new Lot: " + current_lot_directory_name + " lot id: " + csv_load_lot.getId()
					+ " actual status: " + csv_load_lot.getStatus());
		}

		directories_processed++;

		File _log_data_dir = new File(RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
				+ current_lot_directory_name + GlobalProperties.getInstance().getFileSeparator() + "LOG_DATA");

		// IF LOG_DATA dont exists archive the entire Lot and continue
		if (!_log_data_dir.exists()) {
			LOGGER.error("[CSV] Directory LOG_DATA not found on :" + current_lot_directory_name
					+ " finishprocessLogDataing and archiving lot");
			moveDirLot2ArchiveBuffer(_file_pointer);
			csv_load_lot.setFinished(true);
			zipArchive(_file_pointer.getName());
			return;
		} else {
			LOGGER.info("[CSV] Directory LOG_DATA found on :" + current_lot_directory_name);
		}

		// GET ALL FILES FROM LOG_DATA DIR on this Lot
		File[] _logdata_list = _log_data_dir.listFiles();
		processLogData(_logdata_list, _file_pointer);

	}

	private void processLogData(File[] _logdata_list, File _file_pointer) throws Exception {
		// TODO Auto-generated method stub

		LOGGER.info("[CSV] Total files on " + current_lot_directory_name + " :" + _logdata_list.length
				+ " files to process");

		// DIRECTORY EMPTY
		if (_logdata_list.length == 0) {
			LOGGER.error("[CSV] Directory LOG_DATA empty. Archiving and removing: " + _file_pointer.getName());
			if (!GlobalProperties.getInstance().isCSV_ARCHIVE_ON()) {
				closeLot();
				LOGGER.info("[CSV] Cleaning  directory: " + _file_pointer.getName());
				FileUtils.deleteDirectory(_file_pointer);
				return;
			}
			moveDirLot2ArchiveBuffer(_file_pointer);
			csv_load_lot.setFinished(true);
			zipArchive(_file_pointer.getName());
			return;
		}

		alterDefaultLotTableRaw();

		Arrays.sort(_logdata_list);

		LOGGER.info("[CSV] processing " + current_lot_directory_name + " :" + _logdata_list.length
				+ " files now...");
		int i = 0;
		for (File _logdata_file : _logdata_list) {
			/**
			 * FROM THIS POINT IT's a File
			 */

			// END OF LOT SIGNAL
			if (_logdata_file.getName().toUpperCase().equals("RTD_FIM_DE_LOTE.TXT")) {
				csv_load_lot.setFinished(true);
				csv_db_registry = CsvLoadRegistry.registerCSV(current_lot_directory_name, csv_load_lot.getId(),
						_logdata_file.getName(), RTD_DIRETCTORY, CsvLoadRegistry.STATUS_END_LOT, connection);
				moveFile2ArchiveBuffer(_logdata_file, CsvLoadRegistry.STATUS_END_LOT);
				File index = new File(RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
						+ current_lot_directory_name);
				LOGGER.info("[CSV] END OF LOT found. Removing directory: " + index.getName() + " lot: "
						+ csv_load_lot.getId() + " setted as finished.");
				FileUtils.deleteDirectory(index);

				zipArchive(current_lot_directory_name);
				return; // get out of function NO MORE FILES After FIM_DE_LOTE.txt
			}

			// RTD LOG FILES JUST ARCHIVE IT
			if (_logdata_file.getName().toUpperCase().endsWith(".LOG")) {
				csv_db_registry = CsvLoadRegistry.registerCSV(current_lot_directory_name, csv_load_lot.getId(),
						_logdata_file.getName(), RTD_DIRETCTORY, CsvLoadRegistry.STATUS_END_LOT, connection);
				moveFile2ArchiveBuffer(_logdata_file, CsvLoadRegistry.STATUS_RTDLOG_FILE);
				continue; // CONTINUE SEEKING FOR FILES
			}

			// in the case a file is not .csv
			if (!_logdata_file.getName().toUpperCase().endsWith(".CSV")) {
				boolean is_csv = false;

				// check for RTD pattern and if first line has the word 'asset' in the colums
				// name
				if (_logdata_file.getName().startsWith("RTD") && _logdata_file.getName().endsWith(".txt")) {
					BufferedReader csvTest = new BufferedReader(new FileReader(_logdata_file.getAbsolutePath()));
					String first_line = csvTest.readLine();
					if (first_line.contains("asset")) {
						is_csv = true;
					}
					csvTest.close();
				}

				// If itś not a CSV NOR a txt head CSV header style just archive it
				if (!is_csv) {
					LOGGER.error("File: " + _logdata_file.getAbsoluteFile() + " not supported");
					moveFile2ArchiveBuffer(_logdata_file, CsvLoadRegistry.STATUS_UNKNOWN_FILE);
					continue;
				}
			}

			/**
			 * from this point should be a valid CSV, so load it
			 */
			files_processed++;
			i++;
			try {
				// TRY TO EFFECTLY LOAD the file (at this point it should be a valid CSV file
				loadCSV(_logdata_file);
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
				throw e;
			}
		} // END OF LOOP
		
		LOGGER.info("[CSV] processed " + current_lot_directory_name + " :" + i
				+ " files have been processed in this lot...");
	}

	private void checkAndArchiveLiveData() throws IOException {
		File _live_data_dir = new File(RTD_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
				+ current_lot_directory_name + GlobalProperties.getInstance().getFileSeparator() + "LIVE_DATA");
		if (_live_data_dir.exists()) {
			File _live_data_archive_dir = new File(ARCHIVE_BUFFER_DIRETCTORY
					+ GlobalProperties.getInstance().getFileSeparator() + current_lot_directory_name
					+ GlobalProperties.getInstance().getFileSeparator() + "LIVE_DATA");
			FileUtils.moveDirectory(_live_data_dir, _live_data_archive_dir);
		}
	}

	private void loadCSV(File file) throws Exception {
		try {
			csv_db_registry = CsvLoadRegistry.registerCSV(current_lot_directory_name, csv_load_lot.getId(),
					file.getName(), RTD_DIRETCTORY, CsvLoadRegistry.STATUS_LOADING, connection);

			long rowsInserted = new CopyManager((BaseConnection) connection).copyIn(
					"COPY B3Log.B3SignalLoggerRaw " + "( " + "asset," + "data," + "hora," + "ultimo," + "strike,"
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
			LOGGER.debug("[CSV] Database Error Loading File: " + file.getAbsoluteFile());
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
			LOGGER.debug("[CSV] Error IO Loading File: " + file.getAbsoluteFile());
			LOGGER.debug(e.getMessage());
			throw new Exception(e.getMessage());
		} catch (Exception e) {
			csv_load_lot.incrementFilesErrorNotLoaded();
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_NOTFINISHED_NOTARCHIVED);
			csv_db_registry.setError_msg(e.getMessage());
			csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_ERROR_NOT_ARCHIVED);
			LOGGER.debug("[CSV] Error Loading File: " + file.getAbsoluteFile());
			LOGGER.debug(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	private void removeDefaultLotColumn() {
		// Super POG para adicionar LotName and LotId into the copy load on
		// B3SignalLoggerRaw
		// BACK to NULL
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(
					"ALTER TABLE B3Log.B3SignalLoggerRaw ALTER COLUMN " + "lot_name SET DEFAULT NULL;");
			preparedStatement.execute();
			preparedStatement = connection
					.prepareStatement("ALTER TABLE B3Log.B3SignalLoggerRaw ALTER COLUMN " + "lot_id SET DEFAULT NULL;");
			preparedStatement.execute();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private void moveDirLot2ArchiveBuffer(File _file_pointer) throws Exception {
		if (!GlobalProperties.getInstance().isCSV_ARCHIVE_ON()) {
			return;
		}

		if (_file_pointer.exists()) {
			String archive_path = ARCHIVE_BUFFER_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
					+ _file_pointer.getName();

			// FIXME: this should not happen since the dir already been moved above
//			if (_file_pointer.exists()) {
//				LOGGER.info("Removing directory: " + _file_pointer.getAbsolutePath());
//				FileUtils.deleteDirectory(_file_pointer);
//			}
			// This should fix it. Inside zipArchive() we check for older zips, if so the
			// new zip
			// will have a timestamp suffixed at the end of file name
			File archive_buffer_dest = new File(archive_path);
			if (archive_buffer_dest.exists()) {
				LOGGER.warn("[CSV] Directory: " + _file_pointer.getName()
						+ " already exsits in archive_buffer zipping it...");
				zipArchive(_file_pointer.getName());
			}

			FileUtils.moveDirectory(_file_pointer, archive_buffer_dest);

			return;
		}

		throw new Exception("moveDirectory2ArchiveBuffer() Directory not exsits: " + _file_pointer.getAbsolutePath());
	}

	private void moveFile2ArchiveBuffer(File file, int file_status) throws Exception {
		if (!GlobalProperties.getInstance().isCSV_ARCHIVE_ON()) {
			switch (file_status) {
			case CsvLoadRegistry.STATUS_LOADED_ARCHIVED:
				csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_LOADED_NOTARCHIVED_BY_CONFIG);
			case CsvLoadRegistry.STATUS_ERROR_ARCHIVED:
				csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_ERROR_NOT_ARCHIVED_BY_CONFIG);
			default:
				csv_db_registry.changeStatus(file_status);
			}

			return;
		}
		LOGGER.debug("[CSV] Archiving File: " + file.getAbsoluteFile());
		String archive_path = ARCHIVE_BUFFER_DIRETCTORY + GlobalProperties.getInstance().getFileSeparator()
				+ current_lot_directory_name;
		File f = new File(archive_path);
		if (!f.exists()) {
			f.mkdir();
			// IF LIVE_DATA exists move it to archive_buffer
			checkAndArchiveLiveData();
		}

		try {
			Files.move(Paths.get(file.getAbsolutePath()),
					Paths.get(archive_path + GlobalProperties.getInstance().getFileSeparator() + file.getName()),
					StandardCopyOption.REPLACE_EXISTING);
			csv_db_registry.changeStatus(file_status);
		} catch (IOException e) {
			LOGGER.error("[CSV] Error IO Archiving File: " + file.getAbsoluteFile());
			LOGGER.error(e.getMessage());
			csv_db_registry.changeStatus(CsvLoadRegistry.STATUS_ERROR_NOT_ARCHIVED);
			throw e;
		}
	}

	private void zipArchive(String _directory) throws Exception {
		if (!GlobalProperties.getInstance().isCSV_ARCHIVE_ON()) {
			closeLot();
			return;
		}
		LOGGER.info("[CSV] Total load errors on this lot: " + csv_load_lot.getFiles_error_not_loaded());
		String _archive_buffer_path = ARCHIVE_BUFFER_DIRETCTORY + _directory;
		String zip_file = ARCHIVE_DIRETCTORY + _directory + ".zip";

		// we check for older zips, if so the new zip
		// will have a timestamp suffixed at the end of file name
		File _zip_file = new File(zip_file);
		if (_zip_file.exists()) {
			Timestamp actual_time = new Timestamp(System.currentTimeMillis());
			String actual_time_formated = new SimpleDateFormat("yyyyMMdd_HHmmss").format(actual_time);
			String _zip_name = _directory + "__" + actual_time_formated + ".zip";
			zip_file = ARCHIVE_DIRETCTORY + _zip_name;
			LOGGER.warn(
					"[CSV] Archive zip file already exsists: " + _directory + " zipping with new name: " + _zip_name);
		}

		_zip_file = null;

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
			LOGGER.info("[CSV] Cleaning archive buffer directory: " + _directory2zip_obj.getName());
			FileUtils.deleteDirectory(_directory2zip_obj);
		}

		closeLot();
	}

	private void closeLot() throws Exception {
		if (csv_load_lot.getFiles_error_not_loaded() > 0 && csv_load_lot.getFiles_loaded() > 0
				&& csv_load_lot.isFinished()) {
			if (!GlobalProperties.getInstance().isCSV_ARCHIVE_ON())
				csv_load_lot.changeStatus(CsvLoadLot.STATUS_FINISHED_WITHERRORS_ARCHIVED);
			else
				csv_load_lot.changeStatus(CsvLoadLot.STATUS_FINISHED_WITHERRORS_NOTARCHIVED_BYCONFIG);
			return;
		}

		if (csv_load_lot.getFiles_loaded() > 0 && csv_load_lot.getFiles_error_not_loaded() == 0
				&& csv_load_lot.isFinished()) {
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_FINISHED_NOERRORS_ARCHIVED);
			return;
		}

		if (csv_load_lot.isFinished())
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_FINISHED_WITHERRORS_ARCHIVED);
		else
			csv_load_lot.changeStatus(CsvLoadLot.STATUS_ERROR_ARCHIVED);

		return;
	}

	public void setStartTime() {
		this.start_time = System.currentTimeMillis();
	}

	private void alterDefaultLotTableRaw() throws Exception {
		// Super POG para adicionar LotName and LotId into the copy load on
		// B3SignalLoggerRaw
		PreparedStatement preparedStatement;
		try {
			preparedStatement = connection.prepareStatement("ALTER TABLE B3Log.B3SignalLoggerRaw ALTER COLUMN "
					+ "lot_name SET DEFAULT '" + csv_load_lot.getLot_name() + "';");
			preparedStatement.execute();
			preparedStatement = connection.prepareStatement("ALTER TABLE B3Log.B3SignalLoggerRaw ALTER COLUMN "
					+ "lot_id SET DEFAULT " + csv_load_lot.getId() + ";");
			preparedStatement.execute();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}

	/**
	 * @return the connection
	 */
	public Connection getConnection() {
		return connection;
	}

	/**
	 * @param connection the connection to set
	 */
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

}
