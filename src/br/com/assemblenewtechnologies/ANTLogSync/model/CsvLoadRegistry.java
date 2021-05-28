package br.com.assemblenewtechnologies.ANTLogSync.model;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.controller.MainController;

public class CsvLoadRegistry {

	private BigDecimal id;
	private String lot_name;
	private BigDecimal lot_id;
	private String file_name;
	private String load_path;
	private int status;
	private String error_msg;
	private Timestamp created_at;
	private Timestamp updated_at;
	private BigDecimal processment_execution_id;

	public static final int STATUS_LOADING = 0;
	public static final int STATUS_ARCHIVING = 1;
	public static final int STATUS_LOADED = 2;
	public static final int STATUS_LOADED_ARCHIVED = 3;
	public static final int STATUS_LOADED_NOTARCHIVED_BY_CONFIG = 4;
	public static final int STATUS_ERROR_ARCHIVED = -1;
	public static final int STATUS_ERROR_NOT_ARCHIVED = -2;
	public static final int STATUS_ERROR_NOT_ARCHIVED_BY_CONFIG = -4;
	public static final int STATUS_END_LOT = 10;
	public static final int STATUS_RTDLOG_FILE = 30;
	public static final int STATUS_UNKNOWN_FILE = -10;

	private static Logger LOGGER = LoggerFactory.getLogger(CsvLoadRegistry.class);

	public static CsvLoadRegistry registerCSV(String lot_name, BigDecimal lot_id, String file_name, String load_path,
			int status) throws Exception {
		CsvLoadRegistry cr;
		try {
			cr = new CsvLoadRegistry(lot_name, lot_id, file_name, load_path, status);
		} catch (Exception e) {
			LOGGER.error("Error registering CSV Loading file");
			throw new Exception(e.getMessage(), e);
		}

		return cr;
	}

	/**
	 * Already saves on database
	 * 
	 * @param lot_name
	 * @param lot_id2
	 * @param file_name
	 * @param load_path
	 * @param status
	 * @param processment_execution_id
	 * @throws Exception
	 */
	public CsvLoadRegistry(String lot_name, BigDecimal lot_id2, String file_name, String load_path, int status)
			throws Exception {
		this.lot_name = lot_name;
		this.lot_id = lot_id2;
		this.file_name = file_name;
		this.load_path = load_path;
		this.status = status;
		this.processment_execution_id = MainController.getProcessmentExecution().getId();
		save();
	}

	private void save() throws Exception {
		if (lot_id == null || lot_name == null || lot_name.equals("") || file_name == null || file_name.equals("")
				|| load_path == null || load_path.equals("") || processment_execution_id == null) {
			throw new Exception("CsvLoadRegistry not saved, missing attributes values");
		}

		String compiledQuery = "INSERT INTO  Intellect.csv_load_registry("
				+ "lot_name, lot_id, file_name, load_path, status, processment_execution_id, "
				+ "created_at, updated_at) VALUES " + "(?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement preparedStatement;
		long _now;
		Timestamp _updated_at;
		Timestamp _created_at;
		try {
			_now = System.currentTimeMillis();
			_created_at = new Timestamp(_now);
			_updated_at = new Timestamp(_now);
			preparedStatement = DBConnectionHelper.getCSVConn().prepareStatement(compiledQuery, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, lot_name);
			preparedStatement.setBigDecimal(2, lot_id);
			preparedStatement.setString(3, file_name);
			preparedStatement.setString(4, load_path);
			preparedStatement.setInt(5, status);
			preparedStatement.setBigDecimal(6, processment_execution_id);
			preparedStatement.setTimestamp(7, _created_at);
			preparedStatement.setTimestamp(8, _updated_at);
			preparedStatement.execute();
//			if(!DBConnectionHelper.getCSVConn().getAutoCommit())
//				DBConnectionHelper.getCSVConn().commit();	
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			throw new Exception(e.getMessage(), e);
		}

		ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
		int i = 0;
		if (generatedKeys.next()) {
			if (i > 0) {
				throw new Exception("CsvLoadRegistry.save() error: more then one inserted ID returned...");
			}
			id = generatedKeys.getBigDecimal(1);
			created_at = new Timestamp(_now);
			updated_at = new Timestamp(_now);
			i++;
		}
	}

	public void changeStatus(int _status) throws Exception {
		status = _status;
		update();
	}

	private void update() throws Exception {
		String compiledQuery = "UPDATE Intellect.csv_load_registry SET "
				+ "lot_name = ?, lot_id = ?, file_name = ?, load_path = ?, status = ?, error_msg = ?, "
				+ "updated_at = ? " + "where id = " + id;
		PreparedStatement preparedStatement;
		long _now;
		Timestamp _updated_at;
		try {
			_now = System.currentTimeMillis();
			_updated_at = new Timestamp(_now);
			preparedStatement = DBConnectionHelper.getCSVConn().prepareStatement(compiledQuery);
			preparedStatement.setString(1, lot_name);
			preparedStatement.setBigDecimal(2, lot_id);
			preparedStatement.setString(3, file_name);
			preparedStatement.setString(4, load_path);
			preparedStatement.setInt(5, status);
			preparedStatement.setString(6, error_msg);
			preparedStatement.setTimestamp(7, _updated_at);
			preparedStatement.executeUpdate();
			if (!DBConnectionHelper.getCSVConn().getAutoCommit())
				DBConnectionHelper.getCSVConn().commit();
		} catch (SQLException e) {
			LOGGER.error("Error update() CsvLoadRegistry");
			LOGGER.error(e.getMessage());
			throw new Exception(e.getMessage(), e);
		}
	}

	/**
	 * too much overhead
	 * 
	 * @param file_name
	 * @return
	 * @throws Exception
	 */
	public static boolean checkIfCSVAlreadyProcessed(String file_name) throws Exception {
		Statement stmt;
		try {
			stmt = DBConnectionHelper.getCSVConn().createStatement();
			ResultSet rs = stmt.executeQuery(
					"SELECT * FROM Intellect.csv_load_registry " + "WHERE file_name = '" + file_name + "'");
			int i = 0;
			while (rs.next()) {
				if (i >= 1) {
					return true;
				}
				i++;
			}
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
			throw new Exception(e1);
		}
		return false;
	}

	/**
	 * @return the id
	 */
	public BigDecimal getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(BigDecimal id) {
		this.id = id;
	}

	/**
	 * @return the lot_name
	 */
	public String getLot_name() {
		return lot_name;
	}

	/**
	 * @param lot_name the lot_name to set
	 */
	public void setLot_name(String lot_name) {
		this.lot_name = lot_name;
	}

	/**
	 * @return the file_name
	 */
	public String getFile_name() {
		return file_name;
	}

	/**
	 * @param file_name the file_name to set
	 */
	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}

	/**
	 * @return the load_path
	 */
	public String getLoad_path() {
		return load_path;
	}

	/**
	 * @param load_path the load_path to set
	 */
	public void setLoad_path(String load_path) {
		this.load_path = load_path;
	}

	/**
	 * @return the status
	 */
	public int getStatus() {
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(int status) {
		this.status = status;
	}

	/**
	 * @return the created_at
	 */
	public Timestamp getCreated_at() {
		return created_at;
	}

	/**
	 * @param created_at the created_at to set
	 */
	public void setCreated_at(Timestamp created_at) {
		this.created_at = created_at;
	}

	/**
	 * @return the updated_at
	 */
	public Timestamp getUpdated_at() {
		return updated_at;
	}

	/**
	 * @param updated_at the updated_at to set
	 */
	public void setUpdated_at(Timestamp updated_at) {
		this.updated_at = updated_at;
	}

	/**
	 * @return the processment_execution_id
	 */
	public BigDecimal getProcessment_execution_id() {
		return processment_execution_id;
	}

	/**
	 * @param processment_execution_id the processment_execution_id to set
	 */
	public void setProcessment_execution_id(BigDecimal processment_execution_id) {
		this.processment_execution_id = processment_execution_id;
	}

	/**
	 * @return the lot_id
	 */
	public BigDecimal getLot_id() {
		return lot_id;
	}

	/**
	 * @param lot_id the lot_id to set
	 */
	public void setLot_id(BigDecimal lot_id) {
		this.lot_id = lot_id;
	}

	/**
	 * @return the error_msg
	 */
	public String getError_msg() {
		return error_msg;
	}

	/**
	 * @param error_msg the error_msg to set
	 */
	public void setError_msg(String error_msg) {
		this.error_msg = error_msg;
	}


}
