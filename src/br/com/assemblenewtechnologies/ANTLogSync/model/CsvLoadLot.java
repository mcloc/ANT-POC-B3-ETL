package br.com.assemblenewtechnologies.ANTLogSync.model;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.controller.MainController;

public class CsvLoadLot {
	private BigDecimal id;
	private String lot_name;
	private String load_path;
	private int status;
	private Timestamp created_at;
	private Timestamp updated_at;
	private BigDecimal processment_execution_id;

	private int files_loaded = 0;
	private int files_error_not_loaded = 0;

	public static final int STATUS_LOADING = 0;
	public static final int STATUS_ARCHIVING = 1;
	public static final int STATUS_LOADED = 2;
	public static final int STATUS_LOADED_ARCHIVED = 3;
	public static final int STATUS_LOADED_WITH_ERRORS_ARCHIVED = 4;
	public static final int STATUS_ERROR_ARCHIVED = -1;
	public static final int STATUS_ERROR_NOT_ARCHIVED = -2;

	private static Logger LOGGER = LoggerFactory.getLogger(CsvLoadLot.class);

	public static CsvLoadLot registerCSVLot(String lot_name, String load_path, int status) throws Exception {
		CsvLoadLot csv_lot;
		try {
			csv_lot = new CsvLoadLot(lot_name, load_path, status);
		} catch (Exception e) {
			LOGGER.error("Error registering CSV Loading file");
			throw new Exception(e.getMessage(), e);
		}

		return csv_lot;
	}

	public CsvLoadLot(String lot_name2, String load_path2, int status2) throws Exception {
		this.lot_name = lot_name2;
		this.load_path = load_path2;
		this.status = status2;
		this.processment_execution_id = MainController.getProcessmentExecution().getId();
		save();
	}

	private CsvLoadLot() {
		// TODO Auto-generated constructor stub
	}

	private void save() throws Exception {
		if (lot_name == null || lot_name.equals("") || load_path == null || load_path.equals("")
				|| processment_execution_id == null) {
			throw new Exception("CsvLoadLot not saved, missing attributes values");
		}
		Connection _connection = DBConnectionHelper.getNewConn();
		String compiledQuery = "INSERT INTO  Intellect.csv_load_lot("
				+ "lot_name, load_path, status, processment_execution_id, " + "created_at, updated_at) VALUES "
				+ "(?, ?, ?, ?, ?, ?)";
		PreparedStatement preparedStatement;
		long _now;
		Timestamp _updated_at;
		Timestamp _created_at;
		try {
			_now = System.currentTimeMillis();
			_created_at = new Timestamp(_now);
			_updated_at = new Timestamp(_now);
			preparedStatement = _connection.prepareStatement(compiledQuery, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, lot_name);
			preparedStatement.setString(2, load_path);
			preparedStatement.setInt(3, status);
			preparedStatement.setBigDecimal(4, processment_execution_id);
			preparedStatement.setTimestamp(5, _created_at);
			preparedStatement.setTimestamp(6, _updated_at);
			preparedStatement.execute();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			_connection.rollback();
			_connection.close();
			throw new Exception(e.getMessage(), e);
		}

		ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
		int i = 0;
		if (generatedKeys.next()) {
			if (i > 0) {
				_connection.close();
				throw new Exception("CsvLoadLot.save() error: more then one inserted ID returned...");
			}
			id = generatedKeys.getBigDecimal(1);
			created_at = new Timestamp(_now);
			updated_at = new Timestamp(_now);
			i++;
		}
		_connection.commit();
		_connection.close();
	}

	public void changeStatus(int _status) throws Exception {
		status = _status;
		update();
	}

	private void update() throws Exception {
		Connection _connection = DBConnectionHelper.getNewConn();
		String compiledQuery = "UPDATE Intellect.csv_load_lot SET " 
				+ "lot_name = ?, load_path = ?, status = ?, "
				+ "files_loaded = ?, files_error_not_loaded = ?, updated_at = ? "
				+ "where id = " + id;
		PreparedStatement preparedStatement;
		long _now;
		Timestamp _updated_at;
		try {
			_now = System.currentTimeMillis();
			_updated_at = new Timestamp(_now);
			preparedStatement = _connection.prepareStatement(compiledQuery);
			preparedStatement.setString(1, lot_name);
			preparedStatement.setString(2, load_path);
			preparedStatement.setInt(3, status);
			preparedStatement.setInt(4, files_loaded);
			preparedStatement.setInt(5, files_error_not_loaded);
			preparedStatement.setTimestamp(6, _updated_at);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			LOGGER.error("Error update() CsvLoadLot");
			LOGGER.error(e.getMessage());
			_connection.rollback();
			_connection.close();
			throw new Exception(e.getMessage(), e);
		}
		_connection.commit();
		_connection.close();
	}

	public static boolean checkIfLotAlreadyProcessed(String lot) throws Exception {
		Connection _connection = DBConnectionHelper.getNewConn();
		Statement stmt;
		try {
			stmt = _connection.createStatement();
			ResultSet rs = stmt
					.executeQuery("SELECT * FROM Intellect.csv_load_lot WHERE lot_name = '" + lot + "' ORDER BY updated_at DESC limit 1");
			int i = 0;
			while (rs.next()) {
				_connection.close();
				return true;
				
			}
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
			_connection.close();
			throw new Exception(e1);
		}
		_connection.close();
		return false;
	}

	public static CsvLoadLot getLotByLotName(String lot) throws Exception {
		Connection _connection = DBConnectionHelper.getNewConn();
		Statement stmt;
		try {
			stmt = _connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM Intellect.csv_load_lot " + "WHERE lot_name = '" + lot
					+ "' order by updated_at DESC limit 1");
			CsvLoadLot csv_lot = null;
			while (rs.next()) {
				csv_lot = new CsvLoadLot();
				csv_lot.id = rs.getBigDecimal("id");
				csv_lot.lot_name = rs.getString("lot_name");
				csv_lot.load_path = rs.getString("load_path");
				csv_lot.status = rs.getInt("status");
				csv_lot.files_loaded = rs.getInt("files_loaded");
				csv_lot.files_error_not_loaded = rs.getInt("files_error_not_loaded");
				csv_lot.created_at = rs.getTimestamp("created_at");
				csv_lot.updated_at = rs.getTimestamp("updated_at");
			}
			_connection.close();
			return csv_lot;
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
			//TODO: check if connection.close() get calls on finally before the throw
			_connection.close();
			throw new Exception(e1);
		} 
	}

	public void incrementFilesLoaded() {
		files_loaded++;
	}

	public void incrementFilesErrorNotLoaded() {
		files_error_not_loaded++;
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
	 * @return the files_loaded
	 */
	public int getFiles_loaded() {
		return files_loaded;
	}

	/**
	 * @param files_loaded the files_loaded to set
	 */
	public void setFiles_loaded(int files_loaded) {
		this.files_loaded = files_loaded;
	}

	/**
	 * @return the files_error_not_loaded
	 */
	public int getFiles_error_not_loaded() {
		return files_error_not_loaded;
	}

	/**
	 * @param files_error_not_loaded the files_error_not_loaded to set
	 */
	public void setFiles_error_not_loaded(int files_error_not_loaded) {
		this.files_error_not_loaded = files_error_not_loaded;
	}

}
