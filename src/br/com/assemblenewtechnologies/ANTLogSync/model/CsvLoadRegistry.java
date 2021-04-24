package br.com.assemblenewtechnologies.ANTLogSync.model;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.controller.MainController;

public class CsvLoadRegistry {

	private BigDecimal id;
	private String lot_name;
	private String file_name;
	private String load_path;
	private int status;
	private Timestamp created_at;
	private Timestamp updated_at;
	private BigDecimal processment_execution_id;

	public static final int STATUS_LOADING = 0;
	public static final int STATUS_ARCHIVING = 1;
	public static final int STATUS_LOADED = 2;
	public static final int STATUS_LOADED_ARCHIVED = 3;
	public static final int STATUS_ERROR_ARCHIVED = -1;
	public static final int STATUS_ERROR_NOT_ARCHIVED = -2;

	private static Logger LOGGER = LoggerFactory.getLogger(CsvLoadRegistry.class);
	private Connection connection;

	public static CsvLoadRegistry registerCSV(String lot_name, String file_name, String load_path, int status)
			throws Exception {
		CsvLoadRegistry cr;
		try {
			cr = new CsvLoadRegistry(lot_name, file_name, load_path, status);
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
	 * @param file_name
	 * @param load_path
	 * @param status
	 * @param processment_execution_id
	 * @throws Exception
	 */
	public CsvLoadRegistry(String lot_name, String file_name, String load_path, int status) throws Exception {
		this.lot_name = lot_name;
		this.file_name = file_name;
		this.load_path = load_path;
		this.status = status;
		this.processment_execution_id = MainController.getProcessmentExecution().getId();
		save();
	}

	private void save() throws Exception {
		if (lot_name == null || lot_name.equals("") || file_name == null || file_name.equals("") || load_path == null
				|| load_path.equals("") || processment_execution_id == null) {
			throw new Exception("CsvLoadRegistry not saved, missing attributes values");
		}
		connection = DBConnectionHelper.getNewConn();
		String compiledQuery = "INSERT INTO  Intellect.csv_load_registry("
				+ "lot_name, file_name, load_path, status, processment_execution_id, "
				+ "created_at, updated_at) VALUES " + "(?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement preparedStatement;
		long _now;
		try {
			_now = System.currentTimeMillis();
			preparedStatement = connection.prepareStatement(compiledQuery);
			preparedStatement.setString(1, lot_name);
			preparedStatement.setString(2, file_name);
			preparedStatement.setString(3, load_path);
			preparedStatement.setInt(4, status);
			preparedStatement.setBigDecimal(5, processment_execution_id);
			preparedStatement.setLong(6, _now);
			preparedStatement.setLong(7, _now);
			preparedStatement.execute();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			connection.rollback();
			connection.close();
			throw new Exception(e.getMessage(), e);
		}
		connection.commit();
		ResultSet rs = preparedStatement.getGeneratedKeys();
		int i = 0;
		if (rs.next()) {
			if (i > 0) {
				connection.close();
				throw new Exception("ProcessmentExecetion.save() error: more then one inserted ID returned...");
			}
			id = rs.getBigDecimal(1);
			created_at = new Timestamp(_now);
			updated_at = new Timestamp(_now);
			i++;
		}
		connection.close();
	}
	
	
	public void changeStatus(int statusArchiving) throws Exception {
		status = statusArchiving;
		update();
	}

	private void update() throws Exception {
		connection = DBConnectionHelper.getNewConn();
		String compiledQuery = "UPDATE Intellect.csv_load_registry("
				+ "lot_name, file_name, load_path, status, "
				+ "updated_at) VALUES " + "(?, ?, ?, ?, ?)";
		PreparedStatement preparedStatement;
		long _now;
		try {
			_now = System.currentTimeMillis();
			preparedStatement = connection.prepareStatement(compiledQuery);
			preparedStatement.setString(1, lot_name);
			preparedStatement.setString(2, file_name);
			preparedStatement.setString(3, load_path);
			preparedStatement.setInt(4, status);
			preparedStatement.setLong(5, _now);
			preparedStatement.execute();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			connection.rollback();
			connection.close();
			throw new Exception(e.getMessage(), e);
		}
		connection.commit();
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


}
