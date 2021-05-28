package br.com.assemblenewtechnologies.ANTLogSync.model;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.controller.MainController;

public class ProcessmentErrorLog {
	private String name;
	private String description;
	private int error_code;
	private String processment_group;
	private String processment_mode;
	private BigDecimal processment_id;
	private BigDecimal processment_errors_id;
	private String java_class;

	private static Logger LOGGER = LoggerFactory.getLogger(ProcessmentErrorLog.class);

	
	/**
	 * Save Error on database
	 * 
	 * @param _error_code
	 * @param processment_group
	 * @param processment_mode
	 * @param processment_id
	 * @param _java_class
	 * @throws Exception
	 */
	public static void logError(int _error_code, String processment_group, String _java_class) throws Exception {

		ProcessmentErrorLog error_log = new ProcessmentErrorLog(_error_code);
		error_log.processment_group = processment_group;
		error_log.processment_mode = MainController.getProcessmentExecution().getProcessment_mode();
		error_log.processment_id = MainController.getProcessmentExecution().getId();
		error_log.java_class = _java_class;

		error_log.save();
	}
	
	
	public static void logError(int _error_code, String processment_mode,
			BigDecimal processment_id, String _java_class) throws Exception {

		ProcessmentErrorLog error_log = new ProcessmentErrorLog(_error_code);
		error_log.processment_group = error_log.getProcessment_group();
		error_log.processment_mode = processment_mode;
		error_log.processment_id = processment_id;
		error_log.java_class = _java_class;

		error_log.save();
	}

	private ProcessmentErrorLog(int _error_code) throws Exception {
		ResultSet rs = getErrorByCode(_error_code);
		int i = 0;
		while (rs.next()) {
			if (i > 0) {
				throw new Exception("more then one erro found with this error_code: " + _error_code);
			}

			name = rs.getString("name");
			description = rs.getString("description");
			error_code = rs.getInt("error_code");
			processment_errors_id = rs.getBigDecimal("id");
			i++;
		}
	}

	private ProcessmentErrorLog(BigDecimal error_id) throws Exception {
		ResultSet rs = getErrorById(error_id);
		int i = 0;
		while (rs.next()) {
			if (i > 0) {
				throw new Exception("more then one erro found with this error_id: " + error_id);
			}

			name = rs.getString("name");
			description = rs.getString("description");
			error_code = rs.getInt("error_code");
			processment_errors_id = rs.getBigDecimal("id");
			i++;
		}
	}

	private void save() throws Exception {
		if (name == null || name.equals("") || description == null || description.equals("") || error_code == 0
				|| processment_group == null || processment_group.equals("") || processment_mode == null
				|| processment_mode.equals("") || processment_id == null || processment_errors_id == null) {
			throw new Exception("ProcessmentErrorLog not saved, missing attributes values");
		}
		String compiledQuery = "INSERT INTO  Intellect.processment_errors_log("
				+ "name, description, error_code, processment_group, processment_mode, "
				+ "processment_id, processment_errors_id, java_class, created_at) VALUES " + "(?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement preparedStatement;
		try {
			preparedStatement = DBConnectionHelper.getConn().prepareStatement(compiledQuery);
			preparedStatement.setString(1, name);
			preparedStatement.setString(2, description);
			preparedStatement.setInt(3, error_code);
			preparedStatement.setString(4, processment_group);
			preparedStatement.setString(5, processment_mode);
			preparedStatement.setBigDecimal(6, processment_id);
			preparedStatement.setBigDecimal(7, processment_errors_id);
			preparedStatement.setString(8, java_class);
			preparedStatement.setLong(9, System.currentTimeMillis());
			preparedStatement.execute();
			if(!DBConnectionHelper.getConn().getAutoCommit())
				DBConnectionHelper.getConn().commit();	
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			throw new Exception(e.getMessage(), e);
		}
	}

	private ResultSet getErrorById(BigDecimal error_id) throws Exception {
		Statement stmt;
		try {
			stmt = DBConnectionHelper.getConn().createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM Intellect.processment_errors " + "WHERE id = " + error_id);
			return rs;
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
			throw new Exception(e1);
		}
		
	}

	private ResultSet getErrorByCode(int error_code) throws Exception {
		Statement stmt;
		try {
			stmt = DBConnectionHelper.getConn().createStatement();
			ResultSet rs = stmt
					.executeQuery("SELECT * FROM Intellect.processment_errors " + "WHERE error_code = " + error_code);
			return rs;
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
			throw new Exception(e1);
		}
	}
	
	public void finalize() {
		
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the error_code
	 */
	public int getError_code() {
		return error_code;
	}

	/**
	 * @param error_code the error_code to set
	 */
	public void setError_code(int error_code) {
		this.error_code = error_code;
	}

	/**
	 * @return the processment_group
	 */
	public String getProcessment_group() {
		return processment_group;
	}

	/**
	 * @param processment_group the processment_group to set
	 */
	public void setProcessment_group(String processment_group) {
		this.processment_group = processment_group;
	}

	/**
	 * @return the processment_mode
	 */
	public String getProcessment_mode() {
		return processment_mode;
	}

	/**
	 * @param processment_mode the processment_mode to set
	 */
	public void setProcessment_mode(String processment_mode) {
		this.processment_mode = processment_mode;
	}

	/**
	 * @return the processment_id
	 */
	public BigDecimal getProcessment_id() {
		return processment_id;
	}

	/**
	 * @param processment_id the processment_id to set
	 */
	public void setProcessment_id(BigDecimal processment_id) {
		this.processment_id = processment_id;
	}

	/**
	 * @return the processment_errors_id
	 */
	public BigDecimal getProcessment_errors_id() {
		return processment_errors_id;
	}

	/**
	 * @param processment_errors_id the processment_errors_id to set
	 */
	public void setProcessment_errors_id(BigDecimal processment_errors_id) {
		this.processment_errors_id = processment_errors_id;
	}

}
