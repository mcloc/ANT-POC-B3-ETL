package br.com.assemblenewtechnologies.ANTLogSync.controller;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.Helpers.DBConnectionHelper;
import br.com.assemblenewtechnologies.ANTLogSync.constants.ErrorCodes;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentError;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentErrorLog;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentRotine;

public class ControllerData {
	private static Logger LOGGER = LoggerFactory.getLogger(ControllerData.class);
	private Connection connection;

	private Map<Integer, ProcessmentError> errors = new LinkedHashMap<Integer, ProcessmentError>();
	private Map<Integer, ProcessmentRotine> processment_rotines = new LinkedHashMap<Integer, ProcessmentRotine>();

	public ControllerData() throws Exception {

		connection = DBConnectionHelper.getNewConn();
		connection.setAutoCommit(false);

		load_errors();
		load_rotines();
//		connection.commit(); // no need
		connection.close();
	}

	public void reload() throws Exception {
		errors = new LinkedHashMap<Integer, ProcessmentError>();
		processment_rotines = new LinkedHashMap<Integer, ProcessmentRotine>();

		load_errors();
		load_rotines();
//		connection.commit(); // no need
		connection.close();
	}

	private void load_rotines() throws Exception {
		LOGGER.info("Fetching processment_rotines for mode: " + GlobalProperties.getInstance().getProcessmentMode());
		Statement stmt;
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM Intellect.processment_rotines "
					+ "WHERE processment_mode = '" + GlobalProperties.getInstance().getProcessmentMode() + "' "
					+ "AND active_status = true " + "ORDER BY processment_seq");
			while (rs.next()) {
				ProcessmentRotine rotine;
				try {
					rotine = new ProcessmentRotine(rs);
					processment_rotines.put(rotine.getProcessment_seq(), rotine);
				} catch (Exception e) {
					LOGGER.error(e.getMessage());
					ProcessmentErrorLog.logError(ErrorCodes.STARTUP_ERROR, GlobalProperties.getInstance().getProcessmentMode(), null,
							this.getClass().getName());
					throw new Exception(e);
				}

			}
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
			ProcessmentErrorLog.logError(ErrorCodes.DB_EXCEPTION_ERROR, GlobalProperties.getInstance().getProcessmentMode(), null,
					this.getClass().getName());
			throw new Exception(e1);
		}

		LOGGER.info("Fetched " + processment_rotines.size() + " processment_rotines for mode: "
				+ GlobalProperties.getInstance().getProcessmentMode());
	}

	private void load_errors() throws Exception {
		Statement stmt;
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM Intellect.processment_errors " + "ORDER BY error_code");
			while (rs.next()) {
				ProcessmentError error;
				try {
					error = new ProcessmentError(rs);
					errors.put(error.getError_code(), error);
				} catch (Exception e) {
					LOGGER.error(e.getMessage());
					ProcessmentErrorLog.logError(ErrorCodes.STARTUP_ERROR, GlobalProperties.getInstance().getProcessmentMode(), null,
							this.getClass().getName());
					throw new Exception(e);
				}

			}
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
			ProcessmentErrorLog.logError(ErrorCodes.DB_EXCEPTION_ERROR, GlobalProperties.getInstance().getProcessmentMode(), null,
					this.getClass().getName());
			throw new Exception(e1);
		}
		LOGGER.info("Fetched " + errors.size() + " in errors_map");
	}

	/**
	 * @return the errors
	 */
	public Map<Integer, ProcessmentError> getErrors() {
		return errors;
	}

	/**
	 * @return the processment_rotines
	 */
	public Map<Integer, ProcessmentRotine> getProcessment_rotines() {
		return processment_rotines;
	}

}
