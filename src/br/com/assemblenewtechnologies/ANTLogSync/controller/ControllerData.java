package br.com.assemblenewtechnologies.ANTLogSync.controller;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnection;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentError;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentRotine;

public class ControllerData {
	private static Logger LOGGER = LoggerFactory.getLogger(ControllerData.class);
	private GlobalProperties globalProperties = new GlobalProperties();
	private JDBCConnection jdbcConnection;
	private Connection connection;
	
	private Map<Integer, ProcessmentError> errors = new HashMap<Integer, ProcessmentError>();
	private Map<Integer, ProcessmentRotine> processment_rotines = new LinkedHashMap<Integer, ProcessmentRotine>();

	public ControllerData() throws Exception {

		try {
			jdbcConnection = new JDBCConnection(globalProperties);
			connection = jdbcConnection.getConn();
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			throw new Exception("No database connection...");
		}
		
		load_errors();
		load_rotines();


	}
	
	
	private void load_rotines() throws Exception {
		LOGGER.info("Fetching processment_rotines:");
		Statement stmt;
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM Intellect.processment_rotines "+
					"WHERE processment_mode = '"+globalProperties.getPROCESSMENT_MODE()+"' " +
					"AND active_status = true " +
					"ORDER BY processment_seq");
			while (rs.next()) {
				ProcessmentRotine rotine;
				try {
					rotine = new ProcessmentRotine(rs);
					processment_rotines.put(rotine.getProcessment_seq(), rotine);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		} catch (SQLException e1) {
			LOGGER.error(e1.getMessage());
			throw new Exception(e1);
		}

		
	}


	private void load_errors() {
		// TODO Auto-generated method stub
		
	}
	
	


	public void closeConnection() {
		try {
			connection.close();
			jdbcConnection.connClose();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
		}
	}

	/**
	 * @return the globalProperties
	 */
	public GlobalProperties getGlobalProperties() {
		return globalProperties;
	}

	/**
	 * @param globalProperties the globalProperties to set
	 */
	public void setGlobalProperties(GlobalProperties globalProperties) {
		this.globalProperties = globalProperties;
	}

	/**
	 * @return the jdbcConnection
	 */
	public JDBCConnection getJdbcConnection() {
		return jdbcConnection;
	}

	/**
	 * @param jdbcConnection the jdbcConnection to set
	 */
	public void setJdbcConnection(JDBCConnection jdbcConnection) {
		this.jdbcConnection = jdbcConnection;
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


	/**
	 * @return the errors
	 */
	public Map<Integer, ProcessmentError> getErrors() {
		return errors;
	}


	/**
	 * @param errors the errors to set
	 */
	public void setErrors(Map<Integer, ProcessmentError> errors) {
		this.errors = errors;
	}


	/**
	 * @return the processment_rotines
	 */
	public Map<Integer, ProcessmentRotine> getProcessment_rotines() {
		return processment_rotines;
	}


	/**
	 * @param processment_rotines the processment_rotines to set
	 */
	public void setProcessment_rotines(Map<Integer, ProcessmentRotine> processment_rotines) {
		this.processment_rotines = processment_rotines;
	}

	
	
}
