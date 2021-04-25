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

public class ProcessmentExecution {
	private BigDecimal id; 
	private String processment_mode;
	private Timestamp created_at;
	private Timestamp updated_at;
	private Integer status;
	
	public static final int STATUS_START = 0;
	public static final int STATUS_EXECUTING = 1;
	public static final int STATUS_FINISHED = 3;
	public static final int STATUS_FINISHED_ERRORS = -1;
	public static final int STATUS_FINISHED_INTERRUPTED = -2;
	
	private static Logger LOGGER = LoggerFactory.getLogger(ProcessmentExecution.class);
	private Connection connection;
	
	/**
	 * Create and save on database
	 * 
	 * @param processment_mode
	 * @throws Exception 
	 */
	public ProcessmentExecution(String processment_mode) throws Exception {
		this.processment_mode = processment_mode;
		this.status = 0;
		this.save();
	}
	
	public void updateStatus(Integer status) throws Exception {
		connection = DBConnectionHelper.getNewConn();
		String compiledQuery = "UPDATE Intellect.processment_execution("
				+ "status, updated_at) VALUES " + "(?, ?) WHERE id = "+this.id;
		PreparedStatement preparedStatement;
		long _now;
		Timestamp _updated_at;
		try {
			_now = System.currentTimeMillis();
			_updated_at = new Timestamp(_now);
			preparedStatement = connection.prepareStatement(compiledQuery);
			preparedStatement.setInt(1, status);
			preparedStatement.setTimestamp(2, _updated_at);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			connection.rollback();
			connection.close();
			throw new Exception(e.getMessage(), e);
		}
		connection.commit();
		this.status = status;
		this.updated_at = _updated_at;
		
	}
	
	
	private void save() throws Exception {
		if (processment_mode.equals("")) {
			connection.rollback();
			connection.close();
			throw new Exception("ProcessmentErrorLog not saved, missing attributes values");
		}
		
		connection = DBConnectionHelper.getNewConn();

		String compiledQuery = "INSERT INTO  Intellect.processment_execution("
				+ "processment_mode, status, created_at, updated_at) VALUES " + "(?, ?, ?, ?)";
		PreparedStatement preparedStatement;
		long _now;
		try {
			_now = System.currentTimeMillis();
			preparedStatement = connection.prepareStatement(compiledQuery,
                    Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, processment_mode);
			preparedStatement.setInt(2, status);
			preparedStatement.setTimestamp(3, new Timestamp(_now));
			preparedStatement.setTimestamp(4, new Timestamp(_now));
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
			if(i > 0) {
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

	
	
	
}
