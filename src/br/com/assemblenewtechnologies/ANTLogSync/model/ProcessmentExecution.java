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
		this.save();
	}
	
	
	private void save() throws Exception {
		if (processment_mode.equals("")) {
			connection.rollback();
			connection.close();
			throw new Exception("ProcessmentErrorLog not saved, missing attributes values");
		}
		
		connection = DBConnectionHelper.getNewConn();

		String compiledQuery = "INSERT INTO  Intellect.processment_errors_log("
				+ "processment_mode, created_at, updated_at) VALUES " + "(?, ?, ?)";
		PreparedStatement preparedStatement;
		long _now;
		try {
			_now = System.currentTimeMillis();
			preparedStatement = connection.prepareStatement(compiledQuery,
                    Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, processment_mode);
			preparedStatement.setLong(2, _now);
			preparedStatement.setLong(3, _now);
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
