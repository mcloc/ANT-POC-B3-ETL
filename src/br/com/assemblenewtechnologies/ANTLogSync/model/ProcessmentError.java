package br.com.assemblenewtechnologies.ANTLogSync.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessmentError {
	private String name; 
	private String description;
	private int error_code;
	
	private Logger LOGGER = LoggerFactory.getLogger(ProcessmentError.class);
	
	public ProcessmentError(ResultSet rs) throws Exception {
		try {
			name = rs.getString("name");
			description = rs.getString("description");
			error_code = rs.getInt("error_code");
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			throw new Exception(e);
		}
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
	
	
}
