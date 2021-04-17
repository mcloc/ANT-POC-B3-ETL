package br.com.assemblenewtechnologies.ANTLogSync.model;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessmentErrorLog {
	private String name; 
	private String description;
	private int error_code;
	private String processment_group; 
	private String processment_mode;
	private BigDecimal processment_id; 
	private BigDecimal processment_errors_id;
	
	private Logger LOGGER = LoggerFactory.getLogger(ProcessmentErrorLog.class);
	
	public ProcessmentErrorLog() throws Exception {
		
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
