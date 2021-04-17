package br.com.assemblenewtechnologies.ANTLogSync.model;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessmentExecution {
	private String name; 
	private String description;
	private BigDecimal processment_id;
	private int processment_seq;
	private String processment_group; 
	private String processment_mode;
	private boolean new_thread;
	
	private Logger LOGGER = LoggerFactory.getLogger(ProcessmentExecution.class);
	
	public ProcessmentExecution(String name, String description, BigDecimal processment_id, int processment_seq,
			String processment_group, String processment_mode, boolean new_thread) {
		this.name = name;
		this.description = description;
		this.processment_id = processment_id;
		this.processment_seq = processment_seq;
		this.processment_group = processment_group;
		this.processment_mode = processment_mode;
		this.new_thread = new_thread;
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
	 * @return the processment_seq
	 */
	public int getProcessment_seq() {
		return processment_seq;
	}

	/**
	 * @param processment_seq the processment_seq to set
	 */
	public void setProcessment_seq(int processment_seq) {
		this.processment_seq = processment_seq;
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
	 * @return the new_thread
	 */
	public boolean isNew_thread() {
		return new_thread;
	}

	/**
	 * @param new_thread the new_thread to set
	 */
	public void setNew_thread(boolean new_thread) {
		this.new_thread = new_thread;
	}
	
}
