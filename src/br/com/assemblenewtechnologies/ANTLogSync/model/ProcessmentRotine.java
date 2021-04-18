package br.com.assemblenewtechnologies.ANTLogSync.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.constants.ErrorCodes;

public class ProcessmentRotine {
	private String name; 
	private String description;
	private boolean active_status;
	private int processment_seq;
	private String processment_group; 
	private String processment_mode;
	private boolean new_thread;
	
	private Logger LOGGER = LoggerFactory.getLogger(ProcessmentRotine.class);
	
	public ProcessmentRotine(ResultSet rs) throws Exception {
		try {
			name = rs.getString("name");
			description = rs.getString("description");
			active_status = rs.getBoolean("active_status");
			processment_seq = rs.getInt("processment_seq");
			processment_group = rs.getString("processment_group");
			processment_mode = rs.getString("processment_mode");
			new_thread = rs.getBoolean("new_thread");
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			ProcessmentErrorLog.logError(ErrorCodes.DB_EXCEPTION_ERROR, GlobalProperties.getInstance().getProcessmentMode(), null,
					this.getClass().getName());
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
	 * @return the active_status
	 */
	public boolean isActive_status() {
		return active_status;
	}
	/**
	 * @param active_status the active_status to set
	 */
	public void setActive_status(boolean active_status) {
		this.active_status = active_status;
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
