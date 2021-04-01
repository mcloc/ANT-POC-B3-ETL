package br.com.assemblenewtechnologies.ANTLogSync;

public class GlobalProperties {

	private String DB_HOST = "localhost";
	private int DB_PORT = 5432;
	private String DB_USER = "ANTOption";
	private String DB_PASSWORD = "lacuna";
	private String DB_DATABASE_NAME = "ANTOption";
	private String DB_LOAD_SCHEMA = "B3Log";
	private String DB_ETL_SCHEMA = "B3Log";
	private String DB_EXPERTSYSTEM_SCHEMA = "B3Log";
	private String DB_AUDIT_LOG_SCHEMA = "B3Log";
	private String dbms = "postgres";

	// TODO: getAllFrom database with rest request to web_admin
	private String SHM_ADDRESS_READ = "/dev/shm/serial2arduinoRead";
	private String SHM_ADDRESS_WRITE = "/dev/shm/serial2arduinoWrite";
	private String SHM_ADDRESS_READ_LOCK = "/dev/shm/serial2arduinoReadLock";
	private String SHM_ADDRESS_WRITE_LOCK = "/dev/shm/serial2arduinoWriteLock";
	private Integer BAUD_RATE = 115200;
	private Integer IPC_SERVER_PORT = 1932;
	private Integer REST_RESPONSE_SLEEP = 600;
	private Integer REST_429_SLEEP = 4500;
	private Integer REST_MODULE_SLEEP = 2500;

	/**
	 * GETTERS AND SETTERS
	 * 
	 * @return
	 */

	public String getSHM_ADDRESS_READ() {
		return SHM_ADDRESS_READ;
	}

	/**
	 * @return the dB_ETL_SCHEMA
	 */
	public String getDB_ETL_SCHEMA() {
		return DB_ETL_SCHEMA;
	}

	/**
	 * @param dB_ETL_SCHEMA the dB_ETL_SCHEMA to set
	 */
	public void setDB_ETL_SCHEMA(String dB_ETL_SCHEMA) {
		DB_ETL_SCHEMA = dB_ETL_SCHEMA;
	}

	/**
	 * @return the dB_EXPERTSYSTEM_SCHEMA
	 */
	public String getDB_EXPERTSYSTEM_SCHEMA() {
		return DB_EXPERTSYSTEM_SCHEMA;
	}

	/**
	 * @param dB_EXPERTSYSTEM_SCHEMA the dB_EXPERTSYSTEM_SCHEMA to set
	 */
	public void setDB_EXPERTSYSTEM_SCHEMA(String dB_EXPERTSYSTEM_SCHEMA) {
		DB_EXPERTSYSTEM_SCHEMA = dB_EXPERTSYSTEM_SCHEMA;
	}

	/**
	 * @return the dB_AUDIT_LOG_SCHEMA
	 */
	public String getDB_AUDIT_LOG_SCHEMA() {
		return DB_AUDIT_LOG_SCHEMA;
	}

	/**
	 * @param dB_AUDIT_LOG_SCHEMA the dB_AUDIT_LOG_SCHEMA to set
	 */
	public void setDB_AUDIT_LOG_SCHEMA(String dB_AUDIT_LOG_SCHEMA) {
		DB_AUDIT_LOG_SCHEMA = dB_AUDIT_LOG_SCHEMA;
	}

	/**
	 * @return the dB_DATABASE_NAME
	 */
	public String getDB_DATABASE_NAME() {
		return DB_DATABASE_NAME;
	}

	/**
	 * @param dB_DATABASE_NAME the dB_DATABASE_NAME to set
	 */
	public void setDB_DATABASE_NAME(String dB_DATABASE_NAME) {
		DB_DATABASE_NAME = dB_DATABASE_NAME;
	}

	/**
	 * @return the dbms
	 */
	public String getDbms() {
		return dbms;
	}

	/**
	 * @param dbms the dbms to set
	 */
	public void setDbms(String dbms) {
		this.dbms = dbms;
	}

	/**
	 * @return the dB_HOST
	 */
	public String getDB_HOST() {
		return DB_HOST;
	}

	/**
	 * @param dB_HOST the dB_HOST to set
	 */
	public void setDB_HOST(String dB_HOST) {
		DB_HOST = dB_HOST;
	}

	/**
	 * @return the dB_PORT
	 */
	public int getDB_PORT() {
		return DB_PORT;
	}

	/**
	 * @param dB_PORT the dB_PORT to set
	 */
	public void setDB_PORT(int dB_PORT) {
		DB_PORT = dB_PORT;
	}

	/**
	 * @return the dB_USER
	 */
	public String getDB_USER() {
		return DB_USER;
	}

	/**
	 * @param dB_USER the dB_USER to set
	 */
	public void setDB_USER(String dB_USER) {
		DB_USER = dB_USER;
	}

	/**
	 * @return the dB_PASSWORD
	 */
	public String getDB_PASSWORD() {
		return DB_PASSWORD;
	}

	/**
	 * @param dB_PASSWORD the dB_PASSWORD to set
	 */
	public void setDB_PASSWORD(String dB_PASSWORD) {
		DB_PASSWORD = dB_PASSWORD;
	}

	/**
	 * @return the dB_LOAD_SCHEMA
	 */
	public String getDB_LOAD_SCHEMA() {
		return DB_LOAD_SCHEMA;
	}

	/**
	 * @param dB_LOAD_SCHEMA the dB_LOAD_SCHEMA to set
	 */
	public void setDB_LOAD_SCHEMA(String dB_LOAD_SCHEMA) {
		DB_LOAD_SCHEMA = dB_LOAD_SCHEMA;
	}

	public void setSHM_ADDRESS_READ(String sHM_ADDRESS_READ) {
		SHM_ADDRESS_READ = sHM_ADDRESS_READ;
	}

	public String getSHM_ADDRESS_WRITE() {
		return SHM_ADDRESS_WRITE;
	}

	public void setSHM_ADDRESS_WRITE(String sHM_ADDRESS_WRITE) {
		SHM_ADDRESS_WRITE = sHM_ADDRESS_WRITE;
	}

	public String getSHM_ADDRESS_READ_LOCK() {
		return SHM_ADDRESS_READ_LOCK;
	}

	public void setSHM_ADDRESS_READ_LOCK(String sHM_ADDRESS_READ_LOCK) {
		SHM_ADDRESS_READ_LOCK = sHM_ADDRESS_READ_LOCK;
	}

	public String getSHM_ADDRESS_WRITE_LOCK() {
		return SHM_ADDRESS_WRITE_LOCK;
	}

	public void setSHM_ADDRESS_WRITE_LOCK(String sHM_ADDRESS_WRITE_LOCK) {
		SHM_ADDRESS_WRITE_LOCK = sHM_ADDRESS_WRITE_LOCK;
	}

	public Integer getBAUD_RATE() {
		return BAUD_RATE;
	}

	public void setBAUD_RATE(Integer bAUD_RATE) {
		BAUD_RATE = bAUD_RATE;
	}

	public Integer getIPC_SERVER_PORT() {
		return IPC_SERVER_PORT;
	}

	public void setIPC_SERVER_PORT(Integer iPC_SERVER_PORT) {
		IPC_SERVER_PORT = iPC_SERVER_PORT;
	}

	public Integer getREST_RESPONSE_SLEEP() {
		return REST_RESPONSE_SLEEP;
	}

	public void setREST_RESPONSE_SLEEP(Integer rEST_RESPONSE_SLEEP) {
		REST_RESPONSE_SLEEP = rEST_RESPONSE_SLEEP;
	}

	public Integer getREST_429_SLEEP() {
		return REST_429_SLEEP;
	}

	public void setREST_429_SLEEP(Integer rEST_429_SLEEP) {
		REST_429_SLEEP = rEST_429_SLEEP;
	}

	public Integer getREST_MODULE_SLEEP() {
		return REST_MODULE_SLEEP;
	}

	public void setREST_MODULE_SLEEP(Integer rEST_MODULE_SLEEP) {
		REST_MODULE_SLEEP = rEST_MODULE_SLEEP;
	}

}
