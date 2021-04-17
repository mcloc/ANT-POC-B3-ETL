package br.com.assemblenewtechnologies.ANTLogSync;

public class GlobalProperties {

	private final String PROCESSMENT_MODE = "batch_process";
	private final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	// DB SETTINGS
	private final String DB_HOST = "localhost";
	private final int DB_PORT = 5432;
	private final String DB_USER = "ANTOption";
	private final String DB_PASSWORD = "lacuna";
	private final String DB_DATABASE_NAME = "ANTOption";
	private final String DB_LOAD_SCHEMA = "B3Log";
	private final String DB_ETL_SCHEMA = "Intellect";
	private final String DB_EXPERTSYSTEM_SCHEMA = "Intellect";
	private final String DB_AUDIT_LOG_SCHEMA = "Intellect";
	private final String dbms = "postgres";

	// PATH SEETINGS
	private final String RTD_DIRETCTORY = "/ANT-TOOLCHAIN/ANTOption/data_load/";
	private final String ARCHIVE_DIRETCTORY = "/ANT-TOOLCHAIN/ANTOption/archive/";
	private final String ARCHIVE_BUFFER_DIRETCTORY = "/ANT-TOOLCHAIN/ANTOption/archive_buffer/";

	/**
	 * @return the processmentMode
	 */
	public final String getProcessmentMode() {
		return PROCESSMENT_MODE;
	}

	/**
	 * @return the fileSeparator
	 */
	public final String getFileSeparator() {
		return FILE_SEPARATOR;
	}

	/**
	 * @return the dbHost
	 */
	public final String getDbHost() {
		return DB_HOST;
	}

	/**
	 * @return the dbPort
	 */
	public final int getDbPort() {
		return DB_PORT;
	}

	/**
	 * @return the dbUser
	 */
	public final String getDbUser() {
		return DB_USER;
	}

	/**
	 * @return the dbPassword
	 */
	public final String getDbPassword() {
		return DB_PASSWORD;
	}

	/**
	 * @return the dbDatabaseName
	 */
	public final String getDbDatabaseName() {
		return DB_DATABASE_NAME;
	}

	/**
	 * @return the dbLoadSchema
	 */
	public final String getDbLoadSchema() {
		return DB_LOAD_SCHEMA;
	}

	/**
	 * @return the dbEtlSchema
	 */
	public final String getDbEtlSchema() {
		return DB_ETL_SCHEMA;
	}

	/**
	 * @return the dbExpertsystemSchema
	 */
	public final String getDbExpertsystemSchema() {
		return DB_EXPERTSYSTEM_SCHEMA;
	}

	/**
	 * @return the dbAuditLogSchema
	 */
	public final String getDbAuditLogSchema() {
		return DB_AUDIT_LOG_SCHEMA;
	}

	/**
	 * @return the dbms
	 */
	public final String getDbms() {
		return dbms;
	}

	/**
	 * @return the rtdDiretctory
	 */
	public final String getRtdDiretctory() {
		return RTD_DIRETCTORY;
	}

	/**
	 * @return the archiveDiretctory
	 */
	public final String getArchiveDiretctory() {
		return ARCHIVE_DIRETCTORY;
	}

	/**
	 * @return the archiveBufferDiretctory
	 */
	public final String getArchiveBufferDiretctory() {
		return ARCHIVE_BUFFER_DIRETCTORY;
	}

}
