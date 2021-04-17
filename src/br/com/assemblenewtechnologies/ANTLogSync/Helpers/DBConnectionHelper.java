package br.com.assemblenewtechnologies.ANTLogSync.Helpers;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.constants.ErrorCodes;
import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnection;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentErrorLog;

public class DBConnectionHelper {
	private static DBConnectionHelper dbInstance;

	private Logger LOGGER = LoggerFactory.getLogger(DBConnectionHelper.class);
	private GlobalProperties globalProperties = new GlobalProperties();
	private JDBCConnection jdbcConnection;


	public DBConnectionHelper() throws Exception {
		try {
			jdbcConnection = new JDBCConnection(globalProperties);
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			ProcessmentErrorLog.logError(ErrorCodes.DB_CONNECT_ERROR, globalProperties.getProcessmentMode(), null,
					this.getClass().getName());
			throw new Exception("No database connection...");
		}
	}

	public static synchronized DBConnectionHelper getInstance() throws Exception {
		if (dbInstance == null)
			dbInstance = new DBConnectionHelper();

		return dbInstance;
	}

	public static synchronized void close() throws Exception {
		if (dbInstance == null)
			return;

		dbInstance.jdbcConnection.connClose();
	}

	/**
	 * @return the jdbcConnection
	 */
	public JDBCConnection getJdbcConnection() {
		return jdbcConnection;
	}


}
