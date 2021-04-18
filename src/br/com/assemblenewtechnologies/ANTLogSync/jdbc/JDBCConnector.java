/**
 * 
 */
package br.com.assemblenewtechnologies.ANTLogSync.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.constants.ErrorCodes;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentErrorLog;

/**
 * @author mcloc
 *
 */
public class JDBCConnector {
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCConnector.class);

	private GlobalProperties globalProperties = new GlobalProperties();

	/**
	 * Create JDBC Connection using GlobalProperties FIXME: GlobalProperties must be
	 * hydrated by .ini file It's hard coded in the class at this moment
	 * 
	 * @param _globalProperties
	 * @return
	 * @throws SQLException
	 */
	public JDBCConnector() throws SQLException {
		if (!globalProperties.getDbms().equals("postgres")) {
			LOGGER.error("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
			throw new SQLException("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
		}
	}

	/**
	 * get a NEW CONNECTION. You're Responsible to manage and close() this connection
	 * AutoCommit = false, you must explicit call connection.commit() or rollback()
	 * @return
	 * @throws Exception
	 */
	public Connection getNewConn() throws Exception {
		Properties connectionProps = new Properties();
		connectionProps.put("user", globalProperties.getDbUser());
		connectionProps.put("password", globalProperties.getDbPassword());
		connectionProps.put("reWriteBatchedInserts", true);
		Connection _conn;
		if (globalProperties.getDbms().equals("postgres")) {
			String url = "jdbc:postgresql://" + globalProperties.getDbHost() + "/"
					+ globalProperties.getDbDatabaseName();

			LOGGER.debug("Trying to connect to database: " + globalProperties.getDbDatabaseName());
			try {
				_conn =  DriverManager.getConnection(url, connectionProps);
				_conn.setAutoCommit(false);
				
				return _conn;
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
				throw new Exception("No database connection...");
			}
		}
		
		LOGGER.error("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
		throw new SQLException("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
	}
}
