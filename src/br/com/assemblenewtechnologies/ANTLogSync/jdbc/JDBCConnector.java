/**
 * 
 */
package br.com.assemblenewtechnologies.ANTLogSync.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;

/**
 * @author mcloc
 *
 */
public class JDBCConnector {
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCConnector.class);
	private static Connection _conn;
	private static Connection _csv_conn;
	private static Connection _etl_conn;

	/**
	 * Create JDBC Connection using GlobalProperties FIXME: GlobalProperties must be
	 * hydrated by .ini file It's hard coded in the class at this moment
	 * 
	 * @param _GlobalProperties.getInstance()
	 * @return
	 * @throws Exception 
	 */
	public JDBCConnector() throws Exception {
		if (!GlobalProperties.getInstance().getDbms().equals("postgres")) {
			LOGGER.error("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
			throw new SQLException("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
		}
		
		Properties connectionProps = new Properties();
		connectionProps.put("user", GlobalProperties.getInstance().getDbUser());
		connectionProps.put("password", GlobalProperties.getInstance().getDbPassword());
		connectionProps.put("reWriteBatchedInserts", true);
		if (GlobalProperties.getInstance().getDbms().equals("postgres")) {
			String url = "jdbc:postgresql://" + GlobalProperties.getInstance().getDbHost() + "/"
					+ GlobalProperties.getInstance().getDbDatabaseName();

			LOGGER.debug("Trying to connect to database: " + GlobalProperties.getInstance().getDbDatabaseName());
			try {
				_conn =  DriverManager.getConnection(url, connectionProps);
				_conn.setAutoCommit(false);
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
				throw new Exception("No database connection...");
			}
		}
	}

	/**
	 * get a NEW CONNECTION. You're Responsible to manage and close() this connection
	 * AutoCommit = false, you must explicit call connection.commit() or rollback()
	 * @return
	 * @throws Exception
	 */
	public static Connection getConn() throws Exception {
		if(_conn == null || _conn.isClosed())
			return getNewConn();
		
		return _conn;
	}
	
	
	public static Connection getNewConn() throws Exception {
		Properties connectionProps = new Properties();
		connectionProps.put("user", GlobalProperties.getInstance().getDbUser());
		connectionProps.put("password", GlobalProperties.getInstance().getDbPassword());
		connectionProps.put("reWriteBatchedInserts", true);
		if (GlobalProperties.getInstance().getDbms().equals("postgres")) {
			String url = "jdbc:postgresql://" + GlobalProperties.getInstance().getDbHost() + "/"
					+ GlobalProperties.getInstance().getDbDatabaseName();

			LOGGER.debug("Trying to connect to database: " + GlobalProperties.getInstance().getDbDatabaseName());
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

	/**
	 * @return the _csv_conn
	 * @throws SQLException 
	 */
	public static Connection get_csv_conn() throws Exception {
		if(_csv_conn != null && !_csv_conn.isClosed())
			return _csv_conn;
		
		Properties connectionProps = new Properties();
		connectionProps.put("user", GlobalProperties.getInstance().getDbUser());
		connectionProps.put("password", GlobalProperties.getInstance().getDbPassword());
		connectionProps.put("reWriteBatchedInserts", true);
		if (GlobalProperties.getInstance().getDbms().equals("postgres")) {
			String url = "jdbc:postgresql://" + GlobalProperties.getInstance().getDbHost() + "/"
					+ GlobalProperties.getInstance().getDbDatabaseName();

			LOGGER.debug("Trying to connect to database: " + GlobalProperties.getInstance().getDbDatabaseName());
			try {
				_csv_conn =  DriverManager.getConnection(url, connectionProps);
				_csv_conn.setAutoCommit(true);
				
				return _csv_conn;
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
				throw new Exception("No database connection...");
			}
		}
		LOGGER.error("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
		throw new SQLException("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
	}

	/**
	 * @return the _etl_conn
	 * @throws Exception 
	 */
	public static Connection get_etl_conn() throws Exception {
		if(_etl_conn != null && !_etl_conn.isClosed())
			return _etl_conn;
		
		Properties connectionProps = new Properties();
		connectionProps.put("user", GlobalProperties.getInstance().getDbUser());
		connectionProps.put("password", GlobalProperties.getInstance().getDbPassword());
		connectionProps.put("reWriteBatchedInserts", true);
		if (GlobalProperties.getInstance().getDbms().equals("postgres")) {
			String url = "jdbc:postgresql://" + GlobalProperties.getInstance().getDbHost() + "/"
					+ GlobalProperties.getInstance().getDbDatabaseName();

			LOGGER.debug("Trying to connect to database: " + GlobalProperties.getInstance().getDbDatabaseName());
			try {
				_etl_conn =  DriverManager.getConnection(url, connectionProps);
				_etl_conn.setAutoCommit(false);
				
				return _etl_conn;
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
				throw new Exception("No database connection...");
			}
		}
		LOGGER.error("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
		throw new SQLException("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
	}

	/**
	 * @param _etl_conn the _etl_conn to set
	 */
	public static void set_etl_conn(Connection _etl_conn) {
		JDBCConnector._etl_conn = _etl_conn;
	}

	public static void close_all_conn() {
		try {
			if(_etl_conn != null && !_etl_conn.isClosed())
				_etl_conn.close();
			if(_csv_conn != null && !_csv_conn.isClosed())
				_csv_conn.close();
			if(_conn != null && !_conn.isClosed())
				_conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void close_etl_conn() {
		try {
			if(_etl_conn != null && !_etl_conn.isClosed())
				_etl_conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
	
	public static void close_csv_conn() {
		try {
			if(_csv_conn != null && !_csv_conn.isClosed())
				_csv_conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}

	
}
