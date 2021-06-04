package br.com.assemblenewtechnologies.ANTLogSync.Helpers;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.assemblenewtechnologies.ANTLogSync.GlobalProperties;
import br.com.assemblenewtechnologies.ANTLogSync.constants.ErrorCodes;
import br.com.assemblenewtechnologies.ANTLogSync.jdbc.JDBCConnector;
import br.com.assemblenewtechnologies.ANTLogSync.model.ProcessmentErrorLog;

public class DBConnectionHelper {
	private static DBConnectionHelper dbInstance;

	private Logger LOGGER = LoggerFactory.getLogger(DBConnectionHelper.class);
	
//	private static JDBCConnector jdbcConnector;


	public DBConnectionHelper() throws Exception {
//		try {
//			jdbcConnector = new JDBCConnector();
//		} catch (SQLException e) {
//			LOGGER.error(e.getMessage());
////			ProcessmentErrorLog.logError(ErrorCodes.DB_CONNECT_ERROR, GlobalProperties.getInstance().getProcessmentMode(), null,
////					this.getClass().getName());
//			throw new Exception("No database connection...");
//		}
	}

	public static synchronized DBConnectionHelper getInstance() throws Exception {
		if (dbInstance == null)
			dbInstance = new DBConnectionHelper();

		return dbInstance;
	}
	

	/**
	 * It's mandatory to developer on the other hand manage and CLOSE this connection
	 * @return
	 * @throws Exception
	 */
	public static Connection getConn() throws Exception {
		if (dbInstance == null)
			throw new Exception("No instance of DBConnectionHelper found");

		return JDBCConnector.getConn();
	}
	
	public static Connection getNewConn() throws Exception {
		if (dbInstance == null)
			throw new Exception("No instance of DBConnectionHelper found");

		return JDBCConnector.getNewConn();
	}

	
	public static Connection getCSVConn() throws Exception {
		if (dbInstance == null)
			throw new Exception("No instance of DBConnectionHelper found");

		return JDBCConnector.get_csv_conn();
	}
	
	public static Connection getETLConn() throws Exception {
		if (dbInstance == null)
			throw new Exception("No instance of DBConnectionHelper found");

		return JDBCConnector.get_etl_conn();
	}
	
	public static Connection getETL2Conn() throws Exception {
		if (dbInstance == null)
			throw new Exception("No instance of DBConnectionHelper found");

		return JDBCConnector.get_etl2_conn();
	}
	
	public static void closeAllConnections() {
		JDBCConnector.close_all_conn();
	}

	public static void closeETLConn() {
		JDBCConnector.close_etl_conn();
		JDBCConnector.close_etl2_conn();
		
	}
	
	public static void closeCSVConn() {
		JDBCConnector.close_csv_conn();
		
	}

	public static Connection getNewNonStaticConn() throws Exception {
		return JDBCConnector.getNewNonStaticConn();
	}
}
