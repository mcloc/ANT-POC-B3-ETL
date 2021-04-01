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

/**
 * @author mcloc
 *
 */
public class JDBCConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCConnection.class);

	private GlobalProperties globalProperties;
	private Connection conn = null;

//	private static final JDBCConnection = new JDBCConnection(globalProperties)

	/**
	 * Create JDBC Connection using GlobalProperties FIXME: GlobalProperties must be
	 * hydrated by .ini file It's hard coded in the class at this moment
	 * 
	 * @param _globalProperties
	 * @return
	 * @throws SQLException
	 */
	public JDBCConnection(GlobalProperties _globalProperties) throws SQLException {
		globalProperties = _globalProperties;
		globalProperties = _globalProperties;
		Properties connectionProps = new Properties();
		connectionProps.put("user", globalProperties.getDB_USER());
		connectionProps.put("password", globalProperties.getDB_PASSWORD());
		connectionProps.put("reWriteBatchedInserts", true);



		if (globalProperties.getDbms().equals("postgres")) {
			String url = "jdbc:postgresql://" + globalProperties.getDB_HOST() + "/"
					+ globalProperties.getDB_DATABASE_NAME();
			
			LOGGER.info("Trying to connect to database: " + globalProperties.getDB_DATABASE_NAME());
			conn = DriverManager.getConnection(url, connectionProps);
		} else {
			LOGGER.error("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
			throw new SQLException("DMBS properties not implemented. Only 'postgres' is allowed at the momment");
		}
		LOGGER.info("Connected to database");
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		ResultSet rs = null;
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
			throw new SQLException(e.getMessage());
//			e.printStackTrace();
		}

		return rs;
	}

	/**
	 * @return the conn
	 */
	public Connection getConn() {
		return conn;
	}

	/**
	 * @param conn the conn to set
	 */
	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public void connClose() {
		try {
			this.conn.close();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
//			e.printStackTrace();
		}
	}

}
