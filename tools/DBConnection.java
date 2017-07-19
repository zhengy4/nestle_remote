package nestle_sandbox.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class DBConnection {
	private static Log logger = LogFactory.getLog(DBConnection.class);
	private static final String DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static Connection conn = null;	
	
	public static Connection getConnection(String dbhost, String authinfo, String dbname) {
		try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage());
		}
	
		try {
			//conn = DriverManager.getConnection("jdbc:sqlserver://" + dbhost + ";databaseName=" + dbname + ";user=" + dbuser  + ";password=" + dbpass);
			//conn = DriverManager.getConnection("jdbc:sqlserver://" + dbhost + ";databaseName=" + dbname + ";" + authinfo);
			conn = DriverManager.getConnection("jdbc:sqlserver://" + dbhost + ";database=" + dbname + ";" + authinfo);
			return conn;
		} catch (SQLException e) {
			logger.error(" inside DBconnector");
			logger.error(e.getMessage());
		}
		return conn;
	}
}