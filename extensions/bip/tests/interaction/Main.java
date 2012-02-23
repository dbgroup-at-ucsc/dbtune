package interaction;

import interaction.db.*;
import interaction.workload.*;

import java.io.*;
import java.sql.SQLException;

/*
 * This just has some static functions that are shared among other classes
 * in this package.
 */
public class Main {
	public static DBConnection globalConn; // for debugging only
	
	protected static DBConnection openConnection() throws SQLException {
		if (Configuration.password == null) {
			Configuration.password = interaction.util.PasswordPrompt.getPassword();
		}
		DBConnection conn = new DBConnection();
		conn.open(Configuration.dbName,
				  Configuration.url,
				  Configuration.userName,
				  Configuration.password,
				  Configuration.driverClass);
		return conn;
	}
	
	protected static SQLWorkload getWorkload() throws IOException {
		return new SQLWorkload(Configuration.queryListFile());
	}
}
