package interaction;

import interaction.db.*;
import interaction.workload.*;

import java.io.*;
import java.sql.SQLException;

public class Main {
	public static String sql;
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

//	public static void runSteps(File queryFile) {
//		DBConnection conn;
//		SQLWorkload xacts;
//		
//
//		
//		// open DB connection
//
//		globalConn = conn;
//		System.gc();	
//
//		// do analysis and close connection
//		try {
//			AnalysisMain.analyze(conn, xacts);
//		} catch (SQLException e) {
//			System.out.println(Main.sql);
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//		try {
//			conn.close();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
}
