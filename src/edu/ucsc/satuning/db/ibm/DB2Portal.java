package edu.ucsc.satuning.db.ibm;

import java.io.File;

import edu.ucsc.satuning.Configuration;
import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.db.DBPortal;

public class DB2Portal implements DBPortal<DB2Index> {
	public DatabaseConnection<DB2Index> getConnection() {
		return new DB2Connection();
	}

	// the default parameters
	String dbName = "KARLSCH";
	String userName = "karlsch";
	String password = null;
	String url = "jdbc:db2://localhost:48459/karlsch";
	String driverClass = "com.ibm.db2.jcc.DB2Driver";
	File passwordFile = new File(Configuration.root, "DB2password");
	
	public String dbName() { return dbName; }
	public String driverClass() { return driverClass; }
	public String password() { return password; }
	public File passwordFile() { return passwordFile; }
	public void setPassword(String p) { password = p; }
	public String url() { return url; }
	public String userName() { return userName; }
}
