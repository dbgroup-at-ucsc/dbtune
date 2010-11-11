package edu.ucsc.satuning.db.pg;

import java.io.File;

import edu.ucsc.satuning.Configuration;
import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.db.DBPortal;

public class PGPortal implements DBPortal<PGIndex> {
	public DatabaseConnection<PGIndex> getConnection() {
		return new PGConnection();
	}

	// the default parameters
	public String dbName = "benchmark";
	public String userName = "karlsch";
	public String password = null;
	public String url = "jdbc:postgresql://localhost:5432/benchmark";
	public String driverClass = "org.postgresql.Driver";
	public File passwordFile = new File(Configuration.root, "PGpassword");
	
	public String dbName() { return dbName; }
	public String driverClass() { return driverClass; }
	public String password() { return password; }
	public File passwordFile() { return passwordFile; }
	public void setPassword(String p) { password = p; }
	public String url() { return url; }
	public String userName() { return userName; }
}
