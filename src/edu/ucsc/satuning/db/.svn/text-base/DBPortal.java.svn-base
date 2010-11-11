package edu.ucsc.satuning.db;

import java.io.File;

//todo(Huascar) to be deleted.
public interface DBPortal<I extends DBIndex<I>> {
	DatabaseConnection<I> getConnection();

	// connection parameters
    String dbName();
	String userName();
	String password();
	void setPassword(String p);
	String url();
	String driverClass();
	File passwordFile();
}
