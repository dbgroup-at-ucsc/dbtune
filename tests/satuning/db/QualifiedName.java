package satuning.db;

import java.io.Serializable;

public class QualifiedName implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public  String dbName;
	public  String schemaName;
	public  String name;
	
	QualifiedName(String dbName0, String schemaName0, String name0) {
		dbName = dbName0;
		schemaName = schemaName0;
		name = name0;
	}

	public boolean equals(String dbName2, String schemaName2, String name2) {
		return dbName.equals(dbName2) && schemaName.equals(schemaName2) && name.equals(name2);
	}
	
	public String toString() {
		return dbName+"."+schemaName+"."+name;
	}
}
