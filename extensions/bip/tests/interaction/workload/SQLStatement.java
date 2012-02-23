package interaction.workload;

public class SQLStatement {
	public final SQLCategory type;
	public final String sql;
	
	public enum SQLCategory { QUERY, DML, DDL }
	
	public static final String[] categoryTags = new String[SQLCategory.values().length];
	static {
		categoryTags[SQLCategory.QUERY.ordinal()] = "--query";
		categoryTags[SQLCategory.DML.ordinal()]   = "--dml";
		categoryTags[SQLCategory.DDL.ordinal()]   = "--ddl";	
	}
	
	
	public SQLStatement(SQLCategory type0, String sql0) {
		type = type0;
		sql = sql0;
	}

	/*
	 * The input string should be one line of a transaction file
	 * 
	 * We return the corresponding SQLCategory if the line starts a new statement.
	 * Otherwise, return null. This is part of the intended usage -- the function
	 * simultaneously determines if the line is the start of a new statement, and
	 * if so returns the SQLCategory.  
	 */
	public static SQLCategory toCategory(String line) {
		int numCategories = SQLCategory.values().length;
		
		for (int i = 0; i < numCategories; i++) {
			if (line.equals(categoryTags[i]))
				return SQLCategory.values()[i];
		}
		
		return null;
	}
}
