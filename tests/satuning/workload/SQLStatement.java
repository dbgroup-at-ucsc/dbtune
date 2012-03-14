package satuning.workload;

public class SQLStatement {
	public final SQLCategory type;
	public final String sql;
	
	public enum SQLCategory { QUERY, DML, OTHER }
	
	public SQLStatement(SQLCategory type0, String sql0) {
		type = type0;
		sql = sql0;
	}
}
