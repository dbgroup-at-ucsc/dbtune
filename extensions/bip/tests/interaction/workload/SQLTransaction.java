package interaction.workload;

import java.util.*;

import interaction.workload.SQLStatement.SQLCategory;

public class SQLTransaction implements Iterable<SQLStatement> {
	private List<SQLStatement> stmts;
	public final int id;
	
	public SQLTransaction(List<String> lines, int id0) {
		stmts = extractStmts(lines);
		id = id0;
	}
	
	/*
	 * Will transform a file in a format like
	 * 
	 * --query
	 * SELECT * from R
	 * WHERE a = 10
	 * --dml
	 * DELETE FROM R
	 * 
	 * into a list of SQLStatement objects
	 */
	private static List<SQLStatement> extractStmts(List<String> lines) {
		if (lines.size() == 0)
			return new LinkedList<SQLStatement>();
		List<SQLStatement> stmts = new LinkedList<SQLStatement>();
		
		// type and sqlbuf hold information about the current statement we are reading
		// type is null until the first delimiter is read
		SQLCategory type = null; 
		StringBuffer sqlbuf = new StringBuffer();
		
		if (SQLStatement.toCategory(lines.get(0)) == null) {
			// interpret it as a single query if there is no category indicated
			
			for (String line : lines) {
				if (line.charAt(line.length()-1) == ';')
					sqlbuf.append(line.substring(0, line.length()-1));
				else
					sqlbuf.append(line);
				sqlbuf.append('\n');
			}
			stmts.add(new SQLStatement(SQLCategory.QUERY, sqlbuf.toString()));
		}
		else {
			for (String line : lines) {
				SQLCategory newType = SQLStatement.toCategory(line);
				
				if (newType == null) {
					// continue current statement -- type does not change
					sqlbuf.append(line);
					sqlbuf.append('\n');
				}
				else {
					if (sqlbuf.length() > 0) {
						// we're done with a statement: record it
						assert type != null;
						stmts.add(new SQLStatement(type, sqlbuf.toString()));
						sqlbuf.setLength(0);
					}
					else if (type != null) {
						// we silently ignore empty statements
						// not expected, but not worth throwing an error
					}
					
					type = newType;
				}
			}

			if (type != null && sqlbuf.length() > 0)
				stmts.add(new SQLStatement(type, sqlbuf.toString()));
			sqlbuf.setLength(0);
		}
		
		return stmts;
	}
	
	

	@Override
	public Iterator<SQLStatement> iterator() {
		return stmts.iterator();
	}
}
