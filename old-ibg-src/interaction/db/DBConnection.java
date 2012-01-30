package interaction.db;

import interaction.db.DB2Index.AdviseIndexColumn;
import interaction.util.BitSet;
import interaction.workload.SQLStatement;
import interaction.workload.SQLTransaction;

import java.sql.*;
import java.util.Iterator;

public class DBConnection {
	private Connection conn = null;
	private Statement tempStmt;
	public QueryLibrary qlib;

	public int whatifCount = 0;
	public DB2IndexSet cachedCandidateSet = null;
	
	// connection parameters
	private String dbName;
	private String url;
	private String userName;
	private String password;
	private String driverClass;
	
	public DBConnection() {
	}
	
	public void open(String dbName0, String url0, String userName0, String password0, String driverClass0) throws SQLException {
		if (conn != null)
			throw new SQLException("cannot open connection: already open");
		dbName = dbName0;
		url = url0;
		userName = userName0;
		password = password0;
		driverClass = driverClass0;
		connect();
	}
	
	public void close() throws SQLException {
		if (conn == null)
			throw new SQLException("cannot close connection: already closed");

		dbName = null;
		url = null;
		userName = null;
		password = null;
		driverClass = null;
		disconnect();
	}
	
	private void connect() throws SQLException {
		try { 
			Class.forName(driverClass);
		} catch (Exception e) {
			throw new SQLException("Could not load the driver");
		}
						
		conn = DriverManager.getConnection(url, userName, password);  
		conn.setAutoCommit(false);
		
		tempStmt = conn.createStatement();
		qlib = new QueryLibrary();
		qlib.isolationLevelReadCommitted.execute();
	}
	
	private void disconnect() throws SQLException {
		tempStmt = null;	
		conn.close();
		conn = null;
		qlib = null;
	}

	public void execute(String sql) throws SQLException {
		if (conn == null)
			throw new SQLException("cannot execute: no database connection");
		tempStmt.execute(sql);
	}
	
	public void commit() throws SQLException {
		if (conn == null)
			throw new SQLException("cannot commit: no database connection");
		conn.commit();
	}

	public void rollback() throws SQLException {
		if (conn == null)
			throw new SQLException("cannot rollback: no database connection");
		conn.rollback();
	}
	
	/*
	 * Runs the transactions through the whatif optimizer with the given configuration
	 * 
	 * The input configuration is interpreted as a subset of the indexes given 
	 * in the previous call to fixCandidates. More precisely, the what-if call 
	 * will be performed with an index enabled if and only if it had internalID
	 * "i" in the previous call to fixCandidates and bit "i" is set in config. 
	 * 
	 * The total cost is returned. This function will also clear the existing contents of
	 * usedSet and fill it with the subset of config that is used in some plan.
	 */
	public double whatifOptimize(Iterable<SQLTransaction> xacts, BitSet config, BitSet usedSet) throws SQLException {
		int queryCount; // counted in xacts
		int explainCount; // should be equal to queryCount
		double totalCost;
		
		if (++whatifCount % 80 == 0) {
			fixCandidates(cachedCandidateSet);
			System.err.println();
		}
		
		// clear explain tables that we will end up reading
		qlib.clearExplainObject.execute();
		qlib.clearExplainStatement.execute();
		
		// enable indexes and set explain mode
		qlib.enableAdviseIndexRows.execute(config);
		qlib.explainModeEvaluateIndexes.execute();
	
		// evaluate the queries
		queryCount = 0;
		for (SQLTransaction xact : xacts) {
		//{ SQLTransaction xact = xacts.get(4); 
			for (SQLStatement query : xact) {
				switch (query.type) {
					case QUERY:
					case DML:
						queryCount++;
						try {
							execute(query.sql);
							//throw new Error("returned from execute() in what-if mode");
						} catch (SQLException e) {
							System.err.print('.');
							// expected in explain mode
						}
						break;
					case DDL:
						qlib.explainModeNo.execute();
						execute(query.sql);
						commit();
						qlib.explainModeEvaluateIndexes.execute();
						break;
				} 
			}	
		}

	
		
		// reset explain mode (indexes are left enabled...)
		qlib.explainModeNo.execute();
		
		// post-process the explain tables
		
		// first get workload cost 
		qlib.fetchExplainStatementTotals.execute();
		try {
			explainCount = qlib.fetchExplainStatementTotals.getCount();
			totalCost = qlib.fetchExplainStatementTotals.getTotalCost();
		} catch (SQLException e) {
			try { qlib.fetchExplainStatementTotals.close(); } 
			catch (Exception e2) { }
			throw e;
		}
		qlib.fetchExplainStatementTotals.close();
		if (explainCount != queryCount)
			throw new SQLException("unexpected number of statements: " + explainCount + " (expected " + queryCount + ")");
		
		// now get used indexes, using the tempBitSet
		usedSet.clear();
		qlib.fetchExplainObjectCandidates.execute(usedSet);
		
		commit();
		
		return totalCost;
	}

	/*
	 * This function prepares the database for what-if optimization with the 
	 * given set of candidates. Each call to whatifOptimize is based on the
	 * candidates indicated by this function
	 * 
	 */
	public void fixCandidates(DB2IndexSet candidateSet) throws SQLException {
		cachedCandidateSet = candidateSet;
		qlib.clearAdviseIndex.execute();
		
		qlib.loadAdviseIndex.execute(candidateSet.iterator(), false);
		
//		int indexCount = qlib.loadAdviseIndex.execute(candidateSet.iterator(), false);
//		System.out.println("loaded " + indexCount + " indexes");
	}
	
// NOT USED
//	public int getInt(String sql) throws SQLException {
//		ResultSet rs = tempStmt.executeQuery(sql);
//		
//		try {
//			rs.next();
//			int result = rs.getBigDecimal(1).intValueExact();
//			return result;
//		} catch (ArithmeticException e) {
//			throw new SQLException("could not convert integer");
//		} finally {
//			rs.close();
//		}
//	}
	
	/*
	 * The QueryLibrary is the entry point for creating any SQL
	 * 
	 * Depending on the task, the QueryLibrary may provide a different type
	 * of object. In every case, the execution of the task and the retrieval
	 * of results in encapsulated in the object.
	 *    
	 * This class should not be instantiated before the super class is initialized
	 */
	public class QueryLibrary {
		/* must be called after superclass is instantiated! */
		private QueryLibrary() throws SQLException {
		}

		/*
		 * Common class for commands that return no result
		 */
		public class Command {
			private PreparedStatement ps;
			private Command(String sql) throws SQLException {
				ps = conn.prepareStatement(sql);
			}
			public void execute() throws SQLException {
				if (ps.execute())
					throw new SQLException("Command returned unexpected result set");
			}
		}
	
		/*
		 * Common class for DML statements
		 */
		public class Update {
			private PreparedStatement ps;
			private Update(String sql) throws SQLException {
				ps = conn.prepareStatement(sql);
			}
			public int execute() throws SQLException {
				int count = ps.executeUpdate();
				conn.commit();
				return count;
			}
		}
		
		/*
		 * QUERY: isolationLevelReadCommitted
		 */
		public final Command isolationLevelReadCommitted = new Command("SET ISOLATION READ COMMITTED");
		
		/*
		 * QUERY: explainModeRecommendIndexes
		 */
		public final Command explainModeRecommendIndexes = new Command("SET CURRENT EXPLAIN MODE = RECOMMEND INDEXES");

		/*
		 * QUERY: explainModeEvaluateIndexes
		 */	
		public final Command explainModeEvaluateIndexes =	new Command("SET CURRENT EXPLAIN MODE = EVALUATE INDEXES");
		
		/*
		 * QUERY: explainModeNo
		 */
		public final Command explainModeNo = new Command("SET CURRENT EXPLAIN MODE = NO");
		
		/*
		 * QUERY: clearExplainObject
		 */
		public final Update clearExplainObject = new Update("DELETE FROM SYSTOOLS.explain_object");
		
		/*
		 * QUERY: clearExplainStatement
		 */
		public final Update clearExplainStatement = new Update("DELETE FROM SYSTOOLS.explain_statement");

		/*
		 * QUERY: clearAdviseIndex
		 */
		public final Update clearAdviseIndex = new Update("DELETE FROM SYSTOOLS.advise_index");

		/* 
		 * QUERY: fetchExplainStatementTotals
		 * 
		 * Returns the total cost of 'P' level statements in EXPLAIN_STATEMENT
		 * 
		 * Also provides the number of 'P' level statements
		 */
		public final ExplainStatementTotalsStatement fetchExplainStatementTotals = new ExplainStatementTotalsStatement();
		public class ExplainStatementTotalsStatement {
			private double totalCost;
			private int count;
			private boolean init = false;
			PreparedStatement ps = conn.prepareStatement(
						"SELECT SUM(TOTAL_COST) AS COST, COUNT(*) AS COUNT "
					  + "FROM SYSTOOLS.explain_statement "
					  + "WHERE explain_level = 'P'");
			
			public ExplainStatementTotalsStatement() throws SQLException {
			}
			
			/*
			 * Get results with the getCount and getTotalCost methods.
			 * It is preferable for the caller to close() the object after the information is retrieved 
			 */
			public void execute() throws SQLException {
				ResultSet rs = ps.executeQuery();
				try {
					rs.next();
					totalCost = rs.getDouble(1);
					count = rs.getInt(2);
					init = true;
				} finally {
					rs.close();
					conn.commit();
				}
			}
			
			public int getCount() throws SQLException {
				if (!init) throw new SQLException("no result available");
				return count;
			}
			
			public double getTotalCost() throws SQLException {
				if (!init) throw new SQLException("no result available");
				return totalCost;
			}
			
			public void close() throws SQLException {
				if (!init) throw new SQLException("no result available");
				init = false;
			}
		}

		/*
		 * QUERY: fetchExplainObjectCandidates
		 * 
		 * Finds the entries of EXPLAIN_OBJECT that correspond to candidate indexes
		 * that were enabled by this tool.
		 * The indexes may be returned compactly by filling in a BitSet to represent
		 * the internal IDs of the indexes. 
		 */
		public final ExplainObjectCandidatesStatement fetchExplainObjectCandidates = new ExplainObjectCandidatesStatement();
		public class ExplainObjectCandidatesStatement {
			PreparedStatement ps = conn.prepareStatement(
				"SELECT DISTINCT CAST(SUBSTRING(object_name FROM " 
		      + (DB2Index.indexNameBase.length() + 1) + " USING CODEUNITS16) AS INT) "
			  + "FROM SYSTOOLS.EXPLAIN_OBJECT "
			  + "WHERE OBJECT_NAME LIKE '" + DB2Index.indexNameBase + "_%'");
			
			public ExplainObjectCandidatesStatement() throws SQLException {
			}
			
			public void execute(BitSet cands) throws SQLException {
				ResultSet rs = ps.executeQuery();
				try {
					while (rs.next()) {
						cands.set(rs.getInt(1));
					}
				} finally {
					rs.close();
					conn.commit();
				}
			}
		}
		
		/*
		 * QUERY: readAdviseIndex
		 * 
		 * Gets all of the indexes in the ADVISE_INDEX table
		 */
		public final ReadAdviseIndexStatement readAdviseIndex = new ReadAdviseIndexStatement();
		public class ReadAdviseIndexStatement {
			private PreparedStatement psAll; // use all indexes in table
			private PreparedStatement psOne; // use one index in table
			
			public ReadAdviseIndexStatement() throws SQLException {
				// we want columns in same order as in AdviseIndexColumn
				StringBuilder sbuf = new StringBuilder();
				sbuf.append("SELECT ");
				implode(sbuf, AdviseIndexColumn.values(), ", ");
				sbuf.append(" FROM SYSTOOLS.advise_index");
				psAll = conn.prepareStatement(sbuf.toString());
				sbuf.append(" WHERE name = ?");
				psOne = conn.prepareStatement(sbuf.toString());
			}
			
			public void execute(DB2IndexSet indexes) throws SQLException {
				ResultSet rs = psAll.executeQuery();
				int id = 0;
				try {
					while (rs.next()) {
						++id;
						indexes.add(DB2Index.consFromAdviseIndex(rs, dbName, id));
					}
				} finally {
					rs.close();
					conn.commit();
				}
			}

			public DB2Index execute(String indexName, int id) throws SQLException {
				psOne.setString(1, indexName);
				ResultSet rs = psOne.executeQuery();
				try {
					assert(rs.next());
					DB2Index idx = DB2Index.consFromAdviseIndex(rs, dbName, id);
					return idx;
				} finally {
					rs.close();
					conn.commit();
				}
			}
		}

		/*
		 * QUERY: loadAdviseIndex
		 * 
		 * Inserts indexes into the ADVISE_INDEX table (it is not cleared first).
		 * The 'enable' option determines if the indexes should initially be 
		 * available to the what-if optimization. 
		 */
		public LoadAdviseIndexStatement loadAdviseIndex = new LoadAdviseIndexStatement();
		public class LoadAdviseIndexStatement {
			// Use this to cache the first part of the INSERT (it's long)
			String sqlprefix = null;
			
			public int execute(Iterator<DB2Index> config, boolean enable) throws SQLException {
				StringBuilder sbuf = new StringBuilder();
				
				if (sqlprefix == null) {
					sbuf.append("INSERT INTO SYSTOOLS.advise_index(");
					implode(sbuf, AdviseIndexColumn.values(), ", ");
					sbuf.append(") VALUES ");
					sqlprefix = sbuf.toString();
				}
				else {
					sbuf.append(sqlprefix);
				}
				
				while (config.hasNext()) {
					DB2Index idx = config.next();
					idx.adviseIndexRowText(sbuf, enable);
					if (config.hasNext())
						sbuf.append(", ");
				}
				
				int count = tempStmt.executeUpdate(sbuf.toString());
				conn.commit();
				return count;
			}
		}
		
		/*
		 * QUERY: enableAdviseIndexRows
		 * 
		 * Sets the USE_INDEX column to enable all indexes with IID in the config, and
		 * disable all other indexes. 
		 */
		public EnableAdviseIndexRowsStatement enableAdviseIndexRows = new EnableAdviseIndexRowsStatement();
		public class EnableAdviseIndexRowsStatement {
			
			
			public int execute(BitSet config) throws SQLException {
				String sql;
				
				if (config.cardinality() == 0)
					sql = "UPDATE SYSTOOLS.advise_index SET use_index = 'N'";
				else {
					StringBuilder sbuf = new StringBuilder();
					
					sbuf.append("UPDATE SYSTOOLS.advise_index SET use_index = CASE WHEN iid IN (");
					boolean first = true;
					for (int i = config.nextSetBit(0); i >= 0; i = config.nextSetBit(i+1)) {
						if (!first)
							sbuf.append(", ");
						sbuf.append(i);
						first = false;
					}
					sbuf.append(") THEN 'Y' ELSE 'N' END");
					sql = sbuf.toString();
				}	
				int count = tempStmt.executeUpdate(sql);
				conn.commit();
				return count;
			}
			
		}
	}
	
	private static void implode(StringBuilder sbuf, Object[] data, String separator) {
		for (int i = 0; i < data.length; i++) {
			if (i > 0)
				sbuf.append(separator);
			sbuf.append(data[i]);
		}
	}

	public static void formatIdentifier(String str, StringBuilder sbuf) {
		int strlen = str.length();  
		boolean simple;
		
		if (!Character.isLetter(str.charAt(0)))
			simple = false;
		else {
			simple = true;
			for (int i = 0; i < strlen; i++) {
				char c = str.charAt(i);
				if (c != '_' && !Character.isLetterOrDigit(c)) {
					simple = false;
					break;
				}
			}
		}
		
		if (simple)
			sbuf.append(str);
		else {
			sbuf.append('"');
			for (int i = 0; i < strlen; i++) {
				char c = str.charAt(i);
				sbuf.append(c);
				if (c == '"')
					sbuf.append('"');
			}
			sbuf.append('"');
		}
	}
	
	public static void formatStringLiteral(String str, StringBuilder sbuf) {
		int strlen = str.length();  
		
		sbuf.append('\'');
		for (int i = 0; i < strlen; i++) {
			char c = str.charAt(i);
			if (c == '\'')
				sbuf.append("''");
			else
				sbuf.append(c);
		}
		sbuf.append('\'');
	}

	public String dbName() {
		return dbName;
	}
	
	public String password() {
		return password;
	}
	
	public String userName() {
		return userName;
	}
}
