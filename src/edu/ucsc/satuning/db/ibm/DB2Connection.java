package edu.ucsc.satuning.db.ibm;

import edu.ucsc.satuning.Configuration;
import edu.ucsc.satuning.db.DatabaseWhatIfOptimizer;
import edu.ucsc.satuning.db.ibm.Advisor.FileInfo;
import edu.ucsc.satuning.db.CostOfLevel;
import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.db.DatabaseConnectionManager;
import edu.ucsc.satuning.db.ExplainInfo;
import edu.ucsc.satuning.db.DatabaseIndexExtractor;
import edu.ucsc.satuning.spi.Commands;
import edu.ucsc.satuning.util.DBUtilities;
import edu.ucsc.satuning.db.ibm.DB2IndexMetadata.AdviseIndexColumn;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.Debug;
import edu.ucsc.satuning.workload.SQLStatement.SQLCategory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import static edu.ucsc.satuning.db.ibm.DB2Commands.clearAdviseIndex;
import static edu.ucsc.satuning.db.ibm.DB2Commands.clearExplainObject;
import static edu.ucsc.satuning.db.ibm.DB2Commands.clearExplainStatement;
import static edu.ucsc.satuning.db.ibm.DB2Commands.enableAdviseIndexRows;
import static edu.ucsc.satuning.db.ibm.DB2Commands.explainModeEvaluateIndexes;
import static edu.ucsc.satuning.db.ibm.DB2Commands.explainModeNo;
import static edu.ucsc.satuning.db.ibm.DB2Commands.fetchExplainObjectCandidates;
import static edu.ucsc.satuning.db.ibm.DB2Commands.loadAdviseIndex;


//todo(Huascar) to be removed after peer review.
public class DB2Connection implements DatabaseConnection<DB2Index> {
	private Connection conn = null;
	private Statement tempStmt;
	public QueryLibrary qlib;

	public int whatifCount = 0;
	private Iterable<DB2Index> cachedCandidateSet = new java.util.LinkedList<DB2Index>();
	
	// connection parameters
	private String dbName;
	private String url;
	private String userName;
	private String password;
	private String driverClass;
	
	public DB2Connection() {
	}

    @Override
    public DatabaseConnectionManager<DB2Index> getConnectionManager() {
        return null;  //todo
    }

    @Override
    public DatabaseIndexExtractor<DB2Index> getIndexExtractor() {
        return null;  //todo
    }

    @Override
    public DatabaseWhatIfOptimizer<DB2Index> getWhatIfOptimizer() {
        return null;  //todo
    }

    @Override
    public void install(DatabaseIndexExtractor<DB2Index> db2IndexDatabaseIndexExtractor, DatabaseWhatIfOptimizer<DB2Index> whatIfOptimizer) {
        //todo
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

    @Override
    public boolean isClosed() {
        return false;  //todo
    }

    @Override
    public boolean isOpened() {
        return false;  //todo
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
	
	public int whatifCount() {
		return whatifCount;
	}

	/*
	 * This function prepares the database for what-if optimization with the 
	 * given set of candidates. Each call to whatifOptimize is based on the
	 * candidates indicated by this function
	 * 
	 */
	public void fixCandidates(Iterable<DB2Index> candidateSet) throws SQLException {
		cachedCandidateSet = candidateSet;
        Commands.submitAll(
                Commands.submit(clearAdviseIndex(), conn),
                Commands.submit(loadAdviseIndex(), candidateSet.iterator(), false, conn)
        );
	}
	
	public double whatifOptimize(String sql, BitSet config, BitSet usedSet) throws SQLException {
		int explainCount; // should be equal to 1
		double totalCost;
		
		if (++whatifCount % 80 == 0) {
			fixCandidates(cachedCandidateSet);
			System.err.println();
		}

        // silently supply a command and an input parameter so that it could
        // get executed in the background (since no return to the caller is needed)
        Commands.submitAll(
                // clear explain tables that we will end up reading
                Commands.submit(clearExplainObject(), conn),
                Commands.submit(clearExplainStatement(), conn),
                // enable indexes and set explain mode
                Commands.submit(enableAdviseIndexRows(), conn, config),
                Commands.submit(explainModeEvaluateIndexes(), conn)
        );
	
		// evaluate the query
		try {
			execute(sql);
			//throw new Error("returned from execute() in what-if mode");
			System.err.print('.');
		} catch (SQLException e) {
			System.err.print('.');
			// expected in explain mode
		}

        // reset explain mode (indexes are left enabled...)
        Commands.submit(explainModeNo(), conn);

		// post-process the explain tables
		// first get workload cost
        final CostOfLevel costLevel = Commands.supplyValue(
            DB2Commands.fetchExplainStatementTotals(),
            conn
        );

		try {
			explainCount = costLevel.getCount();
			totalCost    = costLevel.getTotalCost();
		} catch (Throwable e) {
            costLevel.close();
			throw new SQLException(e);
		}
        costLevel.close();
		if (explainCount != 1){
            throw new SQLException("unexpected number of statements: " + explainCount + " (expected 1)");
        }
		
		// now get used indexes, using the input BitSet
		usedSet.clear();
        Commands.submit(fetchExplainObjectCandidates(), conn, usedSet);
		commit();
		
		return totalCost;
	}
	
	public double whatifOptimize(String sql, BitSet config, DB2Index profiledIndex, BitSet usedColumns) throws SQLException {
		int explainCount; // should be equal to 1
		double totalCost;
		
		if (++whatifCount % 80 == 0) {
			fixCandidates(cachedCandidateSet);
			System.err.println();
		}
		
		// clear explain tables that we will end up reading
		qlib.clearExplainObject.execute();
		qlib.clearExplainStatement.execute();
		qlib.clearExplainPredicate.execute();
		
		// enable indexes and set explain mode
		qlib.enableAdviseIndexRows.execute(config);
		qlib.explainModeEvaluateIndexes.execute();
	
		// evaluate the query
		try {
			execute(sql);
			System.err.print('.');
			//throw new Error("returned from execute() in what-if mode");
		} catch (SQLException e) {
			System.err.print('.');
			// expected in explain mode
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
		if (explainCount != 1)
			throw new SQLException("unexpected number of statements: " + explainCount + " (expected 1)");
		
		// now get used columns from the profiled index
		if (profiledIndex != null) {
			String preds = qlib.fetchExplainPredicateString.execute();
			for (int i = 0; i < profiledIndex.columnCount(); i++) {
				if (preds.contains(profiledIndex.getColumn(i).getName()))
					usedColumns.set(i);
			}
		}
		
		commit();
		
		return totalCost;
	}
	
	public Iterable<DB2Index> recommendIndexes(String sql) throws SQLException {		
		java.util.List<DB2IndexMetadata> metaList = new java.util.ArrayList<DB2IndexMetadata>();

		try {
			// Populate the ADVISE_INDEX with indexes for the query
			// Based on the DB2 doc, it seems that an EXPLAIN command would be used
			// but this does not work. What does work is to try to execute the query
			// while in RECOMMEND INDEXES mode. This raises an exception because you're 
			// not allowed to execute queries while explaining is enabled... but it
			// will populate the ADVISE_INDEX table anyway!
			qlib.clearAdviseIndex.execute();
			qlib.explainModeRecommendIndexes.execute();
			try { execute(sql); } catch (SQLException e) { }
			qlib.explainModeNo.execute();

			// read the index list
			qlib.readAdviseIndex.execute(metaList);
			qlib.clearAdviseIndex.execute();
		} catch (SQLException e) {
			try { conn.rollback(); }
			catch (SQLException e2) { 
				Debug.logError("Could not rollback transaction", e2);
			}
			Debug.logError("Could not recommend indexes for statement " + sql,  e);
			throw e;
		} 
		
		try {
			conn.rollback();
		} catch (SQLException e) {
		    Debug.logError("Could not rollback transaction", e);
		}

		// postprocess list of index meta data to get creation cost
		java.util.List<DB2Index> indexList = new java.util.ArrayList<DB2Index>();
		for (DB2IndexMetadata meta : metaList) {
			double creationCost = meta.creationCost(this);
			DB2Index index = new DB2Index(meta, creationCost);
			indexList.add(index);
		}
		return indexList;
	}
	
	public Iterable<DB2Index> 
	recommendIndexes(java.io.File workloadFile) throws SQLException, IOException {
		// run the advisor to get the initial candidates
		// Give a budget of -1 to mean "no budget"
		try {
			FileInfo advisorFile = Advisor.createAdvisorFile(this, Configuration.db2Advis, -1, workloadFile);
			return advisorFile.getCandidates(this);
		} catch (AdvisorException e) {
			e.printStackTrace();
			throw new SQLException();
		}
	}

	public ExplainInfo<DB2Index> getExplainInfo(String sql) throws SQLException {
		SQLCategory category = null;
		QualifiedName updatedTable = null;
		double updateCost = 0;

		try {
			// clear out the tables we'll be reading
			qlib.clearExplainObject.execute();
			qlib.clearExplainStatement.execute();
			qlib.clearExplainOperator.execute();
			
			// execute statment in explain mode = explain
			qlib.explainModeExplain.execute();
			try { execute(sql); } 
			catch (Exception e) { 
				//Debug.println("Exception: " + e);
			}
			qlib.explainModeNo.execute();
			
			// read the statement type
			category = qlib.fetchExplainStatementType.execute();
			
			// get the updated table and update cost if any
			if (category == SQLCategory.DML) {
				updatedTable = qlib.fetchExplainObjectUpdatedTable.execute();
				updateCost = qlib.fetchExplainOpUpdateCost.execute();
			}
		} catch (SQLException e) {
			try { conn.rollback(); }
			catch (SQLException e2) { 
				Debug.logError("Could not rollback transaction", e2);
			}
			Debug.logError("Could not execute statement " + sql,  e);
			throw e;
		} 
		
		try {
			conn.rollback();
		} catch (SQLException e) {
		    Debug.logError("Could not rollback transaction", e);
		}
		
		return new DB2ExplainInfo(category, updatedTable, updateCost);
	}
	
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
		 * QUERY: explainModeNo
		 */
		public final Command explainModeExplain = new Command("SET CURRENT EXPLAIN MODE = EXPLAIN");
		
		/*
		 * QUERY: clearExplainObject
		 */
		public final Update clearExplainObject = new Update("DELETE FROM explain_object");
		
		/*
		 * QUERY: clearExplainStatement
		 */
		public final Update clearExplainStatement = new Update("DELETE FROM explain_statement");

		/*
		 * QUERY: clearExplainOperator
		 */
		public final Update clearExplainOperator = new Update("DELETE FROM explain_operator");

		/*
		 * QUERY: clearExplainPredicate
		 */
		public final Update clearExplainPredicate = new Update("DELETE FROM explain_predicate");
		
		/*
		 * QUERY: clearAdviseIndex
		 */
		public final Update clearAdviseIndex = new Update("DELETE FROM advise_index");

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
					  + "FROM explain_statement "
					  + "WHERE explain_level = 'P'");
			
			public ExplainStatementTotalsStatement() throws SQLException {
			}
			
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
		 * QUERY: fetchExplainStatementType
		 * 
		 * Returns the value in the STATEMENT_TYPE column of EXPLAIN_STATEMENT
		 * Throws an exception if there is more than one row.
		 */
        // DONE and above
		public final FetchExplainStatementTypeStatement fetchExplainStatementType = new FetchExplainStatementTypeStatement();
		public class FetchExplainStatementTypeStatement {
			PreparedStatement ps = conn.prepareStatement(
					"SELECT TRIM(STATEMENT_TYPE) AS TYPE "
				  + "FROM explain_statement "
				  + "WHERE explain_level = 'P'");
		
			public FetchExplainStatementTypeStatement() throws SQLException {
			}

			public SQLCategory execute() throws SQLException {
				ResultSet rs = ps.executeQuery();
				try {
					if (!rs.next())
						throw new SQLException("could not derive stmt type: no rows");
					
					String catString = rs.getString(1);
					if (catString.length() == 0)
						throw new SQLException("could not derive stmt type: empty type");
					if (rs.next())
						throw new SQLException("could not derive stmt type: too many rows");
						
					if (catString.length() > 1) {
						if (catString.equals("UC") || catString.equals("DC"))
							return SQLCategory.DML;
						else
							return SQLCategory.OTHER;
					}
					
					switch (catString.charAt(0)) {
						case 'I' :
						case 'U' :
						case 'D' :
						case 'M' :
							return SQLCategory.DML;
						case 'S' :
							return SQLCategory.QUERY;
						default :
							return SQLCategory.OTHER;
					}
				} finally {
					rs.close();
					conn.commit();
				}
			}
		}

		/* 
		 * QUERY: fetchExplainOpUpdateCost
		 * 
		 * Gets the cost of the update/delete/insert operator
		 * Assumes that this operator has OPERATOR_ID=2, and its
		 * child has OPERATOR_ID=3
		 */
		public final ExplainOperatorUpdateCostStatement fetchExplainOpUpdateCost = new ExplainOperatorUpdateCostStatement();
		public class ExplainOperatorUpdateCostStatement {
			PreparedStatement ps = conn.prepareStatement(
						"SELECT TOTAL_COST "
					  + "FROM EXPLAIN_OPERATOR "
					  + "WHERE OPERATOR_ID = 2 OR OPERATOR_ID = 3 "
					  + "ORDER BY OPERATOR_ID");
			
			public ExplainOperatorUpdateCostStatement() throws SQLException {
			}
			
			public double execute() throws SQLException {
				ResultSet rs = ps.executeQuery();
				try {
					if (!rs.next())
						throw new SQLException("Could not get update cost: no rows");
					double updateOpCost = rs.getDouble(1);
					if (!rs.next())
						throw new SQLException("Could not get update cost: only one row");
					double childOpCost = rs.getDouble(1);
					if (rs.next())
						throw new SQLException("Could not get update cost: too many rows");
					Debug.println("updateCost = " + (updateOpCost - childOpCost));
					return updateOpCost - childOpCost;
				} finally {
					rs.close();
					conn.commit();
				}
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
		      + (DB2IndexMetadata.INDEX_NAME_BASE.length() + 1) + " USING CODEUNITS16) AS INT) "
			  + "FROM EXPLAIN_OBJECT "
			  + "WHERE OBJECT_NAME LIKE '" + DB2IndexMetadata.INDEX_NAME_BASE + "_%'");
			
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
		 * QUERY: fetchExplainPredicateString
		 * 
		 * Puts all the predicates form EXPLAIN_PREDICATE into a space-separated string
		 */
		public final ExplainPredicateStringStatement fetchExplainPredicateString = new ExplainPredicateStringStatement();
		public class ExplainPredicateStringStatement {
			private java.util.Set<String> tempPredicateSet = new java.util.TreeSet<String>();
			
			private static final String sql = "SELECT p.predicate_text " +
				"FROM explain_predicate p, explain_operator o " +
				"WHERE o.operator_id=p.operator_id AND o.operator_type='IXSCAN'";
			PreparedStatement ps = conn.prepareStatement(sql);
			
			public ExplainPredicateStringStatement() throws SQLException {
			}
			
			public String execute() throws SQLException {
				Debug.assertion(tempPredicateSet.size() == 0, "tempPredicateSet was not cleared");
				
				// put predicates into tempPredicateSet in order to eliminate duplicates
				// we can't do DISTINCT in the SQL because of DB2 limitations on data types
				// I haven't found an easy in-database workaround
				try {
					ResultSet rs = ps.executeQuery();
					try {
						while (rs.next()) {
							tempPredicateSet.add(rs.getString(1));
						}
					} finally {
						rs.close();
						conn.commit();
					}
				} catch (SQLException e) {
					Debug.logError("could not execute query " + sql);
					throw e;
				}
				
				StringBuilder sb = new StringBuilder();
				for (String predicateText : tempPredicateSet) {
					if (sb.length() > 0) sb.append(' ');
					sb.append(predicateText);
				}
				String retval = sb.toString();
//				Debug.println("PREDICATES = " + retval);				
				
				tempPredicateSet.clear();
				return retval;
			}
		}
		
		/*
		 * QUERY: fetchExplainObjectUpdatedTable
		 * 
		 * Finds the entries of EXPLAIN_OBJECT that correspond to a table.
		 * The caller should ensure that the last explained command was DML.
		 * We don't handle cases where other tables are involved in the statement. 
		 */
		public final ExplainObjectUpdatedTableStatement fetchExplainObjectUpdatedTable = new ExplainObjectUpdatedTableStatement();
		public class ExplainObjectUpdatedTableStatement {
			PreparedStatement ps = conn.prepareStatement(
				"SELECT trim(object_schema), trim(object_name) "
			  + "FROM EXPLAIN_OBJECT "
			  + "WHERE OBJECT_TYPE = 'TA'");
			
			public ExplainObjectUpdatedTableStatement() throws SQLException {
			}
			
			public QualifiedName execute() throws SQLException {
				ResultSet rs = ps.executeQuery();
				try {
					if (!rs.next())
						throw new SQLException("Could not get updated table: no rows");
					String schemaName = rs.getString(1);
					String tableName = rs.getString(2);
					if (rs.next())
						throw new SQLException("Could not get updated table: too many rows");
					return new QualifiedName(dbName, schemaName, tableName);
				} finally {
					rs.close();
					conn.commit();
				}
			}
		}
		
		/*
		 * QUERY: readAdviseIndex
		 * 
		 * Gets all of the indexes in the ADVISE_INDEX table that are hypothetical
		 */
		public final ReadAdviseIndexStatement readAdviseIndex = new ReadAdviseIndexStatement();
		public class ReadAdviseIndexStatement {
			private PreparedStatement psAll; // use all indexes in table
			private PreparedStatement psOne; // use one index in table
			
			public ReadAdviseIndexStatement() throws SQLException {
				// we want columns in same order as in AdviseIndexColumn
				StringBuilder sbuf = new StringBuilder();
				sbuf.append("SELECT ");
				DBUtilities.implode(sbuf, AdviseIndexColumn.values(), ", ");
				sbuf.append(" FROM advise_index WHERE exists = 'N'");
				psAll = conn.prepareStatement(sbuf.toString());
				sbuf.append(" AND name = ?");
				psOne = conn.prepareStatement(sbuf.toString());
			}
			
			public void execute(List<DB2IndexMetadata> candidateSet) throws SQLException {
				ResultSet rs = psAll.executeQuery();
				int id = 0;
				try {
					while (rs.next()) {
						++id;
						// XXX: passing -1 for unknown index size
						candidateSet.add(DB2IndexMetadata.consFromAdviseIndex(rs, dbName, id, -1));
					}
				} finally {
					rs.close();
					conn.commit();
				}
			}

			public DB2IndexMetadata execute(String indexName, int id, double megabytes) throws SQLException {
				psOne.setString(1, indexName);
				ResultSet rs = psOne.executeQuery();
				try {
					boolean haveResult = rs.next();
					Debug.assertion(haveResult, "did not find index " + indexName + " in ADVISE_INDEX");
					DB2IndexMetadata idx = DB2IndexMetadata.consFromAdviseIndex(rs, dbName, id, megabytes);
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
        // done
		public LoadAdviseIndexStatement loadAdviseIndex = new LoadAdviseIndexStatement();
		public class LoadAdviseIndexStatement {
			// Use this to cache the first part of the INSERT (it's long)
			String sqlprefix = null;
			
			public int execute(Iterator<DB2Index> config, boolean enable) throws SQLException {
				if (!config.hasNext())
					return 0; // nothing to insert
				
				StringBuilder sbuf = new StringBuilder();
				
				if (sqlprefix == null) {
					sbuf.append("INSERT INTO advise_index(");
					DBUtilities.implode(sbuf, AdviseIndexColumn.values(), ", ");
					sbuf.append(") VALUES ");
					sqlprefix = sbuf.toString();
				}
				else {
					sbuf.append(sqlprefix);
				}
				
				while (config.hasNext()) {
					DB2Index idx = config.next();
					idx.meta.adviseIndexRowText(sbuf, enable);
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
        // done
		public EnableAdviseIndexRowsStatement enableAdviseIndexRows = new EnableAdviseIndexRowsStatement();
		public class EnableAdviseIndexRowsStatement {
			
			
			public int execute(BitSet config) throws SQLException {
				String sql;
				
				if (config.cardinality() == 0)
					sql = "UPDATE advise_index SET use_index = 'N'";
				else {
					StringBuilder sbuf = new StringBuilder();
					
					sbuf.append("UPDATE advise_index SET use_index = CASE WHEN iid IN (");
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
