package edu.ucsc.satuning.db.pg;

import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.db.DatabaseConnectionManager;
import edu.ucsc.satuning.db.DatabaseIndexColumn;
import edu.ucsc.satuning.db.DatabaseIndexExtractor;
import edu.ucsc.satuning.db.DatabaseWhatIfOptimizer;
import edu.ucsc.satuning.spi.Commands;
import edu.ucsc.satuning.spi.Supplier;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.DBUtilities;
import edu.ucsc.satuning.util.Debug;
import edu.ucsc.satuning.util.Files;
import edu.ucsc.satuning.util.Objects;
import edu.ucsc.satuning.workload.SQLStatement.SQLCategory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

//todo(Huascar) to be removed after peer review.
public class PGConnection implements DatabaseConnection<PGIndex> {
	private Connection conn = null;
	private Statement tempStmt;
	public QueryLibrary qlib;

	// connection parameters
	
	@SuppressWarnings("unused")
	private String dbName;
	private String url;
	private String userName;
	private String password;
	private String driverClass;
	
	public int whatifCount = 0;
	private ArrayList<PGIndex> cachedCandidateSet = new ArrayList<PGIndex>();
	private BitSet cachedBitSet = new BitSet();
	
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

    @Override
    public DatabaseConnectionManager<PGIndex> getConnectionManager() {
        return null;  //todo
    }

    @Override
    public DatabaseIndexExtractor<PGIndex> getIndexExtractor() {
        return null;  //todo
    }

    @Override
    public DatabaseWhatIfOptimizer<PGIndex> getWhatIfOptimizer() {
        return null;  //todo
    }

    @Override
    public void install(DatabaseIndexExtractor<PGIndex> pgIndexDatabaseIndexExtractor, DatabaseWhatIfOptimizer<PGIndex> whatIfOptimizer) {
        //todo
    }

    public void open(String dbName0, String url0, String userName0,
			String password0, String driverClass0) throws SQLException {
		if (conn != null)
			throw new SQLException("cannot open connection: already open");
		dbName = dbName0;
		url = url0;
		userName = userName0;
		password = password0;
		driverClass = driverClass0;
		connect();
	}

	public Iterable<PGIndex> recommendIndexes(String sql) throws SQLException {
        final Supplier<List<PGIndex>> candidateSetSupplier = Commands.submit(
                PGCommands.recommendIndexes(), 
                conn,
                sql
        );
//		ArrayList<PGIndex> candidateSet = new ArrayList<PGIndex>();
//		qlib.recommendIndexes.execute(candidateSet, sql);
//		return candidateSet;
        return candidateSetSupplier.get();
	}

	public Iterable<PGIndex> recommendIndexes(File workloadFile) throws SQLException, IOException {
		List<PGIndex> candidateSet = new ArrayList<PGIndex>();
		for (String line : Files.getLines(workloadFile)) {
			String sql = DBUtilities.trimSqlStatement(line);
            candidateSet.addAll((Collection<? extends PGIndex>) recommendIndexes(sql));
			//qlib.recommendIndexes.execute(candidateSet, sql);
		}
		return candidateSet;
	}

	public void fixCandidates(Iterable<PGIndex> candidateSet) throws SQLException {
		cachedCandidateSet.clear();
		cachedBitSet.clear();
		for (PGIndex idx : candidateSet) {
			cachedCandidateSet.add(idx);
			cachedBitSet.set(idx.internalId());
		}
	}

	public double whatifOptimize(String sql, BitSet config, BitSet usedSet) throws SQLException {
		++whatifCount;
		Debug.print(".");
		if (whatifCount % 75 == 0) Debug.println();
		qlib.explainIndexes.execute(cachedCandidateSet, config, sql);
		try {
			double qcost = qlib.explainIndexes.cost();
			//Debug.println("   qcost = " + qcost);
			usedSet.clear();
			qlib.explainIndexes.setUsedBits(usedSet);
			return qcost;
		} finally {
			qlib.explainIndexes.close();
		}
	}

	public double whatifOptimize(String sql, BitSet config, PGIndex profiledIndex, BitSet usedColumns) throws SQLException {
		throw new UnsupportedOperationException("whatifOptimize cannot give used columns in PGConnection");
	}

	public int whatifCount() {
		return whatifCount;
	}

	public PGExplainInfo getExplainInfo(String sql) throws SQLException {
		++whatifCount;
		qlib.explainIndexes.execute(cachedCandidateSet, cachedBitSet, sql);
		try {
			SQLCategory cat = qlib.explainIndexes.category();
			double[] maintCost = new double[cachedBitSet.length()];
			qlib.explainIndexes.maintCost(cachedBitSet, maintCost);
			return new PGExplainInfo(cat, maintCost);
		} finally {
			qlib.explainIndexes.close();
		}
	}
	
	private class QueryLibrary {
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
		 * QUERY: explainIndexes
		 */
        //todo DONE
		public final ExplainIndexesStatement explainIndexes = new ExplainIndexesStatement();
		public class ExplainIndexesStatement {
			ResultSet rs;

			public ExplainIndexesStatement() {
				rs = null;
			}
			
			// close() should be called for each execute()
			public void execute(Iterable<PGIndex> indexes, BitSet config, String sql) throws SQLException {
				if (rs != null) {
					Debug.println("ExplainIndexesStatement.close() was not called");
					rs.close();
				}
				
				String explainSql = "EXPLAIN INDEXES " +indexListString(indexes, config)+ sql;
				//Debug.println(explainSql);
				rs = tempStmt.executeQuery(explainSql);
				if (!rs.next()) 
					throw new SQLException("no row returned from EXPLAIN INDEXES");
			}
			
			public SQLCategory category() throws SQLException {
				String catString = rs.getString("category");
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
			}

			public void maintCost(BitSet config, double[] maintCost) throws SQLException {
				int configSize = config.cardinality();
				String[] ohArray = rs.getString("index_overhead").split(" ");
				
				// verify ohArray contents
				if (configSize == 0) {
					// we expect ohArray to have one elt that is the empty string
					// but don't complain if it's empty
					if (ohArray.length != 0) {
						Debug.assertion(ohArray.length == 1, "too many elements in ohArray");
						Debug.assertion(ohArray[0].length() == 0, "unexpected element in ohArray");
					}
				}
				else {
					Debug.assertion(configSize == ohArray.length, "wrong length of ohArray");
				}
				
				// based on above checks, we iterate over number of indices
				for (int i = 0; i < configSize; i++) {
					String ohString = ohArray[i];
					String[] splitVals = ohString.split("=");
					Debug.assertion(splitVals.length == 2, "unexpected result in index_overhead");
					int id = Integer.parseInt(splitVals[0]);
					double oh = Double.parseDouble(splitVals[1]);
					maintCost[id] = oh;
				}
				
			}

			private String indexListString(Iterable<PGIndex> indexes, BitSet config) {
				StringBuilder sb = new StringBuilder();
				sb.append("( ");
				for (PGIndex idx : indexes) { 
					if (config.get(idx.internalId())) {
						sb.append(idx.internalId()).append("(");
						if (idx.getSchema().isSync()){
                            sb.append("synchronized ");
                        }
                        final PGTable table = Objects.as(idx.getSchema().getBaseTable());
						sb.append(table.getOid());
						for (int i = 0; i < idx.columnCount(); i++) {
                            final PGIndexColumn indexColumn = Objects.as(idx.getSchema().getColumns().get(i));
							sb.append(idx.getSchema().getDescending().get(i) ? " desc" : " asc");
							sb.append(" ").append(indexColumn.getAttnum());
						}
						sb.append(") ");
					}
				}
				sb.append(") ");
				return sb.toString();
			}
			
			public double cost() throws SQLException {
				Debug.assertion(rs != null, "cannot get whatif cost: result set is not open");
				return Double.parseDouble(rs.getString("qcost"));
			}
			
			public void setUsedBits(BitSet bs) throws SQLException {
				String indexesString = rs.getString("indexes");
				if (indexesString.length() == 0)
					return; // this avoids a NumberFormatException on empty string
				for (String idString : rs.getString("indexes").split(" "))
					bs.set(Integer.parseInt(idString));
			}
			
			public void close() throws SQLException {
				if (rs != null) {
					try {
						rs.close();
					} finally {
						rs = null;
					}
				}
				else {
					Debug.println("ExplainIndexesStatement.close() called on closed result");
				}
			}
		}
		
		/*
		 * QUERY: recommendIndexes
		 * 
		 */
		public final RecommendIndexesStatement recommendIndexes = new RecommendIndexesStatement();
		public class RecommendIndexesStatement {
			public RecommendIndexesStatement() {
			}
			
			public void execute(ArrayList<PGIndex> candidateSet, String sql) throws SQLException {
				ResultSet rs;
				rs = tempStmt.executeQuery("RECOMMEND INDEXES " + sql);
				
				int id = 0;
				try {
					while (rs.next()) {
						++id;
						
						// reloid
						int reloid = Integer.parseInt(rs.getString("reloid"));
						// isSync
						boolean isSync = rs.getString("sync").charAt(0) == 'Y';
						// columns
						ArrayList<DatabaseIndexColumn> columns = new ArrayList<DatabaseIndexColumn>();
						String columnsString = rs.getString("atts");
						if (columnsString.length() > 0)
							for (String attnum  : columnsString.split(" "))
								columns.add(new PGIndexColumn(Integer.parseInt(attnum)));
						// descending
						ArrayList<Boolean> isDescending = new ArrayList<Boolean>();
						String descendingString = rs.getString("desc");
						if (descendingString.length() > 0)
							for (String desc : rs.getString("desc").split(" "))
								isDescending.add(desc.charAt(0) == 'Y');
						
						PGIndexSchema schema= new PGIndexSchema(reloid, isSync, columns, isDescending);
						
						double creationCost = Double.parseDouble(rs.getString("create_cost"));
						double megabytes = Double.parseDouble(rs.getString("megabytes"));
						
						String indexName = "sat_index_" + id;
						String creationText = rs.getString("create_text");
						if (isSync)
							creationText = creationText.replace("CREATE SYNCHRONIZED INDEX ?", 
								                                "CREATE SYNCHRONIZED INDEX " + indexName);
						else
							creationText = creationText.replace("CREATE INDEX ?", 
															    "CREATE INDEX " + indexName);

						candidateSet.add(new PGIndex(schema, id, creationCost, megabytes, creationText));
					}
				} finally {
					rs.close();
					conn.commit();
				}
			}
		}
	}
}
