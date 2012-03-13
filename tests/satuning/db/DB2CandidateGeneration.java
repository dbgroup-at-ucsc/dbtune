package satuning.db;

import java.sql.SQLException;

import satuning.workload.*;

class DB2CandidateGeneration {
	public static Iterable<DB2IndexMetadata> getCandidates(DBConnection conn, SQLStatement stmt) throws SQLException {
		java.util.List<DB2IndexMetadata> candidateSet = new java.util.ArrayList<DB2IndexMetadata>();
		DBConnection.QueryLibrary qlib = conn.qlib;

		try {
			// Populate the ADVISE_INDEX with indexes for the query
			// Based on the DB2 doc, it seems that an EXPLAIN command would be used
			// but this does not work. What does work is to try to execute the query
			// while in RECOMMEND INDEXES mode. This raises an exception because you're 
			// not allowed to execute queries while explaining is enabled... but it
			// will populate the ADVISE_INDEX table anyway!
			qlib.clearAdviseIndex.execute();
			qlib.explainModeRecommendIndexes.execute();
			try { conn.execute(stmt.sql); } catch (SQLException e) { }
			qlib.explainModeNo.execute();

			// get the indexes
			qlib.readAdviseIndex.execute(candidateSet);
			qlib.clearAdviseIndex.execute();
		} catch (SQLException e) {
			try { conn.rollback(); }
			catch (SQLException e2) { }
			System.out.println("Could not execute " + stmt.type + ": " + e.getMessage());
			throw e;
		} 
		
		try {
			conn.rollback();
		} catch (SQLException e) {
		    System.out.println("Could not rollback transaction");
		    throw e;
		}
		
		return candidateSet;
	}
}
