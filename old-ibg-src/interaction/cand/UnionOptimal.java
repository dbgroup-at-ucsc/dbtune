package interaction.cand;

import java.sql.SQLException;

import interaction.db.*;
import interaction.db.DBConnection.QueryLibrary;
import interaction.workload.*;

public class UnionOptimal {
	public static DB2IndexSet getCandidates(DBConnection conn, SQLWorkload xacts) throws SQLException {
		QueryLibrary qlib = conn.qlib;
		DB2IndexSet candidateSet = new DB2IndexSet();
		
		for (SQLTransaction xact : xacts) {
			boolean fail = false;			
			for (SQLStatement query : xact) {
				try {
					switch (query.type) {
						case QUERY:
							// Populate the ADVISE_INDEX with indexes for the query
							// Based on the DB2 doc, it seems that an EXPLAIN command would be used
							// but this does not work. What does work is to try to execute the query
							// while in RECOMMEND INDEXES mode. This raises an exception because you're 
							// not allowed to execute queries while explaining is enabled... but it
							// will populate the ADVISE_INDEX table anyway!
							qlib.clearAdviseIndex.execute();
							qlib.explainModeRecommendIndexes.execute();
							try { conn.execute(query.sql); } catch (SQLException e) { }
							qlib.explainModeNo.execute();
							
							// get the indexes
							qlib.readAdviseIndex.execute(candidateSet);
							qlib.clearAdviseIndex.execute();
							break;
						case DML:
							// Note: we might candidates for updates, but right now we ignore them
							break;
						case DDL:
							conn.execute(query.sql);
							break;
					}
				} catch (SQLException e) {
					System.out.println("Could not execute " + query.type + ": " + e.getMessage());
					fail = true;
					break;
				}
			}
			
			// abort or commit
			if (fail) conn.rollback(); 
			else conn.commit();
		}
		
		// normalize the index candidates
		candidateSet.normalize();
		
		return candidateSet;
	}
}
