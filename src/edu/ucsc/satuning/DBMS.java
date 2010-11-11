package edu.ucsc.satuning;

import edu.ucsc.satuning.db.ibm.DB2Index;
import edu.ucsc.satuning.db.ibm.DB2Portal;
import edu.ucsc.satuning.db.pg.PGIndex;
import edu.ucsc.satuning.db.pg.PGPortal;

public enum DBMS {
	PG {
		@Override
		public void run(Mode mode) throws Exception {
			mode.<PGIndex>run(new PGPortal());
		}

		@Override
		public void runLogging(Mode mode) throws Exception {
			new MainTemplate<PGIndex>().runLogging(mode);
		}
	},
	IBM {
		@Override
		public void run(Mode mode) throws Exception {
			mode.<DB2Index>run(new DB2Portal());
		}
		
		@Override
		public void runLogging(Mode mode) throws Exception {
			new MainTemplate<DB2Index>().runLogging(mode);
		}
	};
	
	public abstract void run(Mode mode) throws Exception;
	public abstract void runLogging(Mode mode) throws Exception;

	public static DBMS parseArgument(String dbmsString) {
		if (dbmsString.compareToIgnoreCase("pg") == 0)
			return PG;
		else if (dbmsString.compareToIgnoreCase("ibm") == 0 || dbmsString.compareToIgnoreCase("db2") == 0)
			return IBM;
		else
			return null;
	}
}
