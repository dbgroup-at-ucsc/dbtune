package satuning;


import java.io.File;

import satuning.admin.InstantAdmin;

public enum Mode {
	
	WFIT {
		public void run() throws Exception {
			Main.runWFIT(new InstantAdmin(), true, false);
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
		public File logFile() {
			return Configuration.logFile("wfit_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates);
		}
	}, 
	
	OPT {
		public void run() throws Exception {
			Main.runWFIT(new InstantAdmin(), false, true);
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
		public File logFile() {
			return Configuration.logFile("opt_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates);
		}
	}, 
	
	WFIT_OPT {
		public void run() throws Exception {
			Main.runWFIT(new InstantAdmin(), true, true);
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
	}, 
	
	BC {
		public void run() throws Exception {
			Main.runBC();
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
		public File logFile() {
			return Configuration.logFile("bc_"+Configuration.maxHotSetSize);
		}
	},
	
	PROFILE_ONLINE {
		public void run() throws Exception {
			Main.runOnlineProfiling();
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("online");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("online");
		}
	},
	
	PROFILE_OFFLINE {
		public void run() throws Exception {
			Main.runOfflineProfiling();
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
	},
	
	GOOD {
		public void run() throws Exception {
			Main.runGoodInterventions();
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
		public File logFile() {
			// this is the OUTPUT log file, NOT INPUT
			return Configuration.logFile("good_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates);
		}
		public File inputLogFile() {
			return Configuration.logFile("opt_"+Configuration.defaultHotSetSize+"_"+Configuration.defaultNumStates);
		}
		
	},
	
	BAD {
		public void run() throws Exception {
			Main.runBadInterventions();
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
		public File logFile() {
			// this is the OUTPUT log file, NOT INPUT
			return Configuration.logFile("bad_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates);
		}
		public File inputLogFile() {
			return Configuration.logFile("opt_"+Configuration.defaultHotSetSize+"_"+Configuration.defaultNumStates);
		}
	},
	
	NOVOTE {
		public void run() throws Exception {
			Main.runNoVoting();
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
		public File logFile() {
			// this is the OUTPUT log file, NOT INPUT
			return Configuration.logFile("novote_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates);
		}
		public File inputLogFile() {
			return Configuration.logFile("opt_"+Configuration.defaultHotSetSize+"_"+Configuration.defaultNumStates);
		}
	},
	
	SLOW {
		public void run() throws Exception {
			Main.runSlow(true);
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
		public File logFile() {
			return Configuration.logFile("slow_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates+"_"+Configuration.slowAdminLag);
		}
	},
	
	SLOW_NOVOTE {
		public void run() throws Exception {
			Main.runSlow(false);
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
		public File logFile() {
			return Configuration.logFile("slow_novote_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates+"_"+Configuration.slowAdminLag);
		}
	},
	
	AUTO {
		public void run() throws Exception {
			Main.runAutomatic();
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
		public File logFile() {
			return Configuration.logFile("automatic_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates);
		}
	};

    /*
	public static Mode parseMode(String modeString) throws ParseException {
		for (Mode m : values()) 
			if (modeString.equals(m.toString())) 
				return m;
		throw new ParseException("invalid mode: \""+modeString+"\"");
	}
    */
	
	public static String options() {
		StringBuilder sb = new StringBuilder();
		int numValues = values().length;
		for (int i = 0; i < numValues; i++) {
			sb.append(values()[i]);
			if (i < numValues - 1) sb.append(", ");
		}
		return sb.toString();
	}
	
	public abstract void run() throws Exception;
	public File logFile() { return null; } 
	public File inputLogFile() { return null; } 
	public File profiledQueryFile() { return null; }
	public File candidatePoolFile() { return null; }
}
