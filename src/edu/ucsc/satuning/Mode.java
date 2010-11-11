package edu.ucsc.satuning;


import java.io.File;

import org.apache.commons.cli.ParseException;

import edu.ucsc.satuning.admin.InstantAdmin;
import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.db.DBPortal;

public enum Mode {
	
	WFIT {
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runWFIT(new InstantAdmin<I>(), true, false);
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
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runWFIT(new InstantAdmin<I>(), false, true);
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
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runWFIT(new InstantAdmin<I>(), true, true);
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
	}, 
	
	BC {
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runBC(portal);
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
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runOnlineProfiling(portal);
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("online");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("online");
		}
	},
	
	PROFILE_OFFLINE {
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runOfflineProfiling(portal);
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("offline");
		}
	},
	
	CANDGEN_OFFLINE {
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runOfflineCandidateGeneration(portal);
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("offline");
		}
	},
	
	GOOD {
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runGoodInterventions();
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
	},
	
	BAD {
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runBadInterventions();
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
	},
	
	NOVOTE {
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runNoVoting();
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
	},
	
	SLOW {
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runSlow(true);
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
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runSlow(false);
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
		public <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception {
			new MainTemplate<I>().runAutomatic();
		}
		public File candidatePoolFile() {
			return Configuration.candidatePoolFile("online");
		}
		public File profiledQueryFile() {
			return Configuration.profiledQueryFile("online");
		}
		public File logFile() {
			return Configuration.logFile("automatic_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates);
		}
	};

	public static Mode parseMode(String modeString) throws ParseException {
		for (Mode m : values()) 
			if (modeString.equals(m.toString())) 
				return m;
		throw new ParseException("invalid mode: \""+modeString+"\"");
	}
	
	public static String options() {
		StringBuilder sb = new StringBuilder();
		int numValues = values().length;
		for (int i = 0; i < numValues; i++) {
			sb.append(values()[i]);
			if (i < numValues - 1) sb.append(", ");
		}
		return sb.toString();
	}
	
	public abstract <I extends DBIndex<I>> void run(DBPortal<I> portal) throws Exception;
	public File logFile() { return null; } 
	public File profiledQueryFile() { return null; }
	public File candidatePoolFile() { return null; }
}
