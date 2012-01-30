package interaction.ibg.log;

import java.util.ArrayList;
import java.util.List;

import interaction.db.DB2IndexSet;
import interaction.db.DBConnection;
//import interaction.util.BitSet;

public class InteractionLogger {
	private InteractionBank bank; 
//	private BitSet used;

	private BasicLog tempLog; 

	// for counting what-if calls
	private DBConnection conn; 
	
	// for logging running times
	private long timerStart;
	
	// ----------------------------------------------------------
    // for logging running times to analyze interaction
    private long timerStartAnalyzingInteraction;
    
    // the time to initialize IBG for each statement
    private List<Long> timerBuildingIBG;
    // ------------------------------------------------------------
	
	public InteractionLogger(DBConnection conn0, DB2IndexSet candidateSet0) {
		bank = new InteractionBank(candidateSet0);
		conn = conn0;
//		used = new BitSet();
		tempLog = new BasicLog(bank);
		reset();
	}

	public final void reset() {
		bank.clear();
//		used.clear();
		tempLog.clear();
		timerStart = -1;
		// -------------------------------------------------
        timerStartAnalyzingInteraction = -1;
        timerBuildingIBG = new ArrayList<Long>();
        // ---------------------------------------------------
	}
	
	// ------------------------------------------------------------
    /**
     * Recording the running time to build IBG
     * @param time
     *      The time in millis
     */
    public final void addTimerBuildingIBG(long time)
    {
        timerBuildingIBG.add(time);
    }
    
    /**
     * Starting the counter to calculate the time for analyzing the interaction
     * of a pair of indexes
     */
    public final void startTimerAnalyzingInteraction()
    {
        timerStartAnalyzingInteraction = System.currentTimeMillis(); 
    }
    // ------------------------------------------------------------------------
    
	public final void startTimer() {
		timerStart = System.currentTimeMillis();
		conn.whatifCount = 0;
	}
	
	/*
	 * Assign interaction with an exact value
	 */
	public final void assignInteraction(int id1, int id2, double newValue) {
		double oldValue = bank.interactionLevel(id1, id2);
		if (newValue > oldValue) {
			//assert(timerStart > 0);
			//long millis = System.currentTimeMillis() - timerStart;
			// --------------------------------------------------------------------------
            long millis = System.currentTimeMillis() - timerStartAnalyzingInteraction;
            // --------------------------------------------------------------------------
			int whatif = conn.whatifCount;
			tempLog.add(new BasicLog.Entry(id1, id2, newValue, oldValue, millis, whatif));
		}
		
		bank.assignInteraction(id1, id2, newValue);
	}
	
//	public final double interactionLevel(int id1, int id2) {
//		return bank.interactionLevel(id1, id2);
//	}
	
//	public final void assignUsed(int id) {
//		used.set(id);
//	}
//	
//	public final void assignUsed(BitSet other) {
//		used.or(other);
//	}
	
	public final BasicLog getBasicLog() {
		return tempLog;
	}

	public final InteractionBank getInteractionBank() {
		return bank;
	}
}
