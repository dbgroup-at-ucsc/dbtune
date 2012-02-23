package interaction.ibg.parallel;

//import interaction.util.Debug;
import interaction.util.MinQueue;

import java.sql.SQLException;

public class RRConstructionThread implements Runnable {
	private MinQueue<IndexBenefitGraph> pending;
	
	public RRConstructionThread(IndexBenefitGraph[] ibgs0) {
		pending = new MinQueue<IndexBenefitGraph>(ibgs0);
	}

	public void run() {
		while (pending.size() > 0) {
			long timeSoFar, startTime, endTime;
			IndexBenefitGraph ibg;
			boolean success;

			ibg = pending.best();
			timeSoFar = pending.bestPriority();
			pending.deleteMin();
			
			try {
				startTime = System.currentTimeMillis();
				success = ibg.buildNode();
				endTime = System.currentTimeMillis();
				if (success) 
					pending.insertKey(ibg, timeSoFar + (endTime - startTime));
//				else
//					Debug.println("finished construction of " + ibg.xacts.get(0).id +" == " + ibg.nodeCount());
			} catch (SQLException e) {
				System.err.println(" *** IBG construction failed ***");
				e.printStackTrace();
			}
		}
	}
}
