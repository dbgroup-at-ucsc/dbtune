package satuning.engine;

import java.io.Serializable;

import satuning.db.DB2Index;
import satuning.db.ExplainInfo;
import satuning.engine.CandidatePool.Snapshot;
import satuning.ibg.IBGCoveringNodeFinder;
import satuning.ibg.IndexBenefitGraph;
import satuning.ibg.log.InteractionBank;
import satuning.util.BitSet;

public class ProfiledQuery implements Serializable {
	private static final long serialVersionUID = 1L;
	ProfiledQuery() { }
	
	private static final IBGCoveringNodeFinder nodeFinder = new IBGCoveringNodeFinder();
	
	public String sql;
	public ExplainInfo explainInfo;
	public Snapshot candidateSet;
	public IndexBenefitGraph ibg;
	public InteractionBank bank;
	public int whatifCount; // value from DBConnection after profiling
	
	public ProfiledQuery(String sql0, ExplainInfo explainInfo0, Snapshot candidateSet0, IndexBenefitGraph ibg0, InteractionBank bank0, int whatifCount0) {
		sql = sql0;
		explainInfo = explainInfo0;
		candidateSet = candidateSet0;
		ibg = ibg0;
		bank = bank0;
		whatifCount = whatifCount0;
	}
	
	public double cost(BitSet config) {
		double plan = planCost(config);
		double maint = maintenanceCost(config);
		double total = plan + maint;
		//Debug.println("cost = "+plan+" (plan) + "+maint+" (maint) = "+total);
		return total;
	}
	
	private double planCost(BitSet config) {
		return nodeFinder.findCost(ibg, config);
	}
	
	private double maintenanceCost(BitSet config) {
		if (!explainInfo.isDML()) 
			return 0;
		
		double maintenanceCost = 0;
		for (DB2Index index : candidateSet) {
			if (config.get(index.internalId())) {
				maintenanceCost += explainInfo.maintenanceCost(index);
			}
		}
		return maintenanceCost;
	}
}
