package edu.ucsc.satuning.engine;

import java.io.IOException;
import java.io.Serializable;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.db.ExplainInfo;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.ibg.IBGCoveringNodeFinder;
import edu.ucsc.satuning.ibg.IndexBenefitGraph;
import edu.ucsc.satuning.ibg.log.InteractionBank;
import edu.ucsc.satuning.util.BitSet;

public class ProfiledQuery<I extends DBIndex<I>> implements Serializable {
	
	private static final IBGCoveringNodeFinder nodeFinder = new IBGCoveringNodeFinder();
	
	public String sql;
	public ExplainInfo<I> explainInfo;
	public Snapshot<I> candidateSet;
	public IndexBenefitGraph ibg;
	public InteractionBank bank;
	public int whatifCount; // value from DatabaseConnection after profiling
	public double ibgAnalysisTime; // in milliseconds

	// Serialization support
	private static final long serialVersionUID = 2L;
	ProfiledQuery() {}
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeObject(sql);
		out.writeObject(explainInfo);
		out.writeObject(candidateSet);
		out.writeObject(ibg);
		out.writeObject(bank);
		out.writeInt(whatifCount);
		out.writeDouble(ibgAnalysisTime);
	}
	
	@SuppressWarnings("unchecked")
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		sql = (String) in.readObject();
		explainInfo = (ExplainInfo<I>) in.readObject();
		candidateSet = (Snapshot<I>) in.readObject();
		ibg = (IndexBenefitGraph) in.readObject();
		bank = (InteractionBank) in.readObject();
		whatifCount = in.readInt();
		ibgAnalysisTime = in.readDouble();
	}

	
	public ProfiledQuery(String sql0, ExplainInfo<I> explainInfo0, Snapshot<I> candidateSet0, IndexBenefitGraph ibg0, InteractionBank bank0, int whatifCount0, double ibgAnalysisTime0) {
		sql = sql0;
		explainInfo = explainInfo0;
		candidateSet = candidateSet0;
		ibg = ibg0;
		bank = bank0;
		whatifCount = whatifCount0;
		ibgAnalysisTime = ibgAnalysisTime0;
	}
	
	public double totalCost(BitSet config) {
		double plan = planCost(config);
		double maint = maintenanceCost(config);
		double total = plan + maint;
		//Debug.println("cost = "+plan+" (plan) + "+maint+" (maint) = "+total);
		return total;
	}
	
	public double planCost(BitSet config) {
		return nodeFinder.findCost(ibg, config);
	}
	
	public double maintenanceCost(BitSet config) {
		if (!explainInfo.isDML()) 
			return 0;
		
		double maintenanceCost = 0;
		for (I index : candidateSet) {
			if (config.get(index.internalId())) {
				maintenanceCost += explainInfo.maintenanceCost(index);
			}
		}
		return maintenanceCost;
	}
}
