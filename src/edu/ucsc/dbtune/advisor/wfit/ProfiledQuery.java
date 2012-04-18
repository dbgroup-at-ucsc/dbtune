package edu.ucsc.dbtune.advisor.wfit;

import java.io.Serializable;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.advisor.interactions.InteractionBank;
import edu.ucsc.dbtune.advisor.wfit.CandidatePool.Snapshot;
import edu.ucsc.dbtune.ibg.IBGCoveringNodeFinder;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;

import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;

public class ProfiledQuery implements Serializable {
	private static final long serialVersionUID = 1L;
	ProfiledQuery() { }
	
	private static final IBGCoveringNodeFinder nodeFinder = new IBGCoveringNodeFinder();
	
	public String sql;
    public Snapshot candidateSet;
	public IndexBenefitGraph ibg;
    public ExplainedSQLStatement explainInfo;
	public InteractionBank bank;
	public int whatifCount; // value from DBConnection after profiling
	
    public ProfiledQuery(
            String sql0,
            ExplainedSQLStatement explainInfo0,
            Snapshot candidateSet0,
            IndexBenefitGraph ibg0,
            InteractionBank bank0,
            int whatifCount0)
    {
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
        Set<Index> configSet = new HashSet<Index>();
        for (int i = config.nextSetBit(0); i >= 0; i = config.nextSetBit(i+1)) {
            Index idx = candidateSet.findIndexId(i);

            if (idx == null)
                throw new RuntimeException("Can't find index with ID " + i + " in pool");

            configSet.add(idx);
        }

        return nodeFinder.find(ibg, configSet).getCost();
	}
	
	private double maintenanceCost(BitSet config) {
        if (!explainInfo.getStatement().getSQLCategory().isSame(SELECT))
            return 0;
		
		double maintenanceCost = 0;
		for (Index index : candidateSet) {
			if (config.get(index.getId())) {
                maintenanceCost += explainInfo.getUpdateCost(index);
			}
		}
		return maintenanceCost;
	}
}
