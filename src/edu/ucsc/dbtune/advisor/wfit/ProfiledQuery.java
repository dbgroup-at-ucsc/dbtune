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

//CHECKSTYLE:OFF
public class ProfiledQuery implements Serializable {
	private static final long serialVersionUID = 1L;
	ProfiledQuery() { }
	
	private static final IBGCoveringNodeFinder nodeFinder = new IBGCoveringNodeFinder();
	
	public String sql;
    public Snapshot candidateSet;
    public Set<Index> pool;
	public IndexBenefitGraph ibg;
    public ExplainedSQLStatement explainInfo;
	public InteractionBank bank;
	public int whatifCount; // value from DBConnection after profiling
	
    public ProfiledQuery(
            String sql0,
            ExplainedSQLStatement explainInfo0,
            Snapshot candidateSet0,
            Set<Index> pool0,
            IndexBenefitGraph ibg0,
            InteractionBank bank0,
            int whatifCount0)
    {
		sql = sql0;
		explainInfo = explainInfo0;
		candidateSet = candidateSet0;
        pool = pool0;
		ibg = ibg0;
		bank = bank0;
		whatifCount = whatifCount0;
	}
	
	public double cost(BitSet config) {
		double plan = planCost(config);
		double maint = maintenanceCost(config);
		double total = plan + maint;
        //System.out.println("cost = "+plan+" (plan) + "+maint+" (maint) = "+total);
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
        if (explainInfo.getStatement().getSQLCategory().isSame(SELECT))
            return 0;
		
		double maintenanceCost = 0;
        Index idx;

        for (int i = config.nextSetBit(0); i >= 0; i = config.nextSetBit(i+1))
            if ((idx = findIndex(pool, i)) == null)
                throw new RuntimeException("Can't find index with ID " + i + " in pool");
            else
                maintenanceCost += explainInfo.getUpdateCost(idx);

        return maintenanceCost;
	}
    
    private Index findIndex(Set<Index> indexes, int indexId)
    {
        for (Index idx : pool)
            if (idx.getId() == indexId)
                return idx;

        return null;
    }
}
//CHECKSTYLE:ON
