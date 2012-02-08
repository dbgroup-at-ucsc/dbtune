package edu.ucsc.dbtune.bip.interactions;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.bip.core.BIPOutputOnActualOptimizer;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

public class InteractionOnOptimizer implements BIPOutputOnActualOptimizer 
{
    private Map<Integer, Set<Index>> mapThetaIndexSet;
    Index first, second;
    double doiOptimizer;
    
    /**
     * Set the pair of indexes that need to verify the interaction
     * 
     * @param first
     *      The first interacting index
     * @param second
     *      The second interacting index
     * @param mapThetaIndexSet
     *      The four configuration (Aempty, Ac, Ac, Acd) that support the interacting of indexes
     *      {@code first} and {@code second}
     */
    public InteractionOnOptimizer(Index first, Index second, Map<Integer, Set<Index>> mapThetaIndexSet)
    {
        this.mapThetaIndexSet = mapThetaIndexSet;
        this.first = first;
        this.second = second;
    }
    
    @Override
    public void verify(Optimizer optimizer, BIPOutput bip, Set<SQLStatement> workload) 
                throws SQLException 
    {
        Set<Index> Aempty, Ac, Ad, Acd;
        
        // derive four configuration
        Aempty = new HashSet<Index>(mapThetaIndexSet.get(InteractionBIP.IND_EMPTY));
        Ac = new HashSet<Index>(mapThetaIndexSet.get(InteractionBIP.IND_C));
        Ad = new HashSet<Index>(mapThetaIndexSet.get(InteractionBIP.IND_D));
        Acd = new HashSet<Index>(mapThetaIndexSet.get(InteractionBIP.IND_CD));
        
        Ac.add(first);
        Ad.add(second);
        Acd.add(first);
        Acd.add(second);
        
        // compute the cost
        // not that we only have one statement
        double costAempty = 0.0, costAc = 0.0, costAd = 0.0, costAcd = 0.0;
        for (SQLStatement sql : workload) {
            System.out.println(" EMPTY : " + optimizer.explain(sql, new HashSet<Index>()).getTotalCost());
            costAempty = optimizer.explain(sql, Aempty).getTotalCost();
            costAc = optimizer.explain(sql, Ac).getTotalCost();
            costAd = optimizer.explain(sql, Ad).getTotalCost();
            costAcd = optimizer.explain(sql, Acd).getTotalCost();
            
            System.out.println(" Aempty: " + Aempty + " cost: " + costAempty);
            System.out.println(" Ac: " + Ac + " cost: " + costAc);
            System.out.println(" Ad: " + Ad + " cost: " + costAd);
            System.out.println(" Acd: " + Acd + " cost: " + costAcd);
            
            
            
        }
        
        doiOptimizer = Math.abs(costAcd + costAempty - costAc - costAd) / costAcd;
        System.out.println("L75, doi Optimizer: " + doiOptimizer);
    }
    
    /**
     * Retrieve the computed degree of interaction
     * 
     * @return
     *      The {@code doi} value
     */
    public double getDoiOptimizer()
    {
        return doiOptimizer;
    }
}
