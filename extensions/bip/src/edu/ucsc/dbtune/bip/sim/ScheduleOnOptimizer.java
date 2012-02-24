package edu.ucsc.dbtune.bip.sim;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.BIPOutputOnOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

public class ScheduleOnOptimizer implements BIPOutputOnOptimizer 
{
    private double totalCost;
    
    @Override
    public void verify(Optimizer optimizer, IndexTuningOutput bip, Set<SQLStatement> workload) 
                throws SQLException 
    {
        if ( !(bip instanceof Schedule)) 
            throw new RuntimeException(" The BIP result must be a materialization schedule. ");
     
        Schedule ms = (Schedule) bip;
        // the configuration at each window
        totalCost = 0.0;
        Set<Index> Cw = new HashSet<Index>(ms.getIntialIndexes());
        
        for (int w = 0; w < ms.getNumberWindows(); w++) {
            
            Cw.addAll(ms.getMaterializedIndexes(w));
            Cw.removeAll(ms.getDroppedIndexes(w));
            
            // compute the actual query cost
            for (SQLStatement sql : workload)
                totalCost += optimizer.explain(sql, Cw).getTotalCost();
            
        }
    }
    
    /**
     * Retrieve the total cost of using the actual optimizer
     * @return
     *      The total cost
     */
    public double getTotalCost()
    {
        return totalCost;
    }
}
