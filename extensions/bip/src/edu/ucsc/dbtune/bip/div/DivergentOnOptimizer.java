package edu.ucsc.dbtune.bip.div;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.BIPOutputOnOptimizer;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

public class DivergentOnOptimizer implements BIPOutputOnOptimizer 
{
    private double totalCost;
    private double updateCost;
    
    @Override
    public void verify(Optimizer optimizer, IndexTuningOutput bip,
                       Set<SQLStatement> workload) throws SQLException 
    {
        if (!(bip instanceof DivConfiguration)) 
            throw new RuntimeException(" The BIP result must be a divergent configuration. ");
     
        DivConfiguration div = (DivConfiguration) bip;
        
        totalCost = 0.0;
        updateCost = 0.0;
        
        // for each statement, find top-m replicas that yield the top-m best costs
        for (SQLStatement sql : workload) {
            
            List<Double> cost = new ArrayList<Double>();
            
            for (int r = 0; r < div.getNumberReplicas(); r++)
                cost.add(optimizer.explain(sql, div.indexesAtReplica(r)).getTotalCost());
            
            // update statement
            if (sql.getSQLCategory().isSame(NOT_SELECT)) 
                for (int k = 0; k < cost.size(); k++) {
                    totalCost += cost.get(k);
                    updateCost += cost.get(k); 
                }
            // select statement
            else {
                // get top-m best cost
                Collections.sort(cost);
                
                for (int k = 0; k < div.getLoadFactor(); k++)
                    totalCost += (cost.get(k) / div.getLoadFactor());
            }
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

    /**
     * Retrieve the total update cost of using the actual optimizer
     * @return
     *      The total cost
     */
    public double getUpdateCost()
    {
        return updateCost;
    }
}
