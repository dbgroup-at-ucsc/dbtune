package edu.ucsc.dbtune.bip.div;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.workload.SQLStatement;

public class OnlineDivBIP extends AbstractBIPSolver implements OnlineBIP
{
    protected int window;
    protected int startID;
    protected int endID;
    protected DivBIP divBIP;
    protected DivConfiguration initialConf; 
    // map the cost of statements w.r.t  initial configuration
    protected Map<Integer, Double> mapStmtInitialCost;
    
    @Override
    protected void buildBIP() 
    {
        throw new RuntimeException("This method is not applicable");
    }

    @Override
    protected IndexTuningOutput getOutput() throws Exception 
    {
        throw new RuntimeException("This method is not applicable");
    }
    
    /**
     * Populate the set of query plan descriptions
     * @throws Exception
     */
    @Override
    public void populateQueryPlanDescriptions() throws Exception
    {
        super.populatePlanDescriptionForStatements();
    }

    @Override
    public void next() throws Exception
    {
        // Advance one more statement
        endID++;
        if (endID >= workload.size())
            endID = workload.size() - 1;
        
        // advance startID in order to fit in the window
        if (endID - startID + 1 > window)
            startID++;
        
        List<QueryPlanDesc> descs = new ArrayList<QueryPlanDesc>();
        for (int id = startID; id <= endID; id++)
            descs.add(super.queryPlanDescs.get(id));        
        
        // Set the set of query plan description
        divBIP.setQueryPlanDesc(descs);
        divBIP.solve();
    }

    @Override
    public void setWindowDuration(int w) 
    {
        window = w;
        startID = 0;
        endID = -1;
    }

    @Override
    public double getTotalCost() 
    {   
        return divBIP.getObjValue();
    }

    @Override
    public double getTotalCostInitialConfiguration() throws Exception
    {
        double totalCost = 0.0;
        double cost;
        for (int id = startID; id <= endID; id++){
            if (this.mapStmtInitialCost.containsKey(id))
                totalCost += this.mapStmtInitialCost.get(id);
            else  {
                cost = this.computeInitialCost(workload.get(id));
                this.mapStmtInitialCost.put(id, cost);
                totalCost += cost;
            }
        }
        return totalCost;
    }
    
    /**
     * Set the initial configuration
     * @param conf
     */
    public void setInitialConfiguration(DivConfiguration conf)
    {
        this.initialConf = conf;
        this.mapStmtInitialCost = new HashMap<Integer, Double>();
    }
    
    /**
     * Set an given DivBIP object to solve each window
     * 
     * @param divBIP
     */
    public void setDivBIP(DivBIP divBIP)
    {
        this.divBIP = divBIP;
    }
    
    /**
     * Compute the cost of the given statement w.r.t the initial configuration
     * 
     * @param sql
     *      The given SQL statement
     * @return
     */
    protected double computeInitialCost(SQLStatement sql) throws Exception
    {
        double cost;
        List<Double> costs = new ArrayList<Double>();
        InumPreparedSQLStatement inumPrepared;
        
        for (int r = 0; r < initialConf.getNumberReplicas(); r++) {
            inumPrepared = (InumPreparedSQLStatement) this.inumOptimizer.prepareExplain(sql);
            cost = inumPrepared.explain(initialConf.indexesAtReplica(r)).getTotalCost();
            costs.add(cost);
        }
        
        // Get the top-k best costs
        Collections.sort(costs);
        cost = 0.0;
        for (int k = 0; k < initialConf.getLoadFactor(); k++)
            cost += costs.get(k);
        
        // get an average
        return (double) cost / initialConf.getLoadFactor();
    }

}
