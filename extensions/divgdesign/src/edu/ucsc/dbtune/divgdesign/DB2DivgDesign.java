package src.edu.ucsc.dbtune.divgdesign;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class DB2DivgDesign extends DivgDesign 
{
    private DB2Advisor db2Advis;
    private Optimizer  optimizer;
 
    
    /**
     * Set the advisor that will be used to recommend indexes for a given workload.
     * 
     * @param db2advis
     *      The DB2 advisor
     */
    public DB2DivgDesign(DB2Advisor db2Advis, Optimizer optimizer)
    {
        this.db2Advis = db2Advis;
        this.optimizer = optimizer;
    }
    

    @Override
    protected Set<Index> getRecommendation(List<SQLStatement> sqls) throws SQLException 
    {
        Workload wlPartition = new Workload(sqls);
        db2Advis.process(wlPartition);
        return db2Advis.getRecommendation();
    }

    @Override
    protected List<QueryCostAtPartition> statementCosts(SQLStatement sql) throws SQLException 
    {
        // iterate over every replica 
        // and compute the cost
        List<QueryCostAtPartition> costs = new ArrayList<QueryCostAtPartition>();
        double cost;
        
        for (int i = 0; i < n; i++) {
            
            cost = optimizer.explain(sql, indexesAtReplica.get(i)).getTotalCost();
            costs.add(new QueryCostAtPartition(i, cost));
            
        }
            
        return costs;
    }

    
}
