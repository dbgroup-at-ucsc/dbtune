package edu.ucsc.dbtune.bip.core;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * The class uses DB2 Optizimer to generate candidate indexes
 * @author tqtrung
 *
 */
public class DB2CandidateGenerator implements CandidateGenerator 
{
    private Optimizer optimizer;
    private Workload workload;
    
    @Override
    public Set<Index> oneColumnCandidateSet() throws SQLException 
    {
        Set<Index> oneColIndexes = new HashSet<Index>();
        
        for (Index index : optimalCandidateSet()) {
            
            for (Column col : index.columns()) {
                
                try {
                    Index oneColIndex = new Index(col, index.isAscending(col));
                    oneColIndexes.add(oneColIndex);
                } catch (SQLException e) {
                    continue;
                }
                
            }
        }
            
        return oneColIndexes;
    }

    @Override
    public Set<Index> optimalCandidateSet() throws SQLException 
    {
        Set<Index> indexes = new HashSet<Index>();
        boolean isRecommended;
        
        for (SQLStatement stmt : workload) {
            Set<Index> stmtIndexes =  optimizer.recommendIndexes(stmt); 
        
            for (Index index : stmtIndexes) {
                isRecommended = false;
                for (Index recommended : indexes) {
                    if (recommended.equalsContent(index)) {
                        isRecommended = true;
                        break;
                    }   
                }
                
                if (!isRecommended)
                    indexes.add(index);
            }
        }
        
        return indexes;
    }

    @Override
    public Set<Index> powerSetCandidateSet()  throws SQLException
    {
        return null;

    }

    @Override
    public void setOptimizer(Optimizer optimizer) 
    {
        this.optimizer = optimizer;
    }

    @Override
    public void setWorkload(Workload wl) 
    {
        this.workload = wl;
    }
    
    

}
