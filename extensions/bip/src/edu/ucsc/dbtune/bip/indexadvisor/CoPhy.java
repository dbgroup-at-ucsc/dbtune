package edu.ucsc.dbtune.bip.indexadvisor;

import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.BIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

public class CoPhy implements BIPSolver, ConstraintIndexAdvisor 
{
    private Set<Index> candidates;
    private Optimizer  optimizer;
    private List<SQLStatement> workload;
    private double     B;
    private LogListener logger;
    private double      objVal;
        
    @Override
    public void setCandidateIndexes(Set<Index> candidateIndexes) 
    {
        candidates = candidateIndexes;
    }

    @Override
    public void setOptimizer(Optimizer optimizer) 
    {
        this.optimizer = optimizer;
    }

    @Override
    public void setWorkload(List<SQLStatement> wl) 
    {
        workload = wl;
    }

    @Override
    public void setSpaceBudget(double B)
    {
        this.B = B;
    }
    
    /**
     * Retrieve the objective value returned by BIP
     * 
     * @return
     *      The objective value
     */
    public double getObjValue()
    {
        return objVal;
    }
    
    
    /**
     * Set the listener that logs the running time (for experimental purpose)
     * 
     * @param listener
     *      The listener
     */
    public void setLogListenter(LogListener logger)
    {
        this.logger = logger;
    }
    
    @Override
    public IndexTuningOutput solve() throws Exception 
    {
        DivBIP div = new DivBIP();
        
        div.setCandidateIndexes(candidates);
        div.setWorkload(workload); 
        div.setOptimizer(optimizer);
        div.setNumberReplicas(1);
        div.setLoadBalanceFactor(1);
        div.setSpaceBudget(B);
        div.setLogListenter(logger);
        
        DivConfiguration output = (DivConfiguration) div.solve();
        Set<Index> recommended = null;
        IndexTuningOutput result = null;
        
        if (output != null) {
            recommended = output.indexesAtReplica(0);
            result = new IndexTuningOutput();
            result.setIndexes(recommended);
            objVal = div.getObjValue();
        }
        
        return result;
    }
}
