package edu.ucsc.dbtune.bip.core;


import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

/**
 * This class abstracts the common methods shared by different BIP solvers
 * for different index tuning related problems.
 * 
 * @author Quoc Trung Tran
 *
 */
public abstract class AbstractBIPSolver implements BIPSolver 
{
    protected Set<Index> candidateIndexes;    
    protected Workload workload;
    protected List<QueryPlanDesc> listQueryPlanDescs;
    
    protected CPlexBuffer buf;
    protected CPlexSolver cplex;
    protected Map<String, Integer> mapVariableValue;
    protected Environment environment = Environment.getInstance();
    protected int numConstraints;
    protected InumOptimizer inumOptimizer;    
    protected LogListener logger;
    
    @Override    
    public void setWorkload(Workload wl)
    {
        this.workload = wl;
    }
    
    @Override
    public void setCandidateIndexes(Set<Index> candidateIndexes) 
    {
        this.candidateIndexes = candidateIndexes;
    }
    
    @Override
    public void setOptimizer(Optimizer optimizer) throws Exception 
    {
        if (!(optimizer instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
        
        inumOptimizer = (InumOptimizer) optimizer;
    }
   
    @Override
    public IndexTuningOutput solve() throws SQLException, IOException
    {   
        // 1. Communicate with INUM 
        // to derive the query plan descriptions including internal cost, index access cost, etc.
        logger.setStartTimer();
        populatePlanDescriptionForStatements();
        logger.onLogEvent(LogListener.EVENT_POPULATING_INUM);
        
        // 2. Build BIP    
        logger.setStartTimer();
        initializeBuffer();
        buildBIP();       
        logger.onLogEvent(LogListener.EVENT_FORMULATING_BIP);
        
        // 3. Solve the BIP
        logger.setStartTimer();
        IndexTuningOutput result = null;
        cplex = new CPlexImplementer();
        mapVariableValue = cplex.solve(this.buf.getLpFileName());
        if (mapVariableValue != null)
            result = getOutput();
        
        logger.onLogEvent(LogListener.EVENT_SOLVING_BIP);
        return result;            
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
     
    
    /**
     * Communicate with INUM to populate the query plan description 
     * (e.g, internal plan cost, index access costs, etc.)
     * for each statement in the given workload
     * 
     *      
     * @throws SQLException
     *      if the connection to INUM fails
     */
    protected void populatePlanDescriptionForStatements() throws SQLException
    {   
        listQueryPlanDescs = new ArrayList<QueryPlanDesc>();
        Set<Table> listWorkloadTables = new HashSet<Table>();
        
        for (int i = 0; i < workload.size(); i++) {            
            // Set the corresponding SQL statement
            QueryPlanDesc desc =  InumQueryPlanDesc.getQueryPlanDescInstance(workload.get(i));
            // Populate the INUM space 
            desc.generateQueryPlanDesc(inumOptimizer, candidateIndexes);            
            listQueryPlanDescs.add(desc);
            
            // Add referenced tables of each statement
            // into the ``global'' set {@code listWorkloadTables}
            listWorkloadTables.addAll(desc.getTables());
        }
        
        // Add full table scan indexes into the candidate index set
        for (Table table : listWorkloadTables) {
            FullTableScanIndex scanIdx = getFullTableScanIndexInstance(table);
            candidateIndexes.add(scanIdx);
        }
    }
    
    /**
     * Initialize empty buffer files that will store the Binary Integer Program
     *      *      
     * {\bf Note. }There are four files that are created for a BIP,
     * including: {@code prefix.obj}, {@code prefix.cons}, {@code prefix.bin} and {@code prefix.lp}
     * store the objective function, list of constraints, binary variables, and the whole BIP, respectively 
     *      
     */
    protected void initializeBuffer()
    { 
        String prefix = "wl.sql";
        String name = environment.getTempDir() + "/" + prefix;
        try {
            buf = new CPlexBuffer(name);
        }
        catch (IOException e) {
            System.out.println(" Error in opening files " + e.toString());          
        }
    }
    
    
    /**
     * Retrieve the solver that is used to to solve problem. This function is mainly used
     * to obtain information about the solver (e.g., the objective function, number of variables,
     * number of constraints, etc. )
     * 
     * @return
     *      The CPlexSolver object 
     *      
     */
    public CPlexSolver getSolver()
    {
        return this.cplex;
    }
    
    /**
     * Build the BIP and store into a text file
     * 
     * @throws IOException
     */
    protected abstract void buildBIP();
       
    /**
     * Generate the output for the problem by manipulating the outcome from {@code cplex} object
     * 
     * @return
     *      The output formatted for a particular problem 
     *      (e.g., MaterializationSchedule for Scheduling Index Materialization problem)
     */
    protected abstract IndexTuningOutput getOutput();
}
