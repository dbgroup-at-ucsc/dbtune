package edu.ucsc.dbtune.bip.core;

import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

/**
 * This class abstracts the common methods shared by different BIP solvers
 * for different index tuning related problems.
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public abstract class AbstractBIPSolver implements BIPSolver 
{
    protected Set<Index> candidateIndexes;    
    protected Workload workload;
    protected List<QueryPlanDesc> listQueryPlanDescs;
    protected IloLPMatrix matrix;
    protected IloNumVar [] vars;
    protected IloCplex cplex;
    
    protected CPlexBuffer buf;
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
    public void setOptimizer(InumOptimizer optimizer) 
    {
        this.inumOptimizer = optimizer;
    }
    
   
    @Override
    public BIPOutput solve() throws SQLException, IOException
    {   
        // Preprocess steps        
        initializeBuffer();
        logger.onLogEvent(LogListener.EVENT_PREPROCESS);
        
        // 1. Communicate with INUM 
        // to derive the query plan descriptions including internal cost, index access cost, etc.  
        this.populatePlanDescriptionForStatements();
        logger.onLogEvent(LogListener.EVENT_INUM_POPULATING);
        
        // 2. Build BIP        
        buildBIP();       
        logger.onLogEvent(LogListener.EVENT_BIP_FORMULATING);
        
        // 3. Solve the BIP
        BIPOutput result = null;
        if (this.solveBIP() == true) {
            result = this.getOutput();
        }
        logger.onLogEvent(LogListener.EVENT_BIP_SOLVING);
        return result;            
    }
    
    /**
     * Set the listener that logs the running time
     * 
     * @param listener
     *      The listener
     */
    public void setLogListenter(LogListener logger)
    {
        this.logger = logger;
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
            this.buf = new CPlexBuffer(name);
        }
        catch (IOException e) {
            System.out.println(" Error in opening files " + e.toString());          
        }
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
        for (Iterator<SQLStatement> iterStmt = workload.iterator(); iterStmt.hasNext(); ) {
            // Set the corresponding SQL statement
            QueryPlanDesc desc =  InumQueryPlanDesc.getQueryPlanDescInstance(iterStmt.next());
            // Populate the INUM space 
            desc.generateQueryPlanDesc(inumOptimizer, candidateIndexes);            
            listQueryPlanDescs.add(desc);
            
            // Add referenced tables of each statement
            // into the ``global'' set {@code listWorkloadTables}
            listWorkloadTables.addAll(desc.getTables());
        }
        
        // Add full table scan indexes into the pool
        for (Table table : listWorkloadTables) {
            FullTableScanIndex scanIdx = getFullTableScanIndexInstance(table);
            candidateIndexes.add(scanIdx);
        }
    }
    
    
    /**
     * Build the BIP and store into a text file
     * 
     * @throws IOException
     */
    protected abstract void buildBIP();
    
    /**
     * Call CPLEX to solve the formulated BIP, imported from the file
     * 
     * @return
     *      {@code boolean} value to indicate whether the BIP has a feasible solution or not. 
     */
    protected boolean solveBIP()
    {
        try {               
            cplex = new IloCplex(); 
                      
            // Read model from file into cplex optimizer object
            cplex.importModel(this.buf.getLpFileName()); 
            
            // Solve the model 
            return cplex.solve();
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
        
        return false;
    }
    
    /**
     * Generate the output for the problem by manipulating the outcome from {@code cplex} object
     * 
     * @return
     *      The output formatted for a particular problem 
     *      (e.g., MaterializationSchedule for Scheduling Index Materialization problem)
     */
    protected abstract BIPOutput getOutput();
    
    
    /**
     * Retrieve the matrix used in the BIP problem
     * 
     * @param cplex
     *      The model of the BIP problem
     * @return
     *      The matrix of @cplex      
     */ 
    protected IloLPMatrix getMatrix(IloCplex cplex) throws IloException 
    {
        @SuppressWarnings("unchecked")
        Iterator iter = cplex.getModel().iterator();
        
        while (iter.hasNext()) {
            Object o = iter.next();
            if (o instanceof IloLPMatrix) {
                IloLPMatrix matrix = (IloLPMatrix) o;
                return matrix;
            }
        }
        return null;
    }
}
