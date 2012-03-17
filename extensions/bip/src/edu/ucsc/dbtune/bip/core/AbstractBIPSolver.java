package edu.ucsc.dbtune.bip.core;


import ilog.concert.IloException;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.cplex.IloCplex;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
    public static double UNKNOWN_OBJ_VALUE = -99999999;
    
    protected Set<Index>          candidateIndexes;    
    protected Workload            workload;
    protected List<QueryPlanDesc> queryPlanDescs;
    
    protected CPlexBuffer     buf;
    protected IloCplex        cplex;
    protected List<IloNumVar> cplexVar; 
    protected double[]        valVar;
    
    protected Environment   environment = Environment.getInstance();
    
    protected int           numConstraints;
    protected InumOptimizer inumOptimizer;    
    protected LogListener   logger;
    protected double        objVal;
    
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
    public IndexTuningOutput solve() throws Exception
    {   
        // 1. Communicate with INUM 
        // to derive the query plan descriptions including internal cost, index access cost, etc.
        logger.setStartTimer();
        populatePlanDescriptionForStatements();
        logger.onLogEvent(LogListener.EVENT_POPULATING_INUM);
        
        // 2. Build BIP    
        logger.setStartTimer();
        
        // start CPLEX
        cplex = new IloCplex();
        // allow the solution differed 5% from the actual optimal value
        cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
        // not output the log of CPLEX
        cplex.setOut(null);
        // not output the warning
        cplex.setWarning(null);
        
        buildBIP();       
        logger.onLogEvent(LogListener.EVENT_FORMULATING_BIP);
        
        // 3. Solve the BIP
        logger.setStartTimer();
        IndexTuningOutput result = null;
        
        if (cplex.solve()) {
            getMapVariableValue();
            result = getOutput();
            objVal = cplex.getObjValue();
        } else {
            objVal = UNKNOWN_OBJ_VALUE;
        }
        
        logger.onLogEvent(LogListener.EVENT_SOLVING_BIP);
        return result;            
    }
    
    /**
     * Export the CPLEX into the specified file
     * 
     * @param file
     *      The path to the file that stores this BIP.
     *       
     */
    public void exportCplexToFile(String file)
    {
        try {
            cplex.exportModel(file);
        } catch (IloException e) {
            e.printStackTrace();
        }
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
        queryPlanDescs            = new ArrayList<QueryPlanDesc>();
        Set<Table> workloadTables = new HashSet<Table>();
        
        for (int i = 0; i < workload.size(); i++) {            
            // Set the corresponding SQL statement
            QueryPlanDesc desc =  InumQueryPlanDesc.getQueryPlanDescInstance(workload.get(i));
            // Populate the INUM space 
            desc.generateQueryPlanDesc(inumOptimizer, candidateIndexes);            
            queryPlanDescs.add(desc);
            
            // Add referenced tables of each statement
            // into the ``global'' set {@code listWorkloadTables}
            workloadTables.addAll(desc.getTables());
        }
        
        // Add full table scan indexes into the candidate index set
        for (Table table : workloadTables) {
            FullTableScanIndex scanIdx = getFullTableScanIndexInstance(table);
            candidateIndexes.add(scanIdx);
        }
    }
    
    /**
     * Retrieve the assignment of variables by CPLEX
     * @throws Exception
     */
    protected void getMapVariableValue() throws Exception
    {   
        valVar = cplex.getValues(cplexVar.toArray(new IloNumVar[cplexVar.size()]));
    }
  
    /**
     * Create corresponding variables in CPLEX model.
     * 
     * @throws IloException 
     * 
     */
    protected void createCplexVariable(List<BIPVariable> vars) throws IloException
    {   
        IloNumVarType[] type;
        double[]        lb;
        double[]        ub;
        int             size;
        
        size = vars.size();
        type = new IloNumVarType[size];
        lb   = new double[size];
        ub   = new double[size];
        
        // initial variables as Binary Type
        for (int i = 0; i < size; i++) {
            type[i] = IloNumVarType.Int;
            lb[i]   = 0.0;
            ub[i]   = 1.0;
        }
            
        IloNumVar[] iloVar = cplex.numVarArray(size, lb, ub, type);
        cplex.add(iloVar);
        
        for (int i = 0; i < size; i++) {
            iloVar[i].setName(vars.get(i).getName());
        }
        
        cplexVar = new ArrayList<IloNumVar>(Arrays.asList(iloVar));
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
