package edu.ucsc.dbtune.bip.util;

import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * This class abstracts the common methods shared by different BIP solvers
 * for different index tuning related problems.
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public abstract class AbstractBIPSolver implements BIPSolver 
{
    protected List<Index> candidateIndexes;
    protected CPlexBuffer buf;
    protected List<QueryPlanDesc> listQueryPlanDescs;
    protected BIPIndexPool poolIndexes;
    protected Map<Schema, Workload> mapSchemaToWorkload;
    protected InumCommunicator communicator;
    protected Map<Schema, List<IndexFullTableScan>> mapSchemaListIndexFullTableScans;
    protected IloLPMatrix matrix;
    protected IloNumVar [] vars;
    protected IloCplex cplex;
    protected String workloadName;
    protected Environment environment = Environment.getInstance();
    protected int numConstraints;
    
    @Override
    public void setWorkloadName(String workloadName)
    {
        this.workloadName = workloadName;
    }
    
    @Override    
    public void setMapSchemaToWorkload(Map<Schema, Workload> mapSchemaToWorkload)
    {
        this.mapSchemaToWorkload = mapSchemaToWorkload;
    }
    
    @Override
    public void setCandidateIndexes(List<Index> candidateIndexes) {
        this.candidateIndexes = candidateIndexes;
    }
    
    /**
     * TODO: This method is implemented temporarily
     * @param communicator
     */
    public void setCommunicator(InumCommunicator communicator)
    {
        this.communicator = communicator;
    }
   
    @Override
    public BIPOutput solve() throws SQLException
    {   
        // Preprocess steps
        insertIndexesToPool();
        populateSchemaIndexFullTableScans();
        initializeBuffer(this.workloadName);
        
        // 1. Communicate with INUM 
        // to derive the query plan descriptions including internal cost, index access cost, etc.  
        this.populatePlanDescriptionForStatement(this.poolIndexes);
        
        // 2. Build BIP       
        LogListener listener = new LogListener() {
            public void onLogEvent(String component, String logEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
         
        try {
            buildBIP(listener);            
            CPlexBuffer.concat(this.buf.getLpFileName(), buf.getObjFileName(), buf.getConsFileName(), buf.getBinFileName());                          
        }
        catch(IOException e){
            System.out.println("Error " + e);
        }   
        
        // 3. Solve the BIP
        if (this.solveBIP() == true) {
            return this.getOutput();
        } else {
            return null;
        }
    }
    
    
    /**
     * Insert indexes from the candidate indexes into the pool in some order 
     * to make it convenient for constructing variables related to indexes in BIP
     * 
     * For example, in SimBIP, indexes of the same type (created, dropped, or remained)
     * in the system should be stored consecutively.
     * 
     */    
    protected void insertIndexesToPool()
    {
        poolIndexes = new BIPIndexPool();
        // Put all indexes in {@code candidateIndexes} into the pool of {@code MatIndexPool}
        for (Index idx: candidateIndexes) {
            poolIndexes.addIndex(idx);
        }
    }
    
    /**
     * Create the list of full table scan indexes per relation in each schema 
     * that the workload refers to
     * 
     * @throws SQLException
     */
    protected void populateSchemaIndexFullTableScans() throws SQLException
    {
        mapSchemaListIndexFullTableScans = new HashMap<Schema, List<IndexFullTableScan>>();
        for (Entry<Schema, Workload> entry : mapSchemaToWorkload.entrySet()) {
            // create a list of full table scan indexes
            List<IndexFullTableScan> listIndexes = new  ArrayList<IndexFullTableScan>();
            for (Table table : entry.getKey().tables()){
                IndexFullTableScan scanIdx = new IndexFullTableScan(table);
                listIndexes.add(scanIdx);
            }
            mapSchemaListIndexFullTableScans.put(entry.getKey(), listIndexes);
        }
    }
    
    /**
     * Initialize empty buffer files that will store the Binary Integer Program
     * 
     * @param prefix
     *      Prefix name (usually use {@code workloadName})
     *      
     * {\bf Note. }There are four files that are created for a BIP,
     * including: {@code prefix.obj}, {@code prefix.cons}, {@code prefix.bin} and {@code prefix.lp}
     * store the objective function, list of constraints, binary variables, and the whole BIP, respectively 
     *      
     */
    protected void initializeBuffer(String prefix)
    { 
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
     * @param poolIndexes
     *      The pool of indexes managed by the BIP
     *      
     * @throws SQLException
     */
    protected void populatePlanDescriptionForStatement(BIPIndexPool poolIndexes) throws SQLException
    {   
        listQueryPlanDescs = new ArrayList<QueryPlanDesc>();
        
        for (Entry<Schema, Workload> entry : mapSchemaToWorkload.entrySet()) {
            List<IndexFullTableScan> listIndexFullTableScans = mapSchemaListIndexFullTableScans.get(entry.getKey());
            for (Iterator<SQLStatement> iterStmt = entry.getValue().iterator(); iterStmt.hasNext(); ) {
                QueryPlanDesc desc =  new InumQueryPlanDesc();                    
                // Populate the INUM space for each statement
                // We do not add full table scans before populate from @desc
                // so that the full table scan is placed at the end of each slot 
                desc.generateQueryPlanDesc(communicator, entry.getKey(), listIndexFullTableScans, 
                                           iterStmt.next(), poolIndexes);
                listQueryPlanDescs.add(desc);
            }
            
            // Add full table scans into the pool
            for (Index index : listIndexFullTableScans) {
                poolIndexes.addIndex(index);
            }
        }
        // Map index in each slot to its pool ID
        for (QueryPlanDesc desc : listQueryPlanDescs) {
            desc.mapIndexInSlotToPoolID(poolIndexes);
        }
    }
    
    
    /**
     * Build the BIP and store into a text file
     * 
     * @param listener
     *      The logger
     * @throws IOException
     */
    protected abstract void buildBIP(final LogListener listener) throws IOException;
    
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
