package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import edu.ucsc.dbtune.bip.util.BIPIndexPool;
import edu.ucsc.dbtune.bip.util.IndexFullTableScan;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.BIPPreparatorSchema;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.WorkloadPerSchema;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;

public class DivBIP 
{  
    private IloCplex cplex; 
    private Environment environment = Environment.getInstance();
    private DivLinGenerator genDiv;
    private IloLPMatrix matrix;
    private IloNumVar [] vars;
    private int Nreplicas, loadfactor;
    private BIPIndexPool poolIndexes;
    
    
    /** 
     * Divergent index tuning
     *     
     * @param listWorkload
     *      Each entry of this list is a list of SQL Statements that belong to a same schema
     * @param candidateIndexes
     *      List of candidate indexes (the same at each replica)
     * @param Nreplicas
     *      The number of replicas
     * @param loadfactor
     *      Load balancing factor
     * @param B
     *      The maximum space to be constrained all the materialized index at each replica
     * 
     * 
     * @return
     *      A set of indexes to be materialized at each replica
     * @throws SQLException 
     * 
     * {\b Note}: {@code listAgent} will be removed when this class is fully implemented
     */
    public DivRecommendedConfiguration optimalDiv(List<WorkloadPerSchema> listWorkload, List<BIPPreparatorSchema> listPreparators,
                                                  List<Index> candidateIndexes, int Nreplicas, int loadfactor, double B) 
                                                throws SQLException
    {   
        this.Nreplicas = Nreplicas;
        this.loadfactor = loadfactor;
        this.poolIndexes = new BIPIndexPool();
        
        // Put all indexes in {@code candidateIndexes} into the pool of {@code MatIndexPool}
        for (Index idx: candidateIndexes) {
            poolIndexes.addIndex(idx);
        }
      
        
        int iPreparator = 0;
        List<QueryPlanDesc> listQueryPlans = new ArrayList<QueryPlanDesc>();
        for (WorkloadPerSchema wl : listWorkload) {
            //BIPAgentPerSchema agent = new BIPAgentPerSchema(wl.getSchema());
            //TODO: Change this line when the implementation of BIPAgentPerSchema is done
            BIPPreparatorSchema preparator = listPreparators.get(iPreparator++);
            
            for (Iterator<SQLStatement> iterStmt = wl.getWorkload().iterator(); iterStmt.hasNext(); ) {
                QueryPlanDesc desc = new QueryPlanDesc(); 
                desc.generateQueryPlanDesc(preparator, iterStmt.next(), candidateIndexes);
                listQueryPlans.add(desc);
            }
            
            // Add full table scans into the pool
            for (Index index : preparator.getListFullTableScanIndexes()) {
                poolIndexes.addIndex(index);
            }
            
            // Map index in each slot to its pool ID
            for (QueryPlanDesc desc : listQueryPlans) {
                desc.mapIndexInSlotToPoolID(poolIndexes);
            }
        }
        
        // 5. Formulate BIP and run the BIP to derive the set of indexes materialized 
        // at each replica
        return buildOptimalDivergentIndex(listQueryPlans, this.Nreplicas, this.loadfactor, B);
    }
    
    
    
    /**
     * Find an optimal divergen design
     * 
     * @param desc
     *     Query plan description including (internal plan, access costs) derived from INUM  
     * @param W
     *      The number of window time
     * @param B
     *      The maximum space budget at each window time       
     * 
     * @return
     *      The set of materialized indexes with marking the time window when this index is created/dropped
     */
    private DivRecommendedConfiguration buildOptimalDivergentIndex(List<QueryPlanDesc> listQueryPlans, int Nreplicas, int loadfactor, double B)  
    {   
        LogListener listener = new LogListener() {
            public void onLogEvent(String component, String logEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
        
        String cplexFile = "", binFile = "", consFile = "", objFile = "";   
        String workloadName = environment.getTempDir() + "/testwl";
        
        try {                                                       
            genDiv = new DivLinGenerator(workloadName, poolIndexes, listQueryPlans, Nreplicas, loadfactor, B);
            
            // Build BIP for a particular (c,d, @desc)
            genDiv.build(listener); 
            
            cplexFile = workloadName + ".lp";
            binFile = workloadName + ".bin";
            consFile = workloadName + ".cons";
            objFile = workloadName + ".obj";
            
            CPlexBuffer.concat(cplexFile, objFile, consFile, binFile);                          
        }
        catch(IOException e){
            System.out.println("Error " + e);
        }   
        
        
        //Load the corresponding CPLEX problem from the corresponding text file
        try {               
            cplex = new IloCplex(); 
                      
            // Read model from file with name @cplexFile into cplex optimizer object
            cplex.importModel(cplexFile); 
            
            // Solve the model and record the solution into @listIndex 
            // if one was found
            if (cplex.solve()) {  
                System.out.println("In DivBIP, objective value: " + cplex.getObjValue());
                return getListIndexEachReplica();
            } 
            else {
                System.out.println(" INFEASIBLE solution ");
            }
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
         }
        
        return null;        
    }
    
    /**
     * The method reads the value of variables corresponding to the presence of indexes
     * at each replica and returns this list of indexes to materialize at each replica
     * 
     * @return
     *      List of indexes to be materialized at each replica
     */
    private DivRecommendedConfiguration getListIndexEachReplica()
    {
        DivRecommendedConfiguration conf = new DivRecommendedConfiguration(this.Nreplicas);
        
        // Iterate over variables s_{i,w}
        try {
            matrix = getMatrix(cplex);
            vars = matrix.getNumVars();
            
            for (int i = 0; i < vars.length; i++) 
            {
                IloNumVar var = vars[i];
                if (cplex.getValue(var) == 1) {
                    DivVariable divVar = genDiv.getVariable(var.getName());
                    if (divVar.getType() == DivVariablePool.VAR_S){
                        Index index = genDiv.getIndexOfVarS(var.getName());
                        // only record the real indexes
                        if (index.getFullyQualifiedName().contains(IndexFullTableScan.FULL_TABLE_SCAN_SUFFIX) == false) {
                            conf.addIndexReplica(divVar.getReplica(), index);
                        }
                    }
                }
            }
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
        
        return conf;
    }
    
    /**
     * Determine the matrix used in the BIP problem
     * 
     * @param cplex
     *      The model of the BIP problem
     * @return
     *      The matrix of @cplex      
     */ 
    private IloLPMatrix getMatrix(IloCplex cplex) throws IloException 
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
