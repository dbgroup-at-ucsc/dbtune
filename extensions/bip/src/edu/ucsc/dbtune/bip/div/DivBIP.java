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

import edu.ucsc.dbtune.bip.util.MatIndexPool;
import edu.ucsc.dbtune.bip.util.MultiQueryPlanDesc;
import edu.ucsc.dbtune.bip.util.BIPAgentPerSchema;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.MatIndex;
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
    public List<List<MatIndex>> optimalDiv(List<WorkloadPerSchema> listWorkload, List<BIPAgentPerSchema> listAgent,
                                        List<Index> candidateIndexes, int Nreplicas, int loadfactor, double B) 
                                        throws SQLException
    {   
        this.Nreplicas = Nreplicas;
        this.loadfactor = loadfactor;
        
        // Put all indexes in {@code candidateIndexes} into the pool of {@code MatIndexPool}
        for (Index idx: candidateIndexes) {
            int id = MatIndexPool.addMatIndex(idx, MatIndex.INDEX_TYPE_CREATE);          
            MatIndexPool.mapIndexToGlobalId(idx, id);
        }
        MatIndexPool.setNumCandidateIndexes(candidateIndexes.size());
        
        // TODO: change this for loop after {@code listAgent} is removed
        for (BIPAgentPerSchema agent : listAgent) {
            for (Index scanIdx : agent.getListFullTableScanIndexes()) {
                int id = MatIndexPool.addMatIndex(scanIdx, MatIndex.INDEX_TYPE_CREATE);          
                MatIndexPool.mapIndexToGlobalId(scanIdx, id);
            }
        }
        
        
        int iAgent = 0;
        List<MultiQueryPlanDesc> listQueryPlans = new ArrayList<MultiQueryPlanDesc>();
        for (WorkloadPerSchema wl : listWorkload) {
            //BIPAgentPerSchema agent = new BIPAgentPerSchema(wl.getSchema());
            //TODO: Change this line when the implementation of BIPAgentPerSchema is done
            BIPAgentPerSchema agent = listAgent.get(iAgent++);
            
            for (Iterator<SQLStatement> iterStmt = wl.getWorkload().iterator(); iterStmt.hasNext(); ) {
                MultiQueryPlanDesc desc = new MultiQueryPlanDesc(); 
                desc.generateQueryPlanDesc(agent, iterStmt.next(), candidateIndexes);
                listQueryPlans.add(desc);
            }
        }
        
        // 5. Formulate BIP and run the BIP to derive the set of indexes materialized 
        // at each replica
        return buildOptimalDivergentIndex(listQueryPlans, this.Nreplicas, this.loadfactor, B);
    }
    
    /**
     * Convert the list of indexes at each replica into a string
     * @param listIndexReplicas
     *      A recommended configuration 
     * @return
     *      A string representing the recommended configuration in text
     */
    public String printRecommendedConfiguration(List<List<MatIndex>> listIndexReplicas) 
    {
        StringBuffer result = new StringBuffer(); 
        int iReplica = 0;
        for (List<MatIndex> indexEachReplica : listIndexReplicas) {
            result.append("----The " + iReplica +"-th replica -----------\n");
            for (MatIndex idx : indexEachReplica) {
                result.append("----- Index: " + idx.getIndex().getFullyQualifiedName() + " Size: " + idx.getMatSize() + "\n");
            }
            iReplica++;
        }
        return result.toString();
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
    private List<List<MatIndex>> buildOptimalDivergentIndex(List<MultiQueryPlanDesc> listQueryPlans, int Nreplicas, int loadfactor, double B)  
    {   
        LogListener listener = new LogListener() {
            public void onLogEvent(String component, String logEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
        List<List<MatIndex>> listIndexReplicas = new ArrayList<List<MatIndex>>();
        String cplexFile = "", binFile = "", consFile = "", objFile = "";   
        String workloadName = environment.getTempDir() + "/testwl";
        
        try {                                                       
            genDiv = new DivLinGenerator(workloadName, listQueryPlans, Nreplicas, loadfactor, B);
            
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
                listIndexReplicas = getListIndexEachReplica();
                System.out.println(" In CPlex, objective function value: " + cplex.getObjValue());
            } 
            else {
                System.out.println(" INFEASIBLE solution ");
            }
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
         }
        
        return listIndexReplicas;        
    }
    
    /**
     * The method reads the value of variables corresponding to the presence of indexes
     * at each replica and returns this list of indexes to materialize at each replica
     * 
     * @return
     *      List of indexes to be materialized at each replica
     */
    private List<List<MatIndex>> getListIndexEachReplica()
    {
        List<List<MatIndex>> listIndexReplicas = new ArrayList<List<MatIndex>>();
        for (int i = 0; i < Nreplicas; i++) {
            List<MatIndex> listIndexEachReplica = new ArrayList<MatIndex>();
            listIndexReplicas.add(listIndexEachReplica);
        }
        
        // Iterate over variables s_{i,w}
        try {
            matrix = getMatrix(cplex);
            vars = matrix.getNumVars();
            
            for (int i = 0; i < vars.length; i++) 
            {
                IloNumVar var = vars[i];
                if (cplex.getValue(var) == 1) {
                    MatIndex matIdx = DivLinGenerator.deriveMatIndex(var.getName());
                    
                    if (matIdx != null) {
                        // DO NOT consider full table scan indexes
                        if (matIdx.getId() < MatIndexPool.getNumCandidateIndexes()) {
                            listIndexReplicas.get(matIdx.getReplicaID()).add(matIdx);
                        }
                    }
                }
            }
            
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
        
        
        return listIndexReplicas;
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
