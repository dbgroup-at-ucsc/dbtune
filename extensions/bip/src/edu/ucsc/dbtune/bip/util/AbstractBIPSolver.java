package edu.ucsc.dbtune.bip.util;

import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;


public abstract class AbstractBIPSolver implements BIPSolver 
{
    protected List<Index> candidateIndexes;
    protected CPlexBuffer buf;
    protected List<QueryPlanDesc> listQueryPlanDescs;
    protected Map<Schema, Workload> mapSchemaToWorkload;
    protected List<BIPPreparatorSchema> listPreparators;
    protected IloLPMatrix matrix;
    protected IloNumVar [] vars;
    protected IloCplex cplex;
    protected String workloadName;
    protected Environment environment = Environment.getInstance();
    protected int numConstraints;
    
    @Override
    public abstract BIPOutput solve() throws SQLException;

    @Override
    public void setWorkloadName(String workloadName)
    {
        this.workloadName = workloadName;
        String name = environment.getTempDir() + workloadName;
        try {
            this.buf = new CPlexBuffer(name);
        }
        catch (IOException e) {
            System.out.println(" Error in opening files " + e.toString());          
        }
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
     * Retrieve the corresponding variable given its name
     * @param name
     *     The name to be retrieved the corresponding variable        
     * @return
     *     A SimVariable or a NULL 
     */
    protected abstract BIPVariable getVariable(String name); 
    
    /**
     * Add indexes from the candidate indexes into the pool manipulated by the BIP
     * 
     */
    protected abstract void insertIndexesToPool();
    
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
    
    /**
     * Communicate with INUM to populate the query plan description (internal plan cost, index access costs, etc.)
     * for each statement in the given workload
     * 
     * @param poolIndexes
     *      The pool of indexes managed by the BIP
     *      
     * @throws SQLException
     */
    protected void populatePlanDescriptionForStatement(BIPIndexPool poolIndexes) throws SQLException
    {   
        int iPreparator = 0;
        listQueryPlanDescs = new ArrayList<QueryPlanDesc>();
        
        for (Entry<Schema, Workload> entry : mapSchemaToWorkload.entrySet()) {
            //BIPPreparatorSchema agent = new BIPPreparatorSchema(wl.getSchema());
            // TODO: Change this line when the implementation of BIPAgentPerSchema is done
            BIPPreparatorSchema preparator = listPreparators.get(iPreparator++);
            
            for (Iterator<SQLStatement> iterStmt = entry.getValue().iterator(); iterStmt.hasNext(); ) {
                QueryPlanDesc desc =  new QueryPlanDesc();                    
                // Populate the INUM space for each statement
                // We do not add full table scans before populate from @desc
                // so that the full table scan is placed at the end of each slot 
                desc.generateQueryPlanDesc(preparator, iterStmt.next(), poolIndexes);
                listQueryPlanDescs.add(desc);
            }
            
            // Add full table scans into the pool
            for (Index index : preparator.getListFullTableScanIndexes()) {
                poolIndexes.addIndex(index);
            }
        }
        // Map index in each slot to its pool ID
        for (QueryPlanDesc desc : listQueryPlanDescs) {
            desc.mapIndexInSlotToPoolID(poolIndexes);
        }
    }
}
