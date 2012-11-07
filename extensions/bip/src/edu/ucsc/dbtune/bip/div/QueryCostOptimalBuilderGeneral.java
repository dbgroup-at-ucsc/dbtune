package edu.ucsc.dbtune.bip.div;

import static edu.ucsc.dbtune.bip.core.InumQueryPlanDesc.BIP_MAX_VALUE;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_U;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;
import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;

public class QueryCostOptimalBuilderGeneral 
{
 // These class are derived from DivBIP
    protected IloCplex        cplex;
    protected List<IloNumVar> cplexVar;
    protected DivVariablePool poolVariables;  
    protected int numConstraints;
    protected boolean isApproximation;
    
    /**
     * Constructor of this class
     * 
     * @param cplex
     *      A set of constraints built by this class will be updated by cplex object      
     * @param cplexVar
     *      List of Cplex variables (not modified by this class)
     * @param poolVariables
     *      List of Div variables 
     */
    public QueryCostOptimalBuilderGeneral(IloCplex cplex, final List<IloNumVar> cplexVar, 
                                       final DivVariablePool poolVariables,
                                       final boolean isApproximation)
    {
        this.cplex = cplex;
        this.cplexVar = cplexVar;
        this.poolVariables = poolVariables;
        
        numConstraints = 0;
        this.isApproximation = isApproximation;
    }
    
    /**
     * Formulate the expression of a query on a particular replica
     * 
     * @param r
     *      The replica ID
     * @param q
     *      The query ID
     * @param desc
     *      The query plan description.
     *      
     * @return
     *      The linear expression of the query
     *      
     * @throws IloException
     */
    public IloLinearNumExpr queryExprOptimal(int r, int q, QueryPlanDesc desc) 
              throws IloException
    {
        int id;
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {                    
            id = poolVariables.get(VAR_Y, r, q, k, 0, 0).getId();
            expr.addTerm(desc.getInternalPlanCost(k), cplexVar.get(id));                    
        }                
                    
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    id = poolVariables.get(VAR_X, r, q, k, i, index.getId()).getId();
                    expr.addTerm(desc.getAccessCost(k, i, index), cplexVar.get(id));
                }    
        
        return expr;
    }
    
    /**
     * Impose the set of constraints to ensure that the query cost is optimal
     * 
     * @param r
     *      The replica
     * @param q
     *      The query ID
     * @param desc
     *      The query description
     * @throws IloException 
     */
    public void optimalConstraints(int r, int q, QueryPlanDesc desc) throws IloException
    {
        IloLinearNumExpr exprOptimal;
        exprOptimal = queryExprOptimal(r, desc.getStatementID(), desc);
        
        // constraint for the local optimal
        localOptimal(r, desc.getStatementID(), desc, exprOptimal);
        //presentVariableLocalOptimal(r, desc.getStatementID(), desc);
        selectingIndexAtEachSlot(r, desc.getStatementID(), desc);
    }
    
    /**
     * This set of constraints ensure {@code cost_opt(q, X} is not greater than the local optimal cost 
     * of using any template plan.
     *   
     * @throws IloException 
     * 
     */
    protected void localOptimal(int r, int q, QueryPlanDesc desc, IloLinearNumExpr exprQuery)
                   throws IloException
    {   
        // local optimal
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) 
            localOptimal(r, q, desc, exprQuery, t);
    }
    
    
    protected void localOptimal(int r, int q, QueryPlanDesc desc, 
                                IloLinearNumExpr exprQuery, int t)
            throws IloException
    {
        IloLinearNumExpr expr;
        IloLinearNumExpr exprAtomic;
        int idU;
        double approxCoef;
        boolean isFTSInSlot;
        
        if (isApproximation)
            approxCoef = 1.1;
        else 
            approxCoef = 1.0;
        
        expr = cplex.linearNumExpr();
        expr.add(exprQuery);    
                    
        for (int i = 0; i < desc.getNumberOfSlots(t); i++) {
            
            isFTSInSlot = false;
            for (Index index : desc.getIndexesAtSlot(t, i)) {
                idU = poolVariables.get(VAR_U, r, q, t, i, index.getId()).getId();
                expr.addTerm(-approxCoef * desc.getAccessCost(t, i, index), cplexVar.get(idU));
                
                if (index instanceof FullTableScanIndex)
                    isFTSInSlot = true;
            }
            
            // atomic
            exprAtomic = cplex.linearNumExpr();
            for (Index index : desc.getIndexesAtSlot(t, i)) {
                idU = poolVariables.get(VAR_U, r, q, t, i, index.getId()).getId();
                exprAtomic.addTerm(1, cplexVar.get(idU));           
            }
            
            // add an infinity for the FTS-U variable
            if (!isFTSInSlot) {
                Index fts = desc.getFTSAtSlot(t, i);
                idU = poolVariables.get(VAR_U, r, q, t, i, fts.getId()).getId();
                expr.addTerm(-approxCoef * BIP_MAX_VALUE, cplexVar.get(idU));
                exprAtomic.addTerm(1, cplexVar.get(idU));   
            }
            
            cplex.addEq(exprAtomic, 1, "atomic_U" + numConstraints);
            numConstraints++;
        }
        
        cplex.addLe(expr, approxCoef * desc.getInternalPlanCost(t), "local_" + numConstraints);
        numConstraints++; 
    }

     
    /**
     * Constraint on the present of {@code VAR_U} variables.
     * 
     * For example, a variable corresponding to some index {@code a} must be {@code 0}
     * if {@code a} is not recommended.
     * 
     * @throws IloException 
     * 
     */
    protected void presentVariableLocalOptimal(int r, int q, QueryPlanDesc desc) 
                  throws IloException
    {   
        IloLinearNumExpr expr;
        int idU;
        int idS;
            
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++)
            for (int i = 0; i < desc.getNumberOfSlots(t); i++) {
                // not constraint FTS variables
                for (Index index : desc.getIndexesWithoutFTSAtSlot(t, i)) {
                    idU = poolVariables.get(VAR_U, r, q, t, i, index.getId()).getId();
                    idS = poolVariables.get(VAR_S, r, 0, 0, 0, index.getId()).getId();
                    expr = cplex.linearNumExpr();
                    expr.addTerm(1, cplexVar.get(idU));
                    expr.addTerm(-1, cplexVar.get(idS));            
                    cplex.addLe(expr, 0, "U_present_" + numConstraints);
                    numConstraints++;
                                        
                }         
            }
    }
    
    /**
     * 
     * The constraints ensure the index with the small index access cost is used
     * to compute {@code cost(q, r)} 
     * 
     * @throws IloException 
     * 
     */
    protected void selectingIndexAtEachSlot(int r, int q, QueryPlanDesc desc) throws IloException
    {   
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++)
            for (int i = 0; i < desc.getNumberOfSlots(t); i++) 
                selectingIndexAtEachSlot(r, q, t, i, desc);
    }
    
    /**
     * todo
     * @param r
     * @param q
     * @param t
     * @param i
     * @param desc
     * @throws IloException
     */
    protected void selectingIndexAtEachSlot(int r, int q, int t, int i, QueryPlanDesc desc) 
                   throws IloException
    {
        int idU;
        int idS;        
        int idFTS, numIndex;
        Index fts;
        List<Integer> varIDs;
        
        // Sort index access cost
        List<SortableIndexAcessCost> listSortedIndex  = new ArrayList<SortableIndexAcessCost>();
                
        for (Index index : desc.getIndexesAtSlot(t, i)) {
            SortableIndexAcessCost sac = new SortableIndexAcessCost 
                                            (desc.getAccessCost(t, i, index), index);
            listSortedIndex.add(sac);                       
        }                   
            
        numIndex = desc.getIndexesAtSlot(t, i).size();
        if (numIndex == 0) {
            throw new RuntimeException("Slot at #query = " + q + " #plan Id =  " + t
                        + " # slot = " + i + " does not have any indexes"
                        + " that can fit");
        }
                
        fts = desc.getIndexesAtSlot(t, i).get(numIndex - 1);
        if (fts instanceof FullTableScanIndex)
            idFTS = fts.getId();
        else
            idFTS = -1;
                    
        // sort in the increasing order of the index access cost
        Collections.sort(listSortedIndex);
        varIDs = new ArrayList<Integer>();
        
        for (SortableIndexAcessCost sac : listSortedIndex) {  
            
            Index index = sac.getIndex();
            idU = poolVariables.get(VAR_U, r, q, t, i, index.getId()).getId();
            varIDs.add(idU);
                    
            if (index.getId() == idFTS) {
                IloLinearNumExpr exprInternal = cplex.linearNumExpr();
                for (int varID : varIDs)
                    exprInternal.addTerm(1, cplexVar.get(varID));
                
                cplex.addEq(exprInternal, 1, "FTS_" + numConstraints);
                numConstraints++;
                break; // the remaining variables will be assigned value 0 
            } else {                                    
                IloLinearNumExpr exprInternal = cplex.linearNumExpr();
                for (int varID : varIDs)
                    exprInternal.addTerm(1, cplexVar.get(varID));
                    
                idS = poolVariables.get(VAR_S, r, 0, 0, 0, index.getId()).getId();
                exprInternal.addTerm(-1, cplexVar.get(idS));
                cplex.addGe(exprInternal, 0, "select_index_" + numConstraints); 
                numConstraints++;
            }   
        }   
    }
}
