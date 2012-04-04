package edu.ucsc.dbtune.bip.div;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_U;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_XO;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_YO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.interactions.SortableIndexAcessCost;
import edu.ucsc.dbtune.metadata.Index;

/**
 * This class builds a formula for a given query on a particular replica,
 * and ensures that this formula corresponds to the optimal query execution 
 * cost of q on this replica.
 * 
 * @author Quoc Trung Tran
 *
 */
public class QueryCostOptimalBuilder 
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
    public QueryCostOptimalBuilder(IloCplex cplex, final List<IloNumVar> cplexVar, 
                                   final DivVariablePool poolVariables)
    {
        this.cplex = cplex;
        this.cplexVar = cplexVar;
        this.poolVariables = poolVariables;
        
        numConstraints = 0;
        isApproximation = true;
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
            id = poolVariables.get(VAR_YO, r, q, k, 0).getId();
            expr.addTerm(desc.getInternalPlanCost(k), cplexVar.get(id));                    
        }                
                    
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)
                for (Index index : desc.getIndexesAtSlot(i)) {
                    id = poolVariables.get(VAR_XO, r, q, k, index.getId()).getId();
                    expr.addTerm(desc.getAccessCost(k, index), cplexVar.get(id));
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
        presentVariableLocalOptimal(r, desc.getStatementID(), desc);
        selectingIndexAtEachSlot(r, desc.getStatementID(), desc);
        
        // atomic constraints for local optimal
        internalAtomicLocalOptimalConstraint(r, desc.getStatementID(), desc);
        slotAtomicLocalOptimalConstraints(r, desc.getStatementID(), desc);
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
        IloLinearNumExpr expr;        
        int idU;
        double approxCoef;        
        
        if (isApproximation)
            approxCoef = 1.1;
        else 
            approxCoef = 1.0;
        
        // local optimal
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
            
            expr = cplex.linearNumExpr();
            expr.add(exprQuery);    
                        
            for (int i = 0; i < desc.getNumberOfSlots(); i++) 
                for (Index index : desc.getIndexesAtSlot(i)) {
                    idU = poolVariables.get(VAR_U, r, q, t, index.getId()).getId();
                    expr.addTerm(-approxCoef * desc.getAccessCost(t, index), cplexVar.get(idU));
                }
            
            cplex.addLe(expr, approxCoef * desc.getInternalPlanCost(t), "local_" + numConstraints);
            numConstraints++;
        }
        
        // atomic constraint
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                expr = cplex.linearNumExpr();
                for (Index index : desc.getIndexesAtSlot(i)) {
                    idU = poolVariables.get(VAR_U, r, q, t, index.getId()).getId();
                    expr.addTerm(1, cplexVar.get(idU));           
                }
                
                cplex.addEq(expr, 1, "atomic_U" + numConstraints);
                numConstraints++;
            }
        
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
            for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                // not constraint FTS variables
                for (Index index : desc.getIndexesWithoutFTSAtSlot(i)) {
                    idU = poolVariables.get(VAR_U, r, q, t, index.getId()).getId();
                    idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
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
            for (int i = 0; i < desc.getNumberOfSlots(); i++) 
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
     
        // Sort index access cost
        List<SortableIndexAcessCost> listSortedIndex  = new ArrayList<SortableIndexAcessCost>();
                
        for (Index index : desc.getIndexesAtSlot(i)) {
            SortableIndexAcessCost sac = new SortableIndexAcessCost 
                                            (desc.getAccessCost(t, index), index);
            listSortedIndex.add(sac);                       
        }                   
            
        numIndex = desc.getIndexesAtSlot(i).size();
        idFTS = desc.getIndexesAtSlot(i).get(numIndex - 1).getId();
            
        // sort in the increasing order of the index access cost
        Collections.sort(listSortedIndex);
                               
        List<Integer> varIDs = new ArrayList<Integer>();
        
        for (SortableIndexAcessCost sac : listSortedIndex) {  
            
            Index index = sac.getIndex();
            idU = poolVariables.get(VAR_U, r, q, t, index.getId()).getId();
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
                    
                idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                exprInternal.addTerm(-1, cplexVar.get(idS));
                cplex.addGe(exprInternal, 0, "select_index_" + numConstraints); 
                numConstraints++;
            }   
        }   
    }
    
    
        
    /**
     * Internal atomic constraints: only one template plan is chosen to compute {@code cost(q,r)}.
     *  
     * @param r
     *     Replica ID
     * @param q
     *     Statement ID
     * @param desc
     *     The query plan description        
     *  
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX.
     */
    protected void internalAtomicLocalOptimalConstraint(int r, int q, QueryPlanDesc desc) 
              throws IloException
    {
        IloLinearNumExpr expr;
        int idY;
       
        // \sum_{k \in [1, Kq]}y^{r}_{qk} = 1
        expr = cplex.linearNumExpr();
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idY = poolVariables.get(VAR_YO, r, q, k, 0).getId();
            expr.addTerm(1, cplexVar.get(idY));
        }
        
        cplex.addEq(expr, 1, "atomic_internal_local_optimal_" + numConstraints);
        numConstraints++;
    }
    
    /**
     * Slot atomic constraints:
     *  At most one index is selected to plug into a slot. 
     *  An index a is recommended if it is used to compute at least one cost(q,r)
     *  
     * @param r
     *     Replica ID
     * @param q
     *     Statement ID
     * @param desc
     *     The query plan description        
     *  
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX.
     */
    protected void slotAtomicLocalOptimalConstraints(int r, int q, QueryPlanDesc desc) 
                    throws IloException
    {
        IloLinearNumExpr expr;
        int idY;
        int idX;
        int idS;
        
        // \sum_{a \in S_i} x(r, q, k, i, a) = y(r, q, k)
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            
            idY = poolVariables.get(DivVariablePool.VAR_YO, r, q, k, 0).getId();
            
            for (int i = 0; i < desc.getNumberOfSlots(); i++) {            
            
                expr = cplex.linearNumExpr();
                expr.addTerm(-1, cplexVar.get(idY));
                
                for (Index index : desc.getIndexesAtSlot(i)) { 
                    idX = poolVariables.get(VAR_XO, r, q, k, index.getId()).getId();                            
                    expr.addTerm(1, cplexVar.get(idX));
                }
                
                cplex.addEq(expr, 0, "atomic_constraint_local_optimal" + numConstraints);
                numConstraints++;
                
            }
        }
        
        // used index
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)   
                for (Index index : desc.getIndexesAtSlot(i)) {
                    
                    idX = poolVariables.get(VAR_XO, r, q, k, index.getId()).getId();
                    idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                    
                    expr = cplex.linearNumExpr();
                    expr.addTerm(1, cplexVar.get(idX));
                    expr.addTerm(-1, cplexVar.get(idS));
                    cplex.addLe(expr, 0, "index_present_local_optimal" + numConstraints);
                    numConstraints++;
                    
                }
    }
}
