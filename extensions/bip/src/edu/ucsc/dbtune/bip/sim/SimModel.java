package edu.ucsc.dbtune.bip.sim;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.BIPVariable;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.metadata.Index;


public class SimModel extends AbstractBIPSolver implements ScheduleBIPSolver
{
    /** Variables used in constructing BIP */    
    private SimVariablePool poolVariables;
    
    /** Map variable of type CREATE or DROP to the indexes */
    private Map<String,Index> mapVarCreateDropToIndex;
    
    /** Variables for the constraints on windows*/
    protected int     W;    
    protected int     maxNumberIndexesWindow;
    protected double  maxCreationCostWindow;
    protected boolean isConstraintNumberIndexesWindow;
    protected boolean isConstraintCreationCostWindow;
    
    /** Set of indexes that are created, dropped, and not touched */
    protected Set<Index> Sinit;
    protected Set<Index> Screate;
    protected Set<Index> Sdrop;
    protected Set<Index> Sremain;
    
    
    @Override
    public void setConfigurations(Set<Index> Sinit, Set<Index> Smat) 
    {
        this.Sinit = Sinit;
        /**
         * Classify the indexes into one of the three types: 
         * INDEX_TYPE_CREATE, INDEX_TYPE_DROP, INDEX_TYPE_REMAIN
         */
        candidateIndexes = new HashSet<Index> (Smat);
        candidateIndexes.addAll(Sinit);
        
        // Screate = Smat - Sinit
        Screate = new HashSet<Index>(Smat);
        Screate.removeAll(Sinit);
        // Sdrop = Sinit - Smat
        Sdrop = new HashSet<Index>(Sinit);
        Sdrop.removeAll(Smat);
        // Sremain = Sin \cap Smat
        Sremain = new HashSet<Index>(Sinit); 
        Sremain.retainAll(Smat);            
    }
    

    @Override
    public void setNumberWindows(int W) 
    {
        this.W  = W;
    }

    @Override
    public void setNumberofIndexesEachWindow(int n) 
    {
        maxNumberIndexesWindow = n; 
        isConstraintNumberIndexesWindow = true;
    }

    @Override
    public void setCreationCostWindow(int C) 
    {
        this.maxCreationCostWindow = C;
        isConstraintCreationCostWindow = true;
    }
    
    
    public SimModel()
    {   
        isConstraintNumberIndexesWindow = false;
        isConstraintCreationCostWindow = false; 
    }
    
    /**
     * Since the set of indexes are derived from the initial configuration ({@code Sinit}) and
     * the configuration to be materialized ({@code Smat}), this method is left to be empty. 
     */
    @Override
    public void setCandidateIndexes(Set<Index> candidateIndexes)
    {   
    }
    
    @Override
    protected final void buildBIP() 
    {   
        super.numConstraints = 0;
               
        try {            
            // 1. Add variables into {@code poolVariables}
            constructVariables();

            // 2. Index present    
            indexPresentConstraints();
            
            // 3. Construct the query cost at each window time
            queryCostWindow();
            
            // 4. Atomic constraints
            atomicConstraints();        
            usedIndexAtWindow();
            
            // 5. Space constraints
            if (isConstraintCreationCostWindow)
                timeConstraints();
            
            if (isConstraintNumberIndexesWindow)
                numberIndexesAtEachWindowConstraint();
            
        } catch (IloException e) {
            e.printStackTrace();
        }
    }
    
    
    @Override
    protected IndexTuningOutput getOutput()
    { 
        MaterializationSchedule schedule = new MaterializationSchedule(W, Sinit);

        for (int i = 0; i < poolVariables.variables().size(); i++) {
            
            if (valVar[i] == 1) {
                SimVariable simVar = (SimVariable) poolVariables.variables().get(i);
                
                if (simVar.getType() == SimVariablePool.VAR_CREATE 
                        || simVar.getType() == SimVariablePool.VAR_DROP) {
                    Index index = mapVarCreateDropToIndex.get(simVar.getName());
                    schedule.addIndexWindow(index, simVar.getWindow(), simVar.getType());
                }
            }
            
        }   
        
        return schedule;
    }
    
    /**
     * Construct and add all variables used by the BIP 
     * into the pool of variables
     * @throws IloException 
     *  
     */
    private void constructVariables() throws IloException
    {  
        poolVariables = new SimVariablePool();
        mapVarCreateDropToIndex = new HashMap<String,Index>();
        
        // for TYPE_CREATE index 
        for (Index index : Screate) {
            
            for (int w = 0; w < W; w++) {
                SimVariable var = poolVariables.createAndStore(SimVariablePool.VAR_CREATE, w, 
                                                               0, 0, index.getId());
                mapVarCreateDropToIndex.put(var.getName(), index);
            }          
        }
        
        // for TYPE_DROP index
        for (Index index : Sdrop) {
            
            for (int w = 0; w < W; w++) {
                SimVariable var = poolVariables.createAndStore(SimVariablePool.VAR_DROP, w, 
                                                               0, 0, index.getId());
                mapVarCreateDropToIndex.put(var.getName(), index);
            }          
        }
        
        // for TYPE_PRESENT
        for (Index index : candidateIndexes) {            
            for (int w = 0; w < W; w++)
                poolVariables.createAndStore(SimVariablePool.VAR_PRESENT, w, 0, 0, index.getId());                      
        }
        
        // for TYPE_Y, TYPE_X
        for (QueryPlanDesc desc : listQueryPlanDescs){
            
            int q = desc.getStatementID();
            for (int w = 0; w < W; w++) {
                
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) 
                    poolVariables.createAndStore(SimVariablePool.VAR_Y, w, q, k, 0);
                    
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {    
                    
                    for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                        
                        for (Index index : desc.getListIndexesAtSlot(i)) 
                            poolVariables.createAndStore(SimVariablePool.VAR_X, w, q, k, 
                                                         index.getId());
                        
                    }
                }       
            }
        }
        
        
        createCplexVariable();
    }
     
    /**
     * Create corresponding variables in CPLEX model.
     * 
     * @throws IloException 
     * 
     */
    private void createCplexVariable() throws IloException
    {
        List<BIPVariable> vars;
        IloNumVarType[]   type;
        double[]          lb;
        double[]          ub;
        int               size;
        
        vars = poolVariables.variables();
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
     * A well-behaved schedule satisfies the following three conditions:
     * <p>
     * <ol>
     *  <li> Index I in {@code Screate} is created one time. In addition, I is present at window w 
     *  if and only I has been created at some window point between 0 and w </li>
     *  <li> Index I in {@code Sdrop} is dropped one time. Furthermore, I is present at window w 
     *  if and only I has not been dropped at all windows between 0 and w </li>
     *  <li> Indexes in {@code Sremain} remain in the DBMS at all windows</li>
     * </ol>
     * </p>
     * @throws IloException 
     *  
     */
    private void indexPresentConstraints() throws IloException
    {   
        int id;
        int idPresent;
        
        IloLinearNumExpr expr;
        
        // for TYPE_CREATE index
        for (Index index : Screate) {
            
            expr = cplex.linearNumExpr();            
            
            for (int w = 0; w < W; w++) {                
                id = poolVariables.get(SimVariablePool.VAR_CREATE, w, 
                                       0, 0, index.getId()).getId();
                expr.addTerm(1, cplexVar.get(id));
            }
            
            cplex.addEq(expr, 1, "create_constraint_" + numConstraints); 
            numConstraints++; 
            
            for (int w = 0; w < W; w++) {
                expr = cplex.linearNumExpr(); 
                idPresent = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 
                                              0, 0, index.getId()).getId();
                expr.addTerm(-1, cplexVar.get(idPresent));
                
                for (int j = 0; j <= w; j++) {
                    
                    id = poolVariables.get(SimVariablePool.VAR_CREATE, j, 
                                           0, 0, index.getId()).getId();
                    expr.addTerm(1, cplexVar.get(id));
                }
                
                cplex.addEq(expr, 0, "present_constraint_" + numConstraints);
                numConstraints++;
            }
        }
        
        // for TYPE_DROP index
        for (Index index : Sdrop) {
            expr = cplex.linearNumExpr(); 
            
            for (int w = 0; w < W; w++) {
                id = poolVariables.get(SimVariablePool.VAR_DROP, w, 0, 
                                       0, index.getId()).getId();
                expr.addTerm(1, cplexVar.get(id));
            }
            cplex.addEq(expr, 1, "drop_constraint_" + numConstraints); 
            numConstraints++;
            
            for (int w = 0; w < W; w++) {
                expr = cplex.linearNumExpr(); 
                idPresent = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 
                                              0, 0, index.getId()).getId();
                expr.addTerm(1, cplexVar.get(idPresent));
                
                for (int j = 0; j <= w; j++) {
                    id = poolVariables.get(SimVariablePool.VAR_DROP, j, 
                                            0, 0, index.getId()).getId();
                    expr.addTerm(1, cplexVar.get(id));
                }
                cplex.addEq(expr, 1, "present_constraint_" + numConstraints);
                numConstraints++;  
            }
        }
        
        // for TYPE_PRESENT index
        for (Index index : Sremain) {
            
            for (int w = 0; w < W; w++) {
                
                expr      = cplex.linearNumExpr();
                idPresent = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 
                                         0, 0, index.getId()).getId();
                expr.addTerm(1, cplexVar.get(idPresent));
                cplex.addEq(expr, 1, "present_constraint_" + numConstraints);
                numConstraints++;
            }          
        }
    }
    
    /**
     * Build the execution cost of each query at each window
     * 
     * {@code cost(q, w) = \sum_{k \in [1, Kq]} \beta_{qk} y(w,q,k) }
     * {@code            +  \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} }
     * {@code                x(w,q,k,i,a) \gamma_{q,k,i,a} }     
     * 
     * @throws IloException 
     *
     */
    private void queryCostWindow() throws IloException
    {    
        IloLinearNumExpr expr = cplex.linearNumExpr(); 
        int id;
        
        for (QueryPlanDesc desc : listQueryPlanDescs){
            
            int q = desc.getStatementID();
            for (int w = 0; w < W; w++) {
                
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    
                    id = poolVariables.get(SimVariablePool.VAR_Y, w, q, k, 0).getId();
                    expr.addTerm(desc.getInternalPlanCost(k), cplexVar.get(id));        
                }
                          
                // Index access cost                            
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) { 
                    
                    for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                        
                        for (Index index : desc.getListIndexesAtSlot(i)) {
                            
                            id = poolVariables.get(SimVariablePool.VAR_X, w, 
                                                   q, k, index.getId()).getId();
                            expr.addTerm(desc.getAccessCost(k, index), cplexVar.get(id));        
                        }
                    }
                }
            }
        }
        
        cplex.addMinimize(expr);
    }
    
    /**
     * Build a set of atomic constraints on variables {@code VAR_X} and {@code VAR_Y}
     * that are common for methods using INUM.
     * 
     * For example, the summation of all variables of type {@code VAR_Y} of a same {@code theta}
     * must be {@code 1} in order for the optimal execution cost corresponds to only one 
     * template plan.  
     * 
     * @throws IloException 
     * 
     */
    private void atomicConstraints() throws IloException
    {   
        IloLinearNumExpr expr;
        int id;
        int idY;
        
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getStatementID();
        
            for (int w = 0; w < W; w++) {
                
                expr = cplex.linearNumExpr(); 
        
                // (1) \sum_{k \in [1, Kq]}y^{theta}_k = 1
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    id = poolVariables.get(SimVariablePool.VAR_Y, w, q, k, 0).getId();
                    expr.addTerm(1, cplexVar.get(id));
                }
                
                cplex.addEq(expr, 1, "atomic_constraint_" + numConstraints);
                numConstraints++;
            
                // (2) \sum_{a \in S_i} x(w, k, i, a) = y(w, k)
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    
                    idY = poolVariables.get(SimVariablePool.VAR_Y, w, q, k, 0).getId();
                    
                    for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                        
                        expr = cplex.linearNumExpr(); 
                        expr.addTerm(-1, cplexVar.get(idY));
                        
                        for (Index index : desc.getListIndexesAtSlot(i)) {
                            
                            id = poolVariables.get(SimVariablePool.VAR_X, w, 
                                                  q, k, index.getId()).getId();
                            expr.addTerm(1, cplexVar.get(id));
                        }
                        
                        cplex.addEq(expr, 0, "atomic_constraint_" + numConstraints);
                        numConstraints++;
                    }
                }       
            }
        }
    }
    
    /**
     * An index {@code a} is used to compute {@code cost(q, w)} if and only if {@code present_a = 1}
     * @throws IloException 
     */
    private  void usedIndexAtWindow() throws IloException
    {
        IloLinearNumExpr expr;
        int id;
        int idPresent;
        
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getStatementID();
        
            for (int w = 0; w < W; w++) {
                
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    
                    for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                        
                        //for (Index index : desc.getListIndexesAtSlot(i)) {
                        // NOT constraint for FTS
                        for (Index index : desc.getListIndexesWithoutFTSAtSlot(i)) {
                            
                            expr = cplex.linearNumExpr(); 
                            id = poolVariables.get(SimVariablePool.VAR_X, w, 
                                                   q, k, index.getId()).getId();
                            
                            idPresent = poolVariables.get(SimVariablePool.VAR_PRESENT, 
                                                          w, 0, 0, index.getId()).getId();
                            expr.addTerm(1, cplexVar.get(id));
                            expr.addTerm(-1, cplexVar.get(idPresent));
                            
                            cplex.addLe(expr, 0, "index_used_" + numConstraints);
                            numConstraints++;
                        }
                    }
                }       
            }
        }
    }
    /**
     * Time constraint on the materialized indexes at all maintenance window
     * 
     */
    private void timeConstraints()
    {
        /*
        for (int w = 0; w < W; w++) {
            
            List<String> linList = new ArrayList<String>();
            
            for (Index index : Screate){
                String var_create = poolVariables.get(SimVariablePool.VAR_CREATE, w, 
                                                      0, 0, index.getId()).getName();               
                linList.add(Double.toString(index.getCreationCost()) + var_create);
            }
            
            buf.getCons().add("creation_cost_constraint" + numConstraints  
                    + " : " + Strings.concatenate(" + ", linList)                   
                    + " <= " + maxCreationCostWindow);
            numConstraints++;               
        }
        */
    }
    
    private void numberIndexesAtEachWindowConstraint() throws IloException
    {
        IloLinearNumExpr expr;
        int id;
        
        for (int w = 0; w < W; w++) {
            
            expr = cplex.linearNumExpr(); 
            for (Index index : Screate){
                id = poolVariables.get(SimVariablePool.VAR_CREATE, w, 
                                       0, 0, index.getId()).getId();
                expr.addTerm(1, cplexVar.get(id));
            }
            
            cplex.addLe(expr, maxNumberIndexesWindow, "window_" + numConstraints);
            numConstraints++;
        }
    }   
}
