package edu.ucsc.dbtune.bip.interactions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import edu.ucsc.dbtune.bip.core.BIPVariable;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.div.SortableIndexAcessCost;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.cplex.IloCplex;
import ilog.cplex.IloCplex.UnknownObjectException;

import static edu.ucsc.dbtune.bip.core.InumQueryPlanDesc.BIP_MAX_VALUE;
import static edu.ucsc.dbtune.bip.interactions.IIPVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.interactions.IIPVariablePool.VAR_Y;
import static edu.ucsc.dbtune.bip.interactions.IIPVariablePool.VAR_S;
import static edu.ucsc.dbtune.bip.interactions.IIPVariablePool.VAR_U;


public class RestrictModel 
{   
    public static final int IND_EMPTY = 0;
    public static final int IND_C     = 1;
    public static final int IND_D     = 2;
    public static final int IND_CD    = 3;
    
    protected final List<Integer> listTheta = 
                    Arrays.asList(IND_EMPTY, IND_C, IND_D, IND_CD);     
    
    protected IIPVariablePool    poolVariables;    
    protected Map<String, Index> mapVarSIndex;   
    protected QueryPlanDesc      desc;
    protected List<Index>        candidates;
    
    protected double delta;  
    protected Index  indexC;
    protected Index  indexD;
    protected int    numConstraints;
    protected double doiOptimizer;
    protected double doiBIP;
      
    
    protected IloLinearNumExpr exprIteraction1;
    
    protected IloCplex        cplex;
    protected List<IloNumVar> cplexVar;
    
    protected LogListener logger;    
    protected boolean     isApproximation;
    protected Map<Integer, Map<Integer, Double>>    mapThetaVarCoef;
    
    /**
     * The constructor to formulate an instance of {@code RetrictIIP}
     *  
     * @param desc
     *     The query plan description (internal plan cost, index access costs, ...)
     *     on which we are going to determine the interaction.
     * @param logger
     *     The logger to record the running time              
     * @param delta
     *     The delta to determine the interaction
     * @param indexc
     *     The first index
     * @param indexd
     *      The second index
     * @param ic
     *     The relation slot ID that contains the first index 
     * @param id
     *     The relation slot ID that contains the second index    
     * 
     */
    public RestrictModel(IloCplex cplex,
                         final QueryPlanDesc desc, 
                         final LogListener logger, 
                         final double delta, final Index indexc, final Index indexd, 
                         final List<Index> candidateIndexes,
                         final boolean isApproximation)
    {
        this.delta            = delta;
        this.indexC           = indexc;
        this.indexD           = indexd;
        this.desc             = desc;
        this.logger           = logger;
        this.candidates = candidateIndexes;
        this.cplexVar         = null;
        this.cplex            = cplex;
        this.isApproximation  = isApproximation;
    }
    
    /**
     * Check if the given pair of indexes in the constructor interact or not by constructing
     * Binary Integer Program
     * 
     * @param sql
     *     The SQL statement
     * @param optimizer
     *     The conventional optimizer (to compute doi from the conventional optimizer)
     * @return
     *     {@code true} if the given pair of indexes interact,
     *     {@code false} otherwise
     * @throws IloException 
     */
    public boolean solve(SQLStatement sql, Optimizer optimizer) throws IloException
    {
        // 1. clear the model of CPLEX object
        cplex.clearModel();
        
        // 2. Construct BIP
        logger.setStartTimer();    
        
        numConstraints = 0;
        // Construct variable
        constructBinaryVariables();
        
        // Construct the formula of Ctheta
        mapThetaVarCoef = new HashMap<Integer, Map<Integer,Double>>(); 
        
        for (int theta : listTheta)
            buildQueryExecutionCost(theta);
        
        
        // 2.1. Atomic constraints 
        for (int theta : listTheta)
            atomicConstraintForINUM(theta);
        
        interactionPrecondition();        
        
        // 2,2. Optimal constraints
        for (int theta : listTheta)
            localOptimal(theta);
        
        for (int theta : listTheta)
            atomicConstraintLocalOptimal(theta);
        
        for (int theta : listTheta)
            selectingIndexAtEachSlot(theta);
        
        logger.onLogEvent(LogListener.EVENT_FORMULATING_BIP);
        
        // 3. Solve the first BIP        
        logger.setStartTimer();        
        boolean isSolveAlternativeOnly = false;
         
        // 
        // One optimization.
        //
        // If indexes belong the same relation then the first index interaction constraint
        // is false, since Aempty + Acd >= Ac + Ad. 
        // 
        // We invoke the alternative interaction constraint instead.
        //
        boolean isInteracting = false;
        
        if (indexC.getTable() != indexD.getTable()) {
            indexInteractionConstraint1();
            isInteracting = cplex.solve();            
        }
        else
            isSolveAlternativeOnly = true;
        
        if (!isInteracting) {
            solveAlternativeBIP(isSolveAlternativeOnly);
            isInteracting = cplex.solve();
        } 
        
        
        logger.onLogEvent(LogListener.EVENT_SOLVING_BIP);
        
        return isInteracting;
    }
    
    /**
     * Retrieve the degree of interaction computed by BIP
     * 
     * @return
     *     The doi value
     */
    public double getDoiBIP()
    {
        return doiBIP;
    }
    
    /**
     * Retrieve the degree of interaction computed by using actual optimizer
     * (e.g., DB2Optimizer).
     * 
     * @return
     *     The doi value
     */
    public double getDoiOptimizer()
    {
        return doiOptimizer;
    }
    
    /**
     * Retrieve the first of index in the given pair of index
     * 
     * @return
     *     an index object
     */
    public Index getFirstIndex()
    {
        return indexC;
    }
    
    /**
     * Retrieve the second of index in the given pair of index
     * 
     * @return
     *     an index object
     */
    public Index getSecondIndex()
    {
        return indexD; 
    }
    
    /**
     * Construct variables of type: VAR_X, VAR_Y, VAR_S, VAR_U used in the BIP formulation,
     * and store these variables in the pool of {@code poolVariables}.
     * E.g., variable {@code y^c(d, 0)} is used to determine whether the first template plan
     * is used to compute {@code cost(q, A_c \cup \{ c \}).  
     * 
     * We also record the set of variables used in the formula of each  
     * {@code cost(q, Atheta \cup Stheta}} with {@code theta \in \{ EMPTY, C, D, CD })
     * in order to build the index interaction constraint.
     * 
     * We also map variable of type {@code TYPE_S} to the index that this variable is defined 
     * on (for debugging purpose).   
     * @throws IloException 
     *  
     */
    protected void constructBinaryVariables() throws IloException
    {   
        poolVariables = new IIPVariablePool();        
        mapVarSIndex  = new HashMap<String, Index>();        
        int q         = desc.getStatementID();
        
        for (int theta : listTheta) {
            // var y
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                poolVariables.createAndStore(theta, VAR_Y, q, k, 0, 0);
           
            // var x
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                for (int i = 0; i < desc.getNumberOfSlots(k); i++)
                    for (Index index : desc.getIndexesAtSlot(k, i))                        
                        poolVariables.createAndStore(theta, VAR_X, q, k, i, index.getId());   
            
            // var s
            for (Index index : candidates) {                
                BIPVariable var = poolVariables.createAndStore (theta, VAR_S, q, 0, 0, index.getId());
                mapVarSIndex.put(var.getName(), index);
            }
            
            // var u
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) 
                for (int i = 0; i < desc.getNumberOfSlots(t); i++) {
                    boolean hasFTS = false;
                    for (Index index : desc.getIndexesAtSlot(i, i)) { 
                        poolVariables.createAndStore(theta, VAR_U, q, t, i, index.getId());
                        if (index instanceof FullTableScanIndex)
                            hasFTS = true;
                    }
                    // We need a variable for FTS of type VAR_U
                    if (!hasFTS) {
                        Index fts = desc.getFTSAtSlot(t, i);
                        poolVariables.createAndStore(theta, VAR_U, q, t, i, fts.getId());
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
            
        if (cplexVar != null)
            cplexVar = null;
        
        IloNumVar[] iloVar = cplex.numVarArray(size, lb, ub, type);
        cplex.add(iloVar);
        
        for (int i = 0; i < size; i++) {
            iloVar[i].setName(vars.get(i).getName());
        }
        
        cplexVar = new ArrayList<IloNumVar>(Arrays.asList(iloVar));
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
    protected void atomicConstraintForINUM(int theta) throws IloException
    {
        IloLinearNumExpr expr;
        int id;
        int idY;
        int idS;
        
        int q = desc.getStatementID();
        expr = cplex.linearNumExpr();
        // (3a) \sum_{k \in [1, Kq]}y^{theta}_k = 1
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idY = poolVariables.get(theta, VAR_Y, q, k, 0, 0).getId();
            expr.addTerm(1, cplexVar.get(idY));
        }
            
        cplex.addEq(expr, 1, "atomic_" + numConstraints);
        numConstraints++;
            
        // (3b) \sum_{a \in S+_i \cup I_{\emptyset}} x(theta, k, i, a) = y(theta, k)
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idY = poolVariables.get(theta, VAR_Y, q, k, 0, 0).getId();
                
            for (int i = 0; i < desc.getNumberOfSlots(k); i++) {
                expr = cplex.linearNumExpr();
                expr.addTerm(-1, cplexVar.get(idY));
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    id = poolVariables.get(theta, VAR_X, q, k, i, index.getId()).getId();
                    expr.addTerm(1, cplexVar.get(id));                        
                }
                    
                cplex.addEq(expr, 0, "atomic_" + numConstraints);                    
                numConstraints++;
            }
        }       
                
            
        //  x(index) - s(index) <= 0
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            for (int i = 0; i < desc.getNumberOfSlots(k); i++) {
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    
                    id  = poolVariables.get(theta, VAR_X, q, k, i, index.getId()).getId();                        
                    idS = poolVariables.get(theta, VAR_S, q, 0, 0, index.getId()).getId();
                    
                    expr = cplex.linearNumExpr();
                    expr.addTerm(1, cplexVar.get(id));
                    expr.addTerm(-1, cplexVar.get(idS)); 
                    cplex.addLe(expr, 0, "atomic_" + numConstraints);
                    numConstraints++;
                }
            }
        }  
    }
    
    
    /**
     * This set of constraints ensures the investing pairs of indexes 
     * do not appear in {@code Atheta}.
     * 
     * For example, index {@code c} is not allowed to appear in the index-set {@code Aempty}
     * @throws IloException 
     */
    protected void interactionPrecondition() throws IloException
    {   
        IloLinearNumExpr expr;        
        int idS;        
        int q = desc.getStatementID();
        
        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_EMPTY, VAR_S, q, 0, 0, indexC.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 0, "Empty_" + numConstraints); 
        // For s^{empty}_c = 0     
        numConstraints++;
        
        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_EMPTY, VAR_S, q, 0, 0, indexD.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 0, "Empty_" + numConstraints); 
        // For s^{empty}_d = 0     
        numConstraints++;
        
        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_D, VAR_S, q, 0, 0, indexC.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 0, "sdc_" + numConstraints); 
        // For s^{d}_c = 0     
        numConstraints++;
        
        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_D, VAR_S, q, 0, 0, indexD.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 1, "sdc_" + numConstraints); 
        // For s^{d}_d = 1     
        numConstraints++;
             

        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_C, VAR_S, q, 0, 0, indexC.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 1, "scd_" + numConstraints); 
        // For s^{c}_c = 1     
        numConstraints++;
        
        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_C, VAR_S, q, 0, 0, indexD.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 0, "scd_" + numConstraints); 
        // For s^{c}_d = 0     
        numConstraints++;
        
        
        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_CD, VAR_S, q, 0, 0, indexD.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 1, "sdc_" + numConstraints); 
        // For s^{cd}_d = 1     
        numConstraints++;
        
        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_CD, VAR_S, q, 0, 0, indexC.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 1, "sdc_" + numConstraints); 
        // For s^{cd}_c = 1     
        numConstraints++;
    }
    
    /**
     * Construct the formula of the query execution cost of the investigating statement
     * using the configuration {@code Atheta \cup Stheta}
     *        {@code cost(q, Atheta \cup Stheta)}      
     *        {@code  =  sum_{k \in [1, Kq] } beta_k y_{k,theta} }
     *        {@code  +  \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} } 
     *        {@code     x_{kia, theta}}
     * @throws IloException 
     *
     */
    protected void buildQueryExecutionCost(int theta) throws IloException
    {   
        int q;
        int idY, idX;      
        
        q = desc.getStatementID();        
        Map<Integer, Double> coefs = new HashMap<Integer, Double>();
            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
        
            idY = poolVariables.get(theta, VAR_Y, q, k, 0, 0).getId();
            coefs.put(idY, desc.getInternalPlanCost(k));
                
            // index access cost
            for (int i = 0; i < desc.getNumberOfSlots(k); i++) 
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    idX = poolVariables.get(theta, VAR_X, q, k, i, index.getId()).getId();                        
                    coefs.put(idX, desc.getAccessCost(k, i, index));
                }
        }
            
        mapThetaVarCoef.put(theta, coefs);
    }
    
    
    /**
     * The method clears all data structures
     */
    public void clear()
    {
        poolVariables.clear();
        mapThetaVarCoef.clear();
        try {
            cplex.clearModel();
        } catch (IloException e) {
            e.printStackTrace();
        }
        
        cplexVar.clear();
    }   
    
    /**
     * This set of constraints ensure {@code cost(q, Atheta \cup Atheta} is 
     * not greater than the local optimal cost of using any template plan.  
     * @throws IloException 
     * 
     */
    protected void localOptimal(int theta) throws IloException
    {   
        IloLinearNumExpr expr;        
        int idU;
        double approxCoef;        
        int q;
        
        q = desc.getStatementID();        
        if (isApproximation)
            approxCoef = 1.2;
        else 
            approxCoef = 1.0;
        
        boolean isFTSInSlot;// construct C^opt_t
       
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
            expr = cplex.linearNumExpr();
                
            for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(theta).entrySet())
                expr.addTerm(coefs.getValue(), cplexVar.get(coefs.getKey()));
            
            for (int i = 0; i < desc.getNumberOfSlots(t); i++) { 
                isFTSInSlot = false;
                for (Index index : desc.getIndexesAtSlot(t, i)) {
                    idU = poolVariables.get(theta, VAR_U, q, t, i, index.getId()).getId();
                    expr.addTerm(-approxCoef * desc.getAccessCost(t, i, index), cplexVar.get(idU));
                    
                    if (index instanceof FullTableScanIndex)
                        isFTSInSlot = true;
                }
                
                if (isFTSInSlot) {
                    Index fts = desc.getFTSAtSlot(t, i);
                    idU = poolVariables.get(theta, VAR_U, q, t, i, fts.getId()).getId();
                    expr.addTerm(-approxCoef * BIP_MAX_VALUE, cplexVar.get(idU));
                }
                
                cplex.addLe(expr, approxCoef * desc.getInternalPlanCost(t), "local_" + numConstraints);
                numConstraints++;
            }
        }
    }
    
    /**
     * The set of atomic constraints on variable of type {@code VAR_U}
     * @throws IloException 
     */
    protected void atomicConstraintLocalOptimal(int theta) throws IloException
    {   
        IloLinearNumExpr expr, exprPresent;
        int idU, idS;
        int q = desc.getStatementID();
        boolean isFTSInSlot;
        
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
            for (int i = 0; i < desc.getNumberOfSlots(t); i++) {
                expr = cplex.linearNumExpr();
                isFTSInSlot = false;
                for (Index index : desc.getIndexesAtSlot(t, i)) {
                    idU = poolVariables.get(theta, VAR_U, q, t, i, index.getId()).getId();
                    idS = poolVariables.get(theta, VAR_S, q, 0, 0, index.getId()).getId();
                    expr.addTerm(1, cplexVar.get(idU));
                    
                    exprPresent = cplex.linearNumExpr();
                    exprPresent.addTerm(1, cplexVar.get(idU));
                    exprPresent.addTerm(-1, cplexVar.get(idS));            
                    cplex.addLe(exprPresent, 0, "U_present_" + numConstraints);
                    numConstraints++;
                    
                    if (index instanceof FullTableScanIndex)
                        isFTSInSlot = true;
                }   
                    
                // add an infinity for the FTS-U variable
                if (!isFTSInSlot) {
                    Index fts = desc.getFTSAtSlot(t, i);
                    idU = poolVariables.get(theta, VAR_U, q, t, i, fts.getId()).getId();                    
                    expr.addTerm(1, cplexVar.get(idU));   
                }
                    
                cplex.addEq(expr, 1, "atomic_U" + numConstraints);
                numConstraints++;
            }
        }
    }
    
    
    
    /**
     * 
     * The constraints ensure the index with the small index access cost is used
     * to compute {@code cost(q, Atheta \cup Stheta} 
     * @throws IloException 
     * 
     */
    protected void selectingIndexAtEachSlot(int theta) throws IloException
    {   
        int q = desc.getStatementID();
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++)
            for (int i = 0; i < desc.getNumberOfSlots(t); i++) 
                selectingIndexAtEachSlot(theta, q, t, i, desc);
        
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
    protected void selectingIndexAtEachSlot(int theta, int q, int t, int i, QueryPlanDesc desc) 
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
            idU = poolVariables.get(VAR_U, theta, q, t, i, index.getId()).getId();
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
                    
                idS = poolVariables.get(VAR_S, theta, 0, 0, 0, index.getId()).getId();
                exprInternal.addTerm(-1, cplexVar.get(idS));
                cplex.addGe(exprInternal, 0, "select_index_" + numConstraints); 
                numConstraints++;
            }   
        }   
    }
    
    /**
     * Build the first index interaction constraint: 
     * {@code cost(Aempty) + (1 + delta) cost(Acd \cup \{ c,d \} )} 
     *         {@code - cost(Ac \cup \{ c \}) - cost(Ad \cup \{ d \} ) <= 0 }
     * @throws IloException 
     *      
     */
    protected void indexInteractionConstraint1() throws IloException
    {   
        double newDelta = 1 + delta;
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        // Aempty
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_EMPTY).entrySet()){
            expr.addTerm(coefs.getValue(), cplexVar.get(coefs.getKey()));
        }
        
        // for -Ac
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_C).entrySet()){
            expr.addTerm(-coefs.getValue(), cplexVar.get(coefs.getKey()));
        }
        
        // for -Ad
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_D).entrySet()){
            expr.addTerm(-coefs.getValue(), cplexVar.get(coefs.getKey()));
        }
        
        // for newDelta * Acd
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_CD).entrySet()){
            expr.addTerm(newDelta * coefs.getValue(), cplexVar.get(coefs.getKey()));
        }
        
        cplex.addLe(expr, 0, "interaction_1");
        exprIteraction1 = expr;
    }
    
    /**
     * Replace the index interaction constraint by the following:
     * cost(X,c) + cost(X,d) - cost(X) + (delta - 1) cost(X,c,d)  <= 0
     * 
     * @return
     *      {@code true} if CPLEX returns a solution,
     *      {@code false} otherwise
     * @throws IloException 
     */
    protected void solveAlternativeBIP(boolean isSolveAlternativeOnly) 
                   throws IloException
    {       
        // delete the first interaction constraint
        if (isSolveAlternativeOnly == false)
            cplex.delete(exprIteraction1);
        
        double newDelta = delta - 1;
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        // for Ac
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_C).entrySet()){
            expr.addTerm(coefs.getValue(), cplexVar.get(coefs.getKey()));
        }
        
        // for Ad
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_D).entrySet()){
            expr.addTerm(coefs.getValue(), cplexVar.get(coefs.getKey()));
        }
        
        // for -Aempty
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_EMPTY).entrySet()){
            expr.addTerm(-coefs.getValue(), cplexVar.get(coefs.getKey()));
        }
        
        // for newDelta * Acd
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_CD).entrySet()){
            expr.addTerm(newDelta * coefs.getValue(), cplexVar.get(coefs.getKey()));
        }    
        cplex.addLe(expr, 0, "interaction_2");
    }
    
    /**
     * Compute the actual query execution cost derived by the CPLEX solver.
     * This method is implemented to check the correctness of our BIP solution. 
     * @throws IloException 
     * @throws UnknownObjectException 
     */
    protected double computeDoi() throws UnknownObjectException, IloException
    {
        double[] val = cplex.getValues(cplexVar.toArray(new IloNumVar[cplexVar.size()]));
       
        double Cempty, Cc, Cd, Ccd;
        
        Cempty = 0.0;
        Cc = 0.0;
        Cd = 0.0;
        Ccd = 0.0;
        
        // for Aemtpy
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_EMPTY).entrySet())
            Cempty += (coefs.getValue() * val[coefs.getKey()]);
               
        Rt.p(" cemtpy: " + Cempty);
        
        // for Ac
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_C).entrySet())
            Cc += (coefs.getValue() * val[coefs.getKey()]);
        
        Rt.p(" cc: " + Cc);
        
        // Ad
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_D).entrySet())            
            Cd += (coefs.getValue() * val[coefs.getKey()]);
        
        Rt.p(" cd: " + Cd);
        
        // Acd
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_CD).entrySet())
            Ccd += (coefs.getValue() * val[coefs.getKey()]);
        
        Rt.p(" ccd: " + Ccd);
    
        double doi = Math.abs(Cempty + Ccd - Cc - Cd) / Ccd;

        return doi;
    }
}
