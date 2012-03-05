package edu.ucsc.dbtune.bip.interactions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;


import edu.ucsc.dbtune.bip.core.BIPVariable;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.cplex.IloCplex;
import ilog.cplex.IloCplex.UnknownObjectException;

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
    protected Set<Index>         candidateIndexes;
    
    protected double delta;     
    protected int    iC;
    protected int    iD; 
    protected Index  indexC;
    protected Index  indexD;
    protected int    numConstraints;
    protected double doiOptimizer;
    protected double doiBIP;
    
    protected IloLinearNumExpr exprIteraction1;
    
    protected IloCplex        cplex;
    protected List<IloNumVar> cplexVar;
    
    protected LogListener logger;
    
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
                         final Set<Index> candidateIndexes,
                         final int ic, final int id)
    {
        this.delta            = delta;
        this.iC               = ic;
        this.iD               = id;
        this.indexC           = indexc;
        this.indexD           = indexd;
        this.desc             = desc;
        this.logger           = logger;
        this.candidateIndexes = candidateIndexes;
        this.cplexVar         = null;
        this.cplex            = cplex;
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
        buildQueryExecutionCost();
        
        // 2.1. Atomic constraints 
        atomicConstraintForINUM();
        atomicConstraintAtheta();
        interactionPrecondition();        
        
        // 2,2. Optimal constraints
        localOptimal();
        atomicConstraintLocalOptimal();
        presentVariableLocalOptimal();
        selectingIndexAtEachSlot();
        
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
        
        if (iC != iD) {           
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
        
        //if (isInteracting) 
          //  doiBIP = computeDoi();
        
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
                poolVariables.createAndStore(theta, VAR_Y, q, k, 0);
           
            // var x
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                for (int i = 0; i < desc.getNumberOfSlots(); i++)
                    for (Index index : desc.getIndexesAtSlot(i))                        
                        poolVariables.createAndStore(theta, VAR_X, q, k, index.getId());   
            
            // var s
            for (Index index : candidateIndexes) {                
                BIPVariable var = poolVariables.createAndStore (theta, VAR_S, q, 0, index.getId());
                mapVarSIndex.put(var.getName(), index);
            }
            
            // var u
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) 
                for (int i = 0; i < desc.getNumberOfSlots(); i++) 
                    for (Index index : desc.getIndexesAtSlot(i)) 
                        poolVariables.createAndStore(theta, VAR_U, q, t, index.getId());
                
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
    protected void atomicConstraintForINUM() throws IloException
    {
        IloLinearNumExpr expr;
        int id;
        int idY;
        int idS;
        
        int q = desc.getStatementID();
        
        for (int theta : listTheta) {
            
            expr = cplex.linearNumExpr();
            
            // (3a) \sum_{k \in [1, Kq]}y^{theta}_k = 1
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                idY = poolVariables.get(theta, VAR_Y, q, k, 0).getId();
                expr.addTerm(1, cplexVar.get(idY));
            }
            
            cplex.addEq(expr, 1, "atomic_" + numConstraints);
            numConstraints++;
            
            // (3b) \sum_{a \in S+_i \cup I_{\emptyset}} x(theta, k, i, a) = y(theta, k)
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {               
                
                idY = poolVariables.get(theta, VAR_Y, q, k, 0).getId();
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    expr = cplex.linearNumExpr();
                    expr.addTerm(-1, cplexVar.get(idY));
                    
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        
                        id = poolVariables.get(theta, VAR_X, q, k, index.getId()).getId();
                        expr.addTerm(1, cplexVar.get(id));                        
                    }
                    
                    cplex.addEq(expr, 0, "atomic_" + numConstraints);                    
                    numConstraints++;
                }
            }       
            
            
            // x(index) - s(index) <= 0
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        
                        id  = poolVariables.get(theta, VAR_X, q, k, index.getId()).getId();                        
                        idS = poolVariables.get(theta, VAR_S, q, 0, index.getId()).getId();
                        
                        expr = cplex.linearNumExpr();
                        expr.addTerm(1, cplexVar.get(id));
                        expr.addTerm(-1, cplexVar.get(idS)); 
                        cplex.addLe(expr, 0, "atomic_" + numConstraints);
                        numConstraints++;
                    }
                }
            }    
        }    
    }
    
    /**
     * The set of constraints that enforces {@code Atheta} to be atomic in the
     * {@code RestrictIIP} problem.
     * 
     * That is {@code Atheta} has at most one index of a particular relation. 
     * @throws IloException 
     */
    protected void atomicConstraintAtheta() throws IloException
    {
        IloLinearNumExpr expr;        
        int idS;
        
        int q = desc.getStatementID();
        boolean hasTerm;
        
        for (int theta : listTheta) {
            
            for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                
                if ( (theta == IND_C && i == iC) 
                     || (theta == IND_D && i == iD)
                     || (theta == IND_CD && i == iC)
                     || (theta == IND_CD && i == iD)                     
                     )
                // special case to handle later
                    continue;
                
                
                // - not consiDer full table scan
                // - it could be possible s^{theta}_{a} = 1 and s^{theta}_{full table scan} = 1
                // - in this case full table scan is used instead of a for the optimal cost
                expr = cplex.linearNumExpr();
                hasTerm = false; 
                for (Index index : desc.getIndexesWithoutFTSAtSlot(i)) {
                    idS = poolVariables.get(theta, VAR_S, q, 0, index.getId()).getId();
                    expr.addTerm(1, cplexVar.get(idS));
                    hasTerm = true;
                }
                
                if (hasTerm) {
                    cplex.addLe(expr, 1, "Atheta_" + numConstraints);                
                    numConstraints++;
                }
            }
        }
        
        
        // for case of Ac and Acd
        for (int theta : listTheta) {
            
            if (theta == IND_EMPTY || theta == IND_D)
                continue; 
            
            // not consider full table scan
            // and exclude {@code indexC}
            expr = cplex.linearNumExpr();
            hasTerm = false;
            for (Index index : desc.getIndexesWithoutFTSAtSlot(iC)) {
                
                if (!index.equals(indexC)) {
                    idS = poolVariables.get(theta, VAR_S, q, 0, index.getId()).getId();
                    expr.addTerm(1, cplexVar.get(idS));
                    hasTerm = true;
                }
                
            }
            // since we  do not consiDer FTS, the list can be empty
            if (hasTerm) {
                cplex.addLe(expr, 1, "Atheta" + numConstraints);
                numConstraints++;
            }
        }
        
        // for case of Ad and Acd
        for (int theta : listTheta) {
            
            if (theta == IND_EMPTY || theta == IND_C)
                continue;
            
            expr = cplex.linearNumExpr();
            hasTerm = false;
            
            // not consider full table scan 
            for (Index index : desc.getIndexesWithoutFTSAtSlot(iD)){
                
                if (!index.equals(indexD)) {
                    idS = poolVariables.get(theta, VAR_S, q, 0, index.getId()).getId();
                    expr.addTerm(1, cplexVar.get(idS));
                    hasTerm = true;
                }
            }
            
            // since we  do not consiDer FTS, the list can be empty
            if (hasTerm) {
                cplex.addLe(expr, 1, "Atheta_" + numConstraints);
                numConstraints++;
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
        idS = poolVariables.get(IND_EMPTY, VAR_S, q, 0, indexC.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 0, "Empty_" + numConstraints); // For s^{empty}_c = 0     
        numConstraints++;
        
        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_D, VAR_S, q, 0, indexC.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 0, "sdc_" + numConstraints); // For s^{d}_c = 0     
        numConstraints++;
        
        
        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_EMPTY, VAR_S, q, 0, indexD.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 0, "Empty_" + numConstraints); // For s^{empty}_d = 0     
        numConstraints++;

        expr = cplex.linearNumExpr();
        idS = poolVariables.get(IND_C, VAR_S, q, 0, indexD.getId()).getId();
        expr.addTerm(1, cplexVar.get(idS));
        cplex.addEq(expr, 0, "scd_" + numConstraints); // For s^{c}_d = 0     
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
    protected void buildQueryExecutionCost() throws IloException
    {   
        int    q;
        int    idY, idX;       
        
        q = desc.getStatementID();
        
        mapThetaVarCoef = new HashMap<Integer, Map<Integer,Double>>();
        
        for (int theta : listTheta) {
             
            Map<Integer, Double> coefs = new HashMap<Integer, Double>();
            
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
        
                idY = poolVariables.get(theta, VAR_Y, q, k, 0).getId();
                coefs.put(idY, desc.getInternalPlanCost(k));
                
                // index access cost
                for (int i = 0; i < desc.getNumberOfSlots(); i++) 
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        idX = poolVariables.get(theta, VAR_X, q, k, index.getId()).getId();                        
                        coefs.put(idX, desc.getAccessCost(k, index));
                    }
            }
            
            mapThetaVarCoef.put(theta, coefs);
        }
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
    protected void localOptimal() throws IloException
    {   
        IloLinearNumExpr expr;        
        int idU;
        
        int q = desc.getStatementID();
        
        // construct C^opt_t
        for (int theta : listTheta) {
            
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
                
                expr = cplex.linearNumExpr();
                
                for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(theta).entrySet())
                    expr.addTerm(coefs.getValue(), cplexVar.get(coefs.getKey()));
                
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) 
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        idU = poolVariables.get(theta, VAR_U, q, t, index.getId()).getId();
                        expr.addTerm(-desc.getAccessCost(t, index), cplexVar.get(idU));
                    }
                
                cplex.addLe(expr, desc.getInternalPlanCost(t), "local_" + numConstraints);
                numConstraints++;
            }
        }
    }
    
    /**
     * The set of atomic constraints on variable of type {@code VAR_U}
     * @throws IloException 
     */
    protected void atomicConstraintLocalOptimal() throws IloException
    {
        IloLinearNumExpr expr;
        int idU;
        int q = desc.getStatementID();
        
        for (int theta : listTheta) {
            
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    expr = cplex.linearNumExpr();
                    
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        
                        idU = poolVariables.get(theta, VAR_U, q, t, index.getId()).getId();
                        expr.addTerm(1, cplexVar.get(idU));
                                                                      
                    }
                    
                    cplex.addEq(expr, 1, "atomic_U" + numConstraints);
                    numConstraints++;
                }
            }
        }
    }
    
    /**
     * Constraint on the present of {@code VAR_U} variables.
     * 
     * For example, a variable corresponding to some index {@code a} must be {@code 0}
     * if {@code a} has not been appeared in any of {@code Atheta}
     * @throws IloException 
     * 
     */
    protected void presentVariableLocalOptimal() throws IloException
    {   
        IloLinearNumExpr expr;
        int idU;
        int idS;
        
        int q = desc.getStatementID();
        
        for (int theta : listTheta) {
            
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) { 
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    // no need to constraint FTS
                    // since u_{FTS} <= 1
                    for (Index index : desc.getIndexesWithoutFTSAtSlot(i)) {
                        idU = poolVariables.get(theta, VAR_U, q, t, index.getId()).getId();
                        
                        expr = cplex.linearNumExpr();
                        
                        if ( (theta == IND_C && index.equals(indexC))  
                            || (theta == IND_D &&  index.equals(indexD))
                            || (theta == IND_CD && index.equals(indexC))  
                            || (theta == IND_CD &&  index.equals(indexD))        
                            )
                        {
                            // u^{theta}_{c,d} <= 1 (it is implies by binary variable constraint)
                        } 
                        else if ( (theta == IND_EMPTY && index.equals(indexC)) 
                                || (theta == IND_EMPTY && index.equals(indexD))                        
                                || (theta == IND_C && index.equals(indexD))
                                || (theta == IND_D && index.equals(indexC))
                        )
                        {    
                            expr.addTerm(1, cplexVar.get(idU));
                            cplex.addEq(expr, 0, "U_empty_" + numConstraints); // var_u = 0                            
                            numConstraints++;
                            
                        }                        
                        else {                           
                            // Constraint (6b): u^{theta}_{tia} <= sum_{theta}_{a}                             
                            int ga = index.getId();
                            expr.addTerm(1, cplexVar.get(idU));
                            
                            for (int innerTheta : listTheta) {
                                idS = poolVariables.get(innerTheta, VAR_S, q, 0, ga).getId();
                                expr.addTerm(-1, cplexVar.get(idS));
                            }
                                
                            cplex.addLe(expr, 0, "U_sum_" + numConstraints);
                            numConstraints++;
                        } 
                    }                   
                }         
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
    protected void selectingIndexAtEachSlot() throws IloException
    {  
        int idU;
        int idS;        
        int idFTS, numIndex;
        int q = desc.getStatementID();
        
        for (int theta : listTheta) {    
            
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    // Sort index access cost
                    List<SortableIndexAcessCost> listSortedIndex  = 
                                new ArrayList<SortableIndexAcessCost>();
                    
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
                    
                    for (SortableIndexAcessCost sac : listSortedIndex){  
                        
                        Index index = sac.getIndex();
                        idU = poolVariables.get(theta, VAR_U, q, t, index.getId()).getId();
                        varIDs.add(idU);
                        
                        if (index.equals(indexC)) {
                            // --- \sum >= 1 (for the case of theta = IND_C || IND_D
                            // because this sum is also <= 1 (due to atomic constraint)
                            // therefore, we optimizer to write \sum = 1
                            
                            if (theta == IND_C || theta == IND_CD){
                                IloLinearNumExpr exprInternal = cplex.linearNumExpr();
                                
                                for (int varID : varIDs)
                                    exprInternal.addTerm(1, cplexVar.get(varID));
                                
                                cplex.addEq(exprInternal, 1, "optimal_C_" + numConstraints); 
                                numConstraints++;
                                break; // the remaining variables will be assigned value 0
                            }
                        }
                        else if (index.equals(indexD)){
                        
                            if (theta == IND_D || theta == IND_CD) {
                                
                                IloLinearNumExpr exprInternal = cplex.linearNumExpr();
                                for (int varID : varIDs)
                                    exprInternal.addTerm(1, cplexVar.get(varID));
                                
                                cplex.addEq(exprInternal, 1, "optimal_D_" + numConstraints); 
                                numConstraints++;
                                break; // the remaining variables will be assigned value 0
                            }
                        }
                        else {
                            
                            if (index.getId() == idFTS) {
                                IloLinearNumExpr exprInternal = cplex.linearNumExpr();
                                for (int varID : varIDs)
                                    exprInternal.addTerm(1, cplexVar.get(varID));
                            
                                cplex.addEq(exprInternal, 1, "FTS_" + numConstraints);
                                numConstraints++;
                                
                                break; // the remaining variables will be assigned value 0 
                            } else {
                            
                                for (int thetainternal : listTheta) {
                                    
                                    // exprInternal >= 0                                    
                                    IloLinearNumExpr exprInternal = cplex.linearNumExpr();
                                    for (int varID : varIDs)
                                        exprInternal.addTerm(1, cplexVar.get(varID));
                                    
                                    idS = poolVariables.get(thetainternal, VAR_S, q, 0, index.getId())
                                                        .getId();
                                    exprInternal.addTerm(-1, cplexVar.get(idS));
                                    cplex.addGe(exprInternal, 0, "select_index_" + numConstraints); 
                                    numConstraints++;
                                }   
                            }   
                        }                                           
                    }   
                }
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
               
        System.out.println(" cemtpy: " + Cempty);
        
        // for Ac
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_C).entrySet())
            Cc += (coefs.getValue() * val[coefs.getKey()]);
        
        System.out.println(" cc: " + Cc);
        
        // Ad
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_D).entrySet())            
            Cd += (coefs.getValue() * val[coefs.getKey()]);
        
        
        System.out.println(" cd: " + Cd);
        
        // Acd
        for (Entry<Integer, Double> coefs : mapThetaVarCoef.get(IND_CD).entrySet())
            Ccd += (coefs.getValue() * val[coefs.getKey()]);
        
        System.out.println(" ccd: " + Ccd);
    
        double doi = Math.abs(Cempty + Ccd - Cc - Cd) / Ccd;

        return doi;
    }
}
