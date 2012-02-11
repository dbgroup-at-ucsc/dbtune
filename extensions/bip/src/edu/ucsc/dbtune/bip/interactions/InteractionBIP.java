package edu.ucsc.dbtune.bip.interactions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import com.google.caliper.internal.guava.collect.Sets;


import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.BIPVariable;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import ilog.concert.IloException;
 
/**
 * The class is responsible for solving the interaction problem: 
 * Given a query {@code q}, finds pairs of indexes {@code (c,d)} 
 * that interact with respect to {@code q}; i.e., {@code doi_q(c,d) >= delta}.
 *  
 * @author Quoc Trung Tran
 *
 */
public class InteractionBIP extends AbstractBIPSolver
{	
	public static HashMap<String,Integer> pairInteractingIndexNames;
	
    public static final int IND_EMPTY = 0;
    public static final int IND_C = 1;
    public static final int IND_D = 2;
    public static final int IND_CD = 3;
    
    public final List<Integer> listTheta = 
                    Arrays.asList(IND_EMPTY, IND_C, IND_D, IND_CD); 
                         
    private Map<Integer, String> CTheta;
    private Map<Integer, List<String>> elementCTheta;    
    private Map<Integer, List<String>> varTheta;
    private Map<String, Double> coefVarYXTheta;
    private IIPVariablePool poolVariables;
    private RestrictIIPParam restrictIIP;
    private Map<String, Index> mapVarSIndex;
    
    private InteractionOutput interactionOutput;
    private QueryPlanDesc investigatingDesc;
    private double delta;
	private CPlexInteraction cplex;	
	private SQLStatement sql;
    private Optimizer optimizer;
    
    
    public InteractionBIP(double delta)
    {
        this.delta = delta;
        pairInteractingIndexNames = new HashMap<String, Integer>();
        cplex = new CPlexInteraction();
    }
	
    /**
     * Set the conventional optimizer that will be used to verify the correctness of BIP solution
     * @param optimizer
     *      A conventional optimizer (e.g., DB2Optimizer)
     */
    public void setConventionalOptimizer(Optimizer optimizer)
    {
        this.optimizer = optimizer;
    }
    
    
    @Override
    public IndexTuningOutput solve() throws SQLException, IOException
    {   
        // 1. Communicate with INUM 
        // to derive the query plan description including internal cost, index access cost,
        // index at each slot, etc.  
        logger.setStartTimer();
        populatePlanDescriptionForStatements();
        logger.onLogEvent(LogListener.EVENT_POPULATING_INUM);
        
        // 2. Iterate over the list of query plan descs that have been derived
        interactionOutput = new InteractionOutput();
        int i = 0;
        try {
            for (QueryPlanDesc desc : listQueryPlanDescs) {
                sql = workload.get(i);
                investigatingDesc = desc;
                findInteractions();
                i++;
            }
        } catch (IloException e) {
            throw new RuntimeException(e);
        }
        return getOutput();
    }
    
    
    @Override
    protected IndexTuningOutput getOutput() 
    {
        return interactionOutput;
    }
   
    
    /**
     * Find pairs of indexes from the pool of candidate indexes that interact with each other
     * @throws IloException 
     * @throws IOException 
     *  
     */
    public void findInteractions() throws IloException, IOException  
    {   
        // Derive list of indexes that might interact
        List<Index> indexes = new ArrayList<Index>();
        Map<Index, Integer> mapIndexSlotID = new HashMap<Index, Integer>();
        
        // Note: NOT consider FTS
        for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
            for (Index index : investigatingDesc.getListIndexesWithoutFTSAtSlot(i)) {
                indexes.add(index);
                mapIndexSlotID.put(index, i);
            }   
        }
        
        Index indexc, indexd;
        int ic, id;
        boolean isInteracting;
        
        for (int pos_c = 0; pos_c < indexes.size(); pos_c++) {
            indexc = indexes.get(pos_c);
            ic = mapIndexSlotID.get(indexc);
            
            for (int pos_d = pos_c + 1; pos_d < indexes.size(); pos_d++) {
                indexd = indexes.get(pos_d);
                if (isInteracting(indexc, indexd))
                    continue;
                
                id = mapIndexSlotID.get(indexc);
                // initialize an instance of RestrictIIP problem
                restrictIIP = new RestrictIIPParam(delta, ic, id, indexc, indexd);
                
                // 2. Construct BIP
                logger.setStartTimer();
                initializeBuffer();
                buildBIP();
                logger.onLogEvent(LogListener.EVENT_FORMULATING_BIP);
                
                // 3. Solve the first BIP
                isInteracting = false;
                logger.setStartTimer();
                Map<String, Integer> mapVarVal = cplex.solve(buf.getLpFileName());                
                if (mapVarVal != null) { 
                    cacheInteractingIndexes(indexc, indexd);
                    isInteracting = true;
                } else {
                    // formulate the alternative BIP
                   mapVarVal = solveAlternativeBIP(); 
                   if (mapVarVal != null) {
                       cacheInteractingIndexes(indexc, indexd);
                       isInteracting = true;
                   }
                }  
                logger.onLogEvent(LogListener.EVENT_SOLVING_BIP);
                
                // compute the doi
                if (isInteracting) 
                    storeInteractingPair(indexc, indexd, mapVarVal, optimizer);
            }
        }
    }
    
    
    /**
     * The function builds the BIP for the Restrict IIP problem, includes three sets of constraints:
     * <p>
     * <ol> 
     *  <li> Atomic constraints </li> 
     *  <li> Optimal constraints </li>
     *  <li> One alternative of index interaction constraint </li>
     * </ol>
     * </p>   
     * 
     * <b> Note </b>: The alternative index interaction constraint, (13), will be considered only 
     * if this problem formulation does not return any solution     
     */
    @Override
    protected void buildBIP() 
    {
        super.numConstraints = 0;
        
        // Construct variable
        constructVariables();
        
        // Construct the formula of Ctheta
        buildQueryExecutionCost();
        
        // 1. Atomic constraints 
        atomicConstraintForINUM();
        atomicConstraintAtheta();
        interactionPrecondition();
        
        
        // 2. Optimal constraints
        localOptimal();
        atomicConstraintLocalOptimal();
        presentVariableLocalOptimal();
        selectingIndexAtEachSlot();
        
        // 3. Index interaction 
        indexInteractionConstraint1();
        
        // 4. Binary variables
        binaryVariableConstraints();
        
        buf.close();
        try {
            CPlexBuffer.concat(buf.getLpFileName(), buf.getObjFileName(), 
                                buf.getConsFileName(), buf.getBinFileName());
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot concantenate text files that store BIP.");
        }
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
     *  
     */
    private void constructVariables()
    {   
        poolVariables = new IIPVariablePool();
        varTheta = new HashMap<Integer, List<String>>();
        mapVarSIndex = new HashMap<String, Index>();
        
        for (int theta : listTheta) {
            
            List<String> listVarTheta = new ArrayList<String>();
            // var y
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                
                BIPVariable var = poolVariables.createAndStore(theta, IIPVariablePool.VAR_Y, k, 0);
                listVarTheta.add(var.getName());
            }
           
            // var x
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {    
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        
                        BIPVariable var = poolVariables.createAndStore
                                                   (theta, IIPVariablePool.VAR_X, k, index.getId());
                        listVarTheta.add(var.getName());
                    }
                }
            }       
            
            // var s
            for (Index index : candidateIndexes) {
                
                BIPVariable var = poolVariables.createAndStore
                                                (theta, IIPVariablePool.VAR_S, 0, index.getId());
                listVarTheta.add(var.getName());
                mapVarSIndex.put(var.getName(), index);
            }
            
            // var u
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) {
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        
                        BIPVariable var =  poolVariables.createAndStore(theta, 
                                                         IIPVariablePool.VAR_U, t, index.getId()); 
                        listVarTheta.add(var.getName());
                    }
                }
            }
            
            // record the list of variables of each theta
            varTheta.put(theta, listVarTheta);
        }
    }
    

    /**
     * Construct the formula of the query execution cost of the investigating statement
     * using the configuration {@code Atheta \cup Stheta}
     *        {@code cost(q, Atheta \cup Stheta)}      
     *        {@code  =  sum_{k \in [1, Kq] } beta_k y_{k,theta} }
     *        {@code  +  \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} } 
     *        {@code     x_{kia, theta}}
     *
     */
    private void buildQueryExecutionCost()
    {   
        CTheta = new HashMap<Integer, String>();
        elementCTheta = new HashMap<Integer, List<String>>();
        coefVarYXTheta = new HashMap<String, Double>(); 
        
        double cost;
        String element;
        
        for (int theta : listTheta) {
            
            String ctheta = "";
            List<String> linListElement = new ArrayList<String>();          
            List<String> linList = new ArrayList<String>();
            
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                
                String var = poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0).getName();
                cost = investigatingDesc.getInternalPlanCost(k);
                element = cost + var;
                linList.add(element);
                linListElement.add(element);
                coefVarYXTheta.put(var, cost);   
            }
            ctheta  = Strings.concatenate(" + ", linList);
                        
            // Index access cost
            linList.clear();
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        
                        String var = poolVariables.get(theta, IIPVariablePool.VAR_X, 
                                                        k, index.getId()).getName();
                        cost = investigatingDesc.getAccessCost(k, index);
                        element = cost + var;
                        linList.add(element);
                        linListElement.add(element);
                        coefVarYXTheta.put(var, cost);
                    }
                }
            }       
            ctheta = ctheta + " + " + Strings.concatenate(" + ", linList);
            
            /** Record ctheta as well as element of ctheta */
            CTheta.put(theta, ctheta);
            elementCTheta.put(theta, linListElement);
        }
    }
     
    /**
     * Build a set of atomic constraints on variables {@code VAR_X} and {@code VAR_Y}
     * that are common for methods using INUM.
     * 
     * For example, the summation of all variables of type {@code VAR_Y} of a same {@code theta}
     * must be {@code 1} in order for the optimal execution cost corresponds to only one 
     * template plan.  
     * 
     */
    private void atomicConstraintForINUM()
    {
        for (int theta : listTheta) {
            
            List<String> linList = new ArrayList<String>();
            // (3a) \sum_{k \in [1, Kq]}y^{theta}_k = 1
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++)
                linList.add(poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0).getName());
            
            buf.getCons().println("atomic_3a_" + numConstraints + ": " + 
            		Strings.concatenate(" + ", linList) + " = 1");
            numConstraints++;
            
            // (3b) \sum_{a \in S+_i \cup I_{\emptyset}} x(theta, k, i, a) = y(theta, k)
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                
                String var_y = poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0).getName();
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    linList.clear();
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        String var_x = poolVariables.get(theta, IIPVariablePool.VAR_X, 
                                                         k, index.getId()).getName();
                        linList.add(var_x);
                        String var_s = poolVariables.get(theta, IIPVariablePool.VAR_S, 
                                                         0, index.getId()).getName();
                        
                        // (4a) s_a^{theta} \geq x_{kia}^{theta}
                        buf.getCons().println("atomic_4a_" + numConstraints + ":" 
                                            + var_x + " - " 
                                            + var_s
                                            + " <= 0 ");
                        numConstraints++;
                    }
                    buf.getCons().println("atomic_3b_" + numConstraints  
                                            + ": " + Strings.concatenate(" + ", linList) 
                                            + " - " + var_y
                                            + " = 0");
                    numConstraints++;
                }
            }       
        }    
    }
    
    /**
     * The set of constraints that enforces {@code Atheta} to be atomic in the
     * {@code RestrictIIP} problem.
     * 
     * That is {@code Atheta} has at most one index of a particular relation. 
     */
    private void atomicConstraintAtheta()
    {
        for (int theta : listTheta) {
            
            for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                
                List<String> linList = new ArrayList<String>();
                if ( (theta == IND_C && i == restrictIIP.getPosRelContainC()) 
                     || (theta == IND_D && i == restrictIIP.getPosRelContainD())
                     || (theta == IND_CD && i == restrictIIP.getPosRelContainC())
                     || (theta == IND_CD && i == restrictIIP.getPosRelContainD())                     
                     )
                // special case to handle later
                    continue;
                
                
                // - not consider full table scan
                // - it could be possible s^{theta}_{a} = 1 and s^{theta}_{full table scan} = 1
                // - in this case full table scan is used instead of a for the optimal cost
                for (Index index : investigatingDesc.getListIndexesWithoutFTSAtSlot(i))
                    linList.add(poolVariables.get(theta, IIPVariablePool.VAR_S, 0, index.getId())
                                             .getName());      
                
                if (linList.size() > 0){
                    buf.getCons().println("atomic_4b_" +  numConstraints + ":" 
                            + Strings.concatenate(" + ", linList) 
                            + " <= 1");
                    numConstraints++;
                }
            }
        }
        
        
        // for case of Ac and Acd
        for (int theta : listTheta) {
            
            if (theta == IND_EMPTY || theta == IND_D)
                continue; 
            
            List<String> linList = new ArrayList<String>();
            // not consider full table scan
            // and exclude {@code indexc}
            for (Index index : investigatingDesc.getListIndexesWithoutFTSAtSlot
                                    (restrictIIP.getPosRelContainC())) {
                
                if (!index.equals(restrictIIP.getIndexC())) 
                    linList.add(poolVariables.get(theta, IIPVariablePool.VAR_S, 0, index.getId())
                                             .getName());
                
            }
            // since we  do not consider FTS, the list can be empty
            if (linList.size() > 0) {
                buf.getCons().println("atomic_4b_" +  numConstraints + ":" 
                                    + Strings.concatenate(" + ", linList) 
                                    + " <= 1");
                numConstraints++;
            }
        }
        
        // for case of Ad and Acd
        for (int theta : listTheta) {
            
            if (theta == IND_EMPTY || theta == IND_C)
                continue;
            
            List<String> linList = new ArrayList<String>();
            // not consider full table scan 
            for (Index index : investigatingDesc.getListIndexesWithoutFTSAtSlot
                                                (restrictIIP.getPosRelContainD())){
                
                if (!index.equals(restrictIIP.getIndexD())) 
                    linList.add(poolVariables.get(theta, IIPVariablePool.VAR_S, 0, index.getId())
                                             .getName());
                
            }
            // since we  do not consider FTS, the list can be empty
            if (linList.size() > 0) {
                buf.getCons().println("atomic_4b_" + numConstraints + ":" 
                                    + Strings.concatenate(" + ", linList) 
                                    + "  <= 1");
                numConstraints++;
            }
        }
    }
    
    /**
     * This set of constraints ensures the investing pairs of indexes 
     * do not appear in {@code Atheta}.
     * 
     * For example, index {@code c} is not allowed to appear in the index-set {@code Aempty}
     */
    private void interactionPrecondition()
    {   
        buf.getCons().println("atomic_4c_" + numConstraints + ":" 
                            + poolVariables.get(IND_EMPTY, IIPVariablePool.VAR_S, 0, 
                                                restrictIIP.getIndexC().getId()).getName() 
                                                + " = 0 "); // For s^{empty}_c = 0     
        numConstraints++;
        buf.getCons().println("atomic_4c_" + numConstraints + ":" 
                            + poolVariables.get(IND_D, IIPVariablePool.VAR_S, 0, 
                                                restrictIIP.getIndexC().getId()).getName() 
                                                + " = 0 "); // For s^{d}_c = 0  
        numConstraints++;

        buf.getCons().println("atomic_4c_" + numConstraints + ":" 
                            + poolVariables.get(IND_EMPTY, IIPVariablePool.VAR_S, 0, 
                                                restrictIIP.getIndexD().getId()).getName() 
                                                + " = 0 "); // For s^{empty}_d = 0
        numConstraints++;       
        buf.getCons().println("atomic_4c_" + numConstraints + ":"
                            + poolVariables.get(IND_C, IIPVariablePool.VAR_S, 0, 
                                                restrictIIP.getIndexD().getId()).getName() 
                                                + " = 0 "); // For s^{c}_d = 0
        numConstraints++;
    }
    
    
    
    /**
     * This set of constraints ensure {@code cost(q, Atheta \cup Atheta} is 
     * not greater than the local optimal cost of using any template plan.  
     * 
     */
    private void localOptimal()
    {   
        // construct C^opt_t
        for (int theta : listTheta) {
            
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) {
                
                List<String> linList = new ArrayList<String>();     
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, t, 
                                                        index.getId()).getName();       
                        linList.add(investigatingDesc.getAccessCost(t, index) + var_u);                                              
                    }
                }
                // Constraint (5)
                buf.getCons().println("optimal_5_" + numConstraints + ":" 
                        + CTheta.get(theta) + " - " 
                        + Strings.concatenate(" - ", linList)        
                        + " <=  " + investigatingDesc.getInternalPlanCost(t));
                numConstraints++;
            }
        }
    }
    
    /**
     * The set of atomic constraints on variable of type {@code VAR_U}
     */
    private void atomicConstraintLocalOptimal()
    {
        for (int theta : listTheta) {
            
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) {
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    List<String> linList = new ArrayList<String>();
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, t, 
                                                        index.getId()).getName();       
                        linList.add(var_u);                                              
                    }
                    // Atomic constraint (6a) 
                    buf.getCons().println("optimal_6a_" + numConstraints + ":" 
                                        + Strings.concatenate(" + ", linList) + " = 1");
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
     * 
     */
    private void presentVariableLocalOptimal()
    {   
        for (int theta : listTheta) {
            
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) { 
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    // no need to constraint FTS
                    // since u_{FTS} <= 1
                    for (Index index : investigatingDesc.getListIndexesWithoutFTSAtSlot(i)) {
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, t, 
                                                        index.getId()).getName();                       
                        if ( (theta == IND_C && index.equals(restrictIIP.getIndexC()))  
                            || (theta == IND_D &&  index.equals(restrictIIP.getIndexD()))
                            || (theta == IND_CD && index.equals(restrictIIP.getIndexC()))  
                            || (theta == IND_CD &&  index.equals(restrictIIP.getIndexD()))        
                            )
                        {
                            // u^{theta}_{c,d} <= 1 (it is implies by binary variable constraint)
                        } 
                        else if ( (theta == IND_EMPTY && index.equals(restrictIIP.getIndexC())) 
                                || (theta == IND_EMPTY && index.equals(restrictIIP.getIndexD()))                        
                                || (theta == IND_C && index.equals(restrictIIP.getIndexD()))
                                || (theta == IND_D && index.equals(restrictIIP.getIndexC()))
                        )
                        {                           
                            buf.getCons().println("optimal_6c_" + numConstraints + ":" 
                                                 + var_u + " = 0 ");
                            numConstraints++;
                            
                        }                        
                        else {                           
                            // Constraint (6b): u^{theta}_{tia} <= sum_{theta}_{a}                             
                            int ga = index.getId();
                            buf.getCons().println("optimal_6b_" + numConstraints + ":" 
                                        + var_u 
                                        + " - " + poolVariables.get(IND_EMPTY, 
                                                                    IIPVariablePool.VAR_S, 0, ga)
                                                                    .getName()
                                        + " - " + poolVariables.get(IND_C, 
                                                                    IIPVariablePool.VAR_S, 0, ga)
                                                                    .getName()
                                        + " - " + poolVariables.get(IND_D, 
                                                                    IIPVariablePool.VAR_S, 0, ga)
                                                                    .getName()
                                        + " - " + poolVariables.get(IND_CD, 
                                                                    IIPVariablePool.VAR_S, 0, ga)
                                                                    .getName()
                                        + " <= 0 ");
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
     * 
     */
    private void selectingIndexAtEachSlot()
    {  
        int idFTS, numIndex;
        for (int theta : listTheta) {    
            
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) {
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    // Sort index access cost
                    List<SortableIndexAcessCost> listSortedIndex  = 
                                new ArrayList<SortableIndexAcessCost>();
                    
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        SortableIndexAcessCost sac = new SortableIndexAcessCost
                                                (investigatingDesc.getAccessCost(t, index), index);
                        listSortedIndex.add(sac);                       
                    }                   
                    
                    numIndex = investigatingDesc.getListIndexesAtSlot(i).size();
                    idFTS = investigatingDesc.getListIndexesAtSlot(i).get(numIndex - 1).getId();
                    
                    // sort in the increasing order of the index access cost
                    Collections.sort(listSortedIndex);
                    List<String> linList = new ArrayList<String>();
                    
                    for (SortableIndexAcessCost sac : listSortedIndex){  
                        
                        Index index = sac.getIndex();
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, t, 
                                                        index.getId()).getName();
                        linList.add(var_u);
                        String LHS = Strings.concatenate(" + ", linList);
                        
                        if (index.equals(restrictIIP.getIndexC())){
                            // --- \sum >= 1 (for the case of theta = IND_C || IND_D
                            // because this sum is also <= 1 (due to atomic constraint)
                            // therefore, we optimizer to write \sum = 1
                            if (theta == IND_C || theta == IND_CD) {
                                buf.getCons().println("optimal_7c_" + numConstraints + ":" 
                                                       + LHS  
                                                       + " = 1");
                                numConstraints++;
                            }
                        }
                        else if (index.equals(restrictIIP.getIndexD())) {
                            // --- \sum >= 1
                            if (theta == IND_D || theta == IND_CD) {
                                buf.getCons().println("optimal_7d_" + numConstraints + ":" 
                                        + LHS  
                                        + " = 1");
                                numConstraints++;
                            }
                        }
                        else {                
                            // Full table scan: must use
                            if (index.getId() == idFTS) {
                                
                                buf.getCons().println("optimal_7_FTS_" + numConstraints + ":" 
                                        + LHS + " = 1");
                                numConstraints++;
                            } else {
                                
                                for (int thetainternal = IND_EMPTY; thetainternal <= IND_CD; 
                                     thetainternal++) {
                                    // Constraint (8)
                                    buf.getCons().println("optimal_7_" + numConstraints + ":" 
                                            + LHS + " - "
                                            + poolVariables.get(thetainternal, IIPVariablePool.VAR_S, 
                                                            0, index.getId()).getName()
                                            + " >= 0");
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
     *      
     */

    private void indexInteractionConstraint1()
    {    
        ArrayList<String> listcd = new ArrayList<String>();
        double coef, realCoef;
        coef = 1 + restrictIIP.getDelta();
        Object found;
        for (String var : varTheta.get(IND_CD)) {
            found = coefVarYXTheta.get(var);
            if (found != null) {
                realCoef = (Double) found * coef;
                listcd.add(realCoef + var);
            }   
        }
        
        buf.getCons().println("interaction_8a:" 
                              + CTheta.get(IND_EMPTY) 
                              + " + " + Strings.concatenate(" + ", listcd) 
                              + " - " + Strings.concatenate(" - ", elementCTheta.get(IND_C))
                              + " - " + Strings.concatenate(" - ", elementCTheta.get(IND_D))
                              + " <= 0");
    }
    
    /**
     * Replace the index interaction constraint by the following:
     * cost(X,c) + cost(X,d) - cost(X) + (delta - 1) cost(X,c,d)  <= 0
     * 
     * @return
     *      A map from variable to their assigned binary values
     *      or NULL, otherwise
     */
    public Map<String, Integer> solveAlternativeBIP()
    {   
        Map<String,Double> mapVarCoef = new HashMap<String, Double>();
        double deltaCD = restrictIIP.getDelta() - 1; 
        double coef, existingCoef;
        Object found;
        
        for (int theta : listTheta) {
            
            for (String var : varTheta.get(theta)) {
                coef = 0.0;
                found = coefVarYXTheta.get(var);
                // Variables of TYPE Y or X
                if (found != null) {
                    existingCoef = (Double) found;
                    switch(theta) {
                    case IND_EMPTY:
                        coef = -existingCoef;
                        break;
                    case IND_C:
                        coef = existingCoef;
                        break;
                    case IND_D:
                        coef = existingCoef;
                        break;
                    case IND_CD:
                        coef = deltaCD * existingCoef;
                        break;
                    }
                    mapVarCoef.put(var, coef);
                }
            }           
        }
        
        return cplex.solveAlternativeInteractionConstraint(mapVarCoef);
    }
    
    /**
     * Output the list of variables into @bin file to constraint
     * binary variables
     * 
     */
    private void binaryVariableConstraints()
    {   
        int NUM_VAR_PER_LINE = 10;
        String strListVars = poolVariables.enumerateList(NUM_VAR_PER_LINE);
        buf.getBin().println(strListVars);
    }
    
    
    /**
     * Store the these two indexes as interact indexes and add to the cache
     * 
     * @param indexc
     *      The first index
     * @param indexd
     *      The second index
     */
    private void cacheInteractingIndexes(Index indexc, Index indexd)
    {                                  
        String combinedName = indexc + "_" + indexd;        
        pairInteractingIndexNames.put(combinedName, 1);
        combinedName = indexd + "_" + indexc;        
        pairInteractingIndexNames.put(combinedName, 1);
    }
	
	
	/**
	 * Checking whether pair of indexes (indexc, indexd) have been in the cache
	 * 
	 * @param indexc, indexd
	 * 		The two given indexes
	 * 
	 * @return {@code true/false}
	 */
	private boolean isInteracting(Index indexc, Index indexd)
	{
		String key = indexc  + "_" + indexd;
		return pairInteractingIndexNames.containsKey(key); 
	}
		
	
	/**
	 * This function computes the degree of interaction from BIP as well as from the actual
	 * optimizer and store this pair in the output
	 * 
	 * @param indexc
	 *     The first interacting index
	 * @param indexd
	 *     The second interacting index	      
	 * @param mapVarVal
	 *     A map between the variable and the assignment found by CPLEX solver
	 * 
	 */
	private void storeInteractingPair(Index indexc, Index indexd, 
	                                  Map<String, Integer> mapVarVal,
	                                  Optimizer optimizer)
	{   
	    IndexInteraction pair = new IndexInteraction(indexc, indexd);
	    pair.setDoiBIP(computeDoi(mapVarVal));
	    
	    // compute doi from the optimizer
	    double eleVar;
	    Object found;
	    Map<Integer, Set<Index>> mapThetaIndexSet = new HashMap<Integer, Set<Index>>();

	    for (int theta : listTheta) {
	        
	        Set<Index> Atheta = new HashSet<Index>();
	        
	        for (String var : varTheta.get(theta)) {
	            
	            if (!mapVarVal.containsKey(var))
	                continue;
	            
	            eleVar = mapVarVal.get(var);
	            if (eleVar == 1) {
	                // if the variable has been assigned value 1 and 
	                // is of type TYPE_S
	                found = mapVarSIndex.get(var);
	                if (found != null)
	                    Atheta.add((Index) found);
	                
	            }
	        }
	        mapThetaIndexSet.put(theta, Atheta);
	    }
	    
	    Set<SQLStatement> sqls = Sets.newHashSet(sql);
	    InteractionOnOptimizer ioo = new InteractionOnOptimizer(indexc, indexd, mapThetaIndexSet);
	    try {
            ioo.verify(optimizer, null, sqls);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	    pair.setDoiOptimizer(ioo.getDoiOptimizer());
	    
	    interactionOutput.addInteraction(pair);
	}
	
	/**
     * Print the actual query execution cost derived by the CPLEX solver.
     * This method is implemented to check the correctness of our BIP solution. 
     */
    private double computeDoi(Map<String, Integer> mapVarVal)
    {
        List<Double> listCTheta = new ArrayList<Double>();
        double coef, eleVar, ctheta;
        Object found;
        
        for (int theta : listTheta) { 
            
            ctheta = 0.0;     
            for (String var : varTheta.get(theta)) {
                coef = 0.0;
                found = coefVarYXTheta.get(var);
                // Variables of TYPE Y or X
                if (found != null) {
                    coef = (Double) found;
                    eleVar = mapVarVal.get(var);
                    ctheta += coef * eleVar;
                }
            }
            
            System.out.println(" Ctheta " + theta + " : " + ctheta);
            listCTheta.add(ctheta);
        }
        
        double doi = Math.abs(listCTheta.get(IND_EMPTY) + listCTheta.get(IND_CD)
                              - listCTheta.get(IND_C) - listCTheta.get(IND_D)) 
                     / listCTheta.get(IND_CD);
        System.out.println(" doi: " + doi);
        return doi;
    }
    
     
}
 
