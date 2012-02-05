package edu.ucsc.dbtune.bip.interactions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.bip.core.BIPVariable;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import ilog.concert.IloException;
 
/**
 * The class is responsible for solving the RestrictIIP problem: 
 * Given a query q, the Index Interaction Problem (IIP) finds pairs of indexes (c,d) 
 * that interact with respect to q; i.e., doi_q(c,d) >= delta.
 *  
 * @author Quoc Trung Tran
 *
 */
public class InteractionBIP extends AbstractBIPSolver
{	
	public static HashMap<String,Integer> pairInteractingIndexNames;	
	public static final int NUM_THETA = 4;
    public static final int IND_EMPTY = 0;
    public static final int IND_C = 1;
    public static final int IND_D = 2;
    public static final int IND_CD = 3;
     
    private List<String> CTheta;
    private List<List<String>> elementCTheta;
    private List<List<String>> varTheta;
    private List<List<Double>> coefVarTheta;
    private IIPVariablePool poolVariables;
    private RestrictIIPParam restrictIIP;
    
    private InteractionOutput interactionOutput;
    private QueryPlanDesc investigatingDesc;
    private double delta;
	private CPlexInteraction cplex;	
    
    public InteractionBIP(double delta)
    {
        this.delta = delta;
        pairInteractingIndexNames = new HashMap<String, Integer>();
        cplex = new CPlexInteraction();
    }
	
    
    @Override
    public BIPOutput solve() throws SQLException, IOException
    {   
        // 1. Communicate with INUM 
        // to derive the query plan description including internal cost, index access cost,
        // index at each slot, etc.  
        this.populatePlanDescriptionForStatements();
        logger.onLogEvent(LogListener.EVENT_POPULATING_INUM);
        
        // 2. Iterate over the list of query plan descs that have been derived
        interactionOutput = new InteractionOutput();
        try {
            for (QueryPlanDesc desc :  listQueryPlanDescs) {
                investigatingDesc = desc;
                findInteractions();
            }
        } catch (IloException e) {
            throw new RuntimeException(e);
        }
        return getOutput();
    }
    
    
    @Override
    protected BIPOutput getOutput() 
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
        for (int pos_c = 0; pos_c < indexes.size(); pos_c++) {
            indexc = indexes.get(pos_c);
            ic = mapIndexSlotID.get(indexc);
            
            for (int pos_d = pos_c + 1; pos_d < indexes.size(); pos_d++) {
                indexd = indexes.get(pos_d);
                if (isInteracting(indexc, indexd) == true)
                    continue;
                
                id = mapIndexSlotID.get(indexc);
                System.out.println("*** Investigating pair of " + indexc
                                    + " vs. " + indexd                                            
                                    + " using statement "  
                                    + investigatingDesc.getStatementID()
                                    + "****");
                // initialize an instance of RestrictIIP problem
                restrictIIP = new RestrictIIPParam(delta, ic, id, indexc, indexd);
                
                // 2. Construct BIP
                initializeBuffer();
                buildBIP();
                logger.onLogEvent(LogListener.EVENT_FORMULATING_BIP);
                
                // 3. Solve the first BIP
                // TODO: for debug only
                Map<String, Integer> mapVarVal = cplex.solve(buf.getLpFileName());                
                if (mapVarVal!= null) {
                    storeInteractingIndexes(indexc, indexd);
                    computeQueryCostTheta(mapVarVal);
                } else {
                    // formulate the alternative BIP
                   if (solveAlternativeBIP() == true)
                       storeInteractingIndexes(indexc, indexd);
                   else 
                        System.out.println(" NO INTERACTION ");
                    
                } 
                logger.onLogEvent(LogListener.EVENT_SOLVING_BIP);
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
        selectingAtEachSlot();
        
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
     */
    private void constructVariables()
    {   
        poolVariables = new IIPVariablePool();
        varTheta = new ArrayList<List<String>>();
        
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            
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
            varTheta.add(listVarTheta);
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
        CTheta = new ArrayList<String>();
        elementCTheta = new ArrayList<List<String>>();
        coefVarTheta = new ArrayList<List<Double>> ();              
        for (int theta = 0; theta < NUM_THETA; theta++) {
            List <Double> coefV = new ArrayList<Double>();
            coefVarTheta.add(coefV);
        }
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {         
            String ctheta = "";
            List<String> linListElement = new ArrayList<String>();          
            List<String> linList = new ArrayList<String>();
            
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                
                String var = poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0).getName();
                String element = investigatingDesc.getInternalPlanCost(k) + var;
                linList.add(element);
                linListElement.add(element);
                coefVarTheta.get(theta).add(investigatingDesc.getInternalPlanCost(k));   
            }
            ctheta  = Strings.concatenate(" + ", linList);
                        
            // Index access cost
            linList.clear();
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        
                        String var = poolVariables.get(theta, IIPVariablePool.VAR_X, 
                                                        k, index.getId()).getName();
                        String element = investigatingDesc.getAccessCost(k, index) + var;
                        linList.add(element);
                        linListElement.add(element);
                        coefVarTheta.get(theta).add(investigatingDesc.getAccessCost(k, index));
                    }
                }
            }       
            ctheta = ctheta + " + " + Strings.concatenate(" + ", linList);
            
            /** Record {@code ctheta}, element of {@code ctheta} */
            CTheta.add(ctheta);         
            elementCTheta.add(linListElement);
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
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            
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
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            
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
        
        for (int theta = IND_C; theta <= IND_CD; theta++) {   
            
            if (theta == IND_D) { 
                continue;
            }
            
            List<String> linList = new ArrayList<String>();
            // not consider full table scan
            // and exclude {@code indexc}
            for (Index index : investigatingDesc.getListIndexesWithoutFTSAtSlot
                                    (            restrictIIP.getPosRelContainC())) {
                
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
        
        
        for (int theta = IND_D; theta <= IND_CD; theta++) {     
            
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
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) {
                
                List<String> linList = new ArrayList<String>();     
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    for (Index index : this.investigatingDesc.getListIndexesAtSlot(i)) {
                        
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
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            
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
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            
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
                                        + " - " + poolVariables.get(IND_C, 
                                                                    IIPVariablePool.VAR_S, 0, ga)
                                        + " - " + poolVariables.get(IND_D, 
                                                                    IIPVariablePool.VAR_S, 0, ga)
                                        + " - " + poolVariables.get(IND_CD, 
                                                                    IIPVariablePool.VAR_S, 0, ga)
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
    private void selectingAtEachSlot()
    {  
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {    
            
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
                    
                    // sort in the increasing order of the index access cost
                    Collections.sort(listSortedIndex);
                    List<String> linList = new ArrayList<String>();
                    
                    for (SortableIndexAcessCost sac : listSortedIndex){  
                        
                        Index index = sac.getIndex();
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, t, 
                                                        index.getId()).getName();
                        linList.add(var_u);
                        String LHS = Strings.concatenate(" + ", linList);
                        
                        if (index.equals(this.restrictIIP.getIndexC())){
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
                        else if (index.equals(this.restrictIIP.getIndexD())) {
                            // --- \sum >= 1
                            if (theta == IND_D || theta == IND_CD) {
                                buf.getCons().println("optimal_7d_" + numConstraints + ":" 
                                        + LHS  
                                        + " = 1");
                                numConstraints++;
                            }
                        }
                        else {                
                            
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
        
        for (int p = 0; p < coefVarTheta.get(IND_CD).size(); p++) {
            
            String element = varTheta.get(IND_CD).get(p);
            // realCoef is supposed to be positive
            realCoef = coef * coefVarTheta.get(IND_CD).get(p);
            listcd.add(realCoef + element);            
        }
        
        buf.getCons().println("interaction_8a:" 
                              + CTheta.get(IND_EMPTY) 
                              + " + " + Strings.concatenate(" + ", listcd) 
                              + " - " + Strings.concatenate(" - ", 
                                                                        elementCTheta.get(IND_C))
                              + " - "  + Strings.concatenate(" - ", 
                                                                        elementCTheta.get(IND_D))
                              + " <= 0");
    }
    
    /**
     * Replace the index interaction constraint by the following:
     * cost(X,c) + cost(X,d) - cost(X) - (1-delta) cost(X,c,d)  <= 0
     * 
     */
    public boolean solveAlternativeBIP()
    {   
        System.out.println(" L715, solve alternative \n");
        Map<String,Double> mapVarCoef = new HashMap<String, Double>();
        double deltaCD = restrictIIP.getDelta() - 1; 
        double coef;
        
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            List<String> listVar = varTheta.get(theta); 
            for (int p = 0; p < listVar.size(); p++) {
                BIPVariable var = poolVariables.get(listVar.get(p));
                coef = 0.0;
                
                if (var.getType() == IIPVariablePool.VAR_X ||
                    var.getType() == IIPVariablePool.VAR_Y){
                    double existingCoef = coefVarTheta.get(theta).get(p);
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
                }
                
                mapVarCoef.put(var.getName(), new Double(coef));                
            }           
        }
        
        Map<String, Integer> mapVarVal = cplex.solveAlternativeInteractionConstraint(mapVarCoef);
        if (mapVarVal != null) {
            computeQueryCostTheta(mapVarVal);
            return true;
        } else 
            return false;
        
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
    private void storeInteractingIndexes(Index indexc, Index indexd)
    {
        IndexInteraction pairIndexes = new IndexInteraction(indexc, indexd);
        interactionOutput.addPairIndexInteraction(pairIndexes);                               
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
     * Print the actual query execution cost derived by the CPLEX solver.
     * This method is implemented to check the correctness of our BIP solution. 
     */
    private void computeQueryCostTheta(Map<String, Integer> mapVarVal)
    {
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) { 
            
            double ctheta = 0.0;        
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                
                String var = poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0).getName();
                double eleVar = mapVarVal.get(var);
                ctheta += investigatingDesc.getInternalPlanCost(k) * eleVar;             
            }
            
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    for (Index index : this.investigatingDesc.getListIndexesAtSlot(i)) {
                        
                        String var = poolVariables.get(theta, IIPVariablePool.VAR_X, 
                                                        k, index.getId()).getName();
                        double eleVar = mapVarVal.get(var);
                        ctheta += investigatingDesc.getAccessCost(k, index) * eleVar;
                    }
                }
            }       
            System.out.println(" Ctheta " + theta + " : " + ctheta);
        }
    }
}
 
