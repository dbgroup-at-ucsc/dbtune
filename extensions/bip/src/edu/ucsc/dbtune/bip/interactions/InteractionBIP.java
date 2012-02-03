package edu.ucsc.dbtune.bip.interactions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.bip.core.BIPVariable;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.StringConcatenator;

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
	public static HashMap<String,Integer> cachedInteractIndexName;	
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
        cachedInteractIndexName = new HashMap<String, Integer>();
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
        // Note: NOT consider FTS
        for (int ic = 0; ic < investigatingDesc.getNumberOfSlots(); ic++)    {  
            
            for (int pos_c = 0; pos_c < investigatingDesc.
                                            getListIndexesWithoutFTSAtSlot(ic).size(); pos_c++){
                
                Index indexc = this.investigatingDesc.
                                            getListIndexesWithoutFTSAtSlot(ic).get(pos_c);
                
                for (int id = ic; id < investigatingDesc.getNumberOfSlots(); id++){
                    
                    for (int pos_d = 0; pos_d < investigatingDesc.
                                            getListIndexesWithoutFTSAtSlot(id).size(); pos_d++){
                        
                        if (ic == id && pos_c >= pos_d)    
                            continue;
                                                
                        Index indexd = investigatingDesc.
                                            getListIndexesWithoutFTSAtSlot(id).get(pos_d);
                        if (checkInCache(indexc, indexd) == true)
                            continue;
                        
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
        }
    }
    
    
    /**
     * The function builds the BIP for the Restrict IIP problem, includes three sets of constraints:
     * <p>
     * <ol> 
     *  <li> Atomic constraints, including (3) - (5) </li> 
     *  <li> Optimal constraints, including (6) - (9) </li>
     *  <li> One alternative of index interaction constraint, (10a) </li>
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
        
        // Construct all variable names
        constructVariables();
        
        // Construct the formula of @C{theta} in (3)
        buildCTheta();
        
        // 1. Atomic constraints (3) - (5)
        buildAtomicConstraints();
        
        // 2. Optimal constraints (6) - (9)
        buildLocalOptimal();
        buidOptimalConstraints();
        
        // 3. Index interaction (10a)
        buildIndexInteractionConstraint1();
        
        // 4. binary variables
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
     * Add all variables into the pool of variables of this BIP formulation
     *  
     */
    private void constructVariables()
    {
        CTheta = new ArrayList<String>();
        elementCTheta = new ArrayList<List<String>>(); 
        varTheta = new ArrayList<List<String>>();
        coefVarTheta = new ArrayList<List<Double>> ();              
        for (int theta = 0; theta < NUM_THETA; theta++) {
            List <Double> coefV = new ArrayList<Double>();
            coefVarTheta.add(coefV);
        }
        poolVariables = new IIPVariablePool();
        
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
     * Construct the formula of C{theta} in (3)
     * 
     * C(theta)=  sum_{k \in [1, Kq] } beta_k y_{k,theta}
     *         + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x_{kia, theta}
     *
     */
    private void buildCTheta()
    {   
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
            ctheta  = StringConcatenator.concatenate(" + ", linList);
                        
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
            ctheta = ctheta + " + " + StringConcatenator.concatenate(" + ", linList);
            
            /** Record {@code ctheta}, element of {@code ctheta} */
            CTheta.add(ctheta);         
            elementCTheta.add(linListElement);
        }
    }
        
    /**
     * Atomic constraints: (3) - (5)
     * 
     */
    private void buildAtomicConstraints()
    {   
        // Constraint (3): atomic configuration for each C{theta} 
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            
            List<String> linList = new ArrayList<String>();
            // (3a) \sum_{k \in [1, Kq]}y^{theta}_k = 1
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++)
                linList.add(poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0).getName());
            
            buf.getCons().println("atomic_3a_" + numConstraints + ": " + 
                                   StringConcatenator.concatenate(" + ", linList) + " = 1");
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
                                            + ": " + StringConcatenator.concatenate(" + ", linList) 
                                            + " - " + var_y
                                            + " = 0");
                    numConstraints++;
                }
            }       
        }
        
        /**
         * Constraint (4b): A_{\theta} is atomic 
         * 
         */
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
                            + StringConcatenator.concatenate(" + ", linList) 
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
                                    + StringConcatenator.concatenate(" + ", linList) 
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
                                    + StringConcatenator.concatenate(" + ", linList) 
                                    + "  <= 1");
                numConstraints++;
            }
        }
        
        /**
         * Constraint (5): constraining for the indexes c and d not appear in X
         */
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
     * Build local optimal formula: C^opt_t 
     * Add variables of type VAR_U into the list of variables
     * 
     */
    private void buildLocalOptimal()
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
                        + StringConcatenator.concatenate(" - ", linList)        
                        + " <=  " + investigatingDesc.getInternalPlanCost(t));
                numConstraints++;
            }
        }
        
        // atomic constraint
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
                                        + StringConcatenator.concatenate(" + ", linList) + " = 1");
                    numConstraints++;
                }
            }
        }
        
        // constraint on the domain of VAR_U variables
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
     * Implement the optimal constraints on variable of type {@code VAR_U}
     * 
     */
    private void buidOptimalConstraints()
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
                        String LHS = StringConcatenator.concatenate(" + ", linList);
                        
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
     * Build the first index interaction constraint (13) (more often)
     * cost(X) + (1 + delta) cost(X, c,d ) - cost(X,c) - cost(X,d) <= 0
     *      
     */

    private void buildIndexInteractionConstraint1()
    {    
        ArrayList<String> listcd = new ArrayList<String>();
        double coef = 1 + restrictIIP.getDelta(), realCoef;     
        
        for (int p = 0; p < coefVarTheta.get(IND_CD).size(); p++) {
            
            String element = varTheta.get(IND_CD).get(p);
            // realCoef is supposed to be positive
            realCoef = coef * coefVarTheta.get(IND_CD).get(p);
            listcd.add(realCoef + element);            
        }
        
        buf.getCons().println("interaction_8a:" + CTheta.get(IND_EMPTY) 
                              + " + " + StringConcatenator.concatenate(" + ", listcd) 
                              + " -  " + StringConcatenator.concatenate(" - ", 
                                                                        elementCTheta.get(IND_C))
                              + " -  " + StringConcatenator.concatenate(" - ", 
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
        Map<String,Double> mapVarCoef = new HashMap<String, Double>();
        double deltaCD = restrictIIP.getDelta() - 1;   
        
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            List<String> listVar = varTheta.get(theta); 
            for (int p = 0; p < listVar.size(); p++) {
                BIPVariable var = poolVariables.get(listVar.get(p));
                
                double coef = 0.0;
                
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
        String combinedName = indexc.getFullyQualifiedName() + "+" + indexd.getFullyQualifiedName();        
        cachedInteractIndexName.put(combinedName, 1);
        combinedName = indexd.getFullyQualifiedName()+ "+" + indexc.getFullyQualifiedName();        
        cachedInteractIndexName.put(combinedName, 1);
    }
	
	
	/**
	 * Checking whether pair of indexes (indexc, indexd) have been in the cache
	 * 
	 * @param indexc, indexd
	 * 		The two given indexes
	 * @param cachedIndexes
	 * 		The hash map containing the cache
	 * 
	 * @return {@code true/false}
	 */
	private boolean checkInCache(Index indexc, Index indexd)
	{
		String key = indexc.getFullyQualifiedName() + "+" + indexd.getFullyQualifiedName();
		if (cachedInteractIndexName.get(key) != null)
			return true;
				
		return false;
	}
		
	
	/**
     * Check the correctness
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
 
