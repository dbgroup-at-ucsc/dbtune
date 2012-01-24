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
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.BIPVariable;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.StringConcatenator;

import ilog.concert.IloException;
import ilog.concert.IloNumVar;
 
 /* 
  * The class is responsible for solving the RestrictIIP problem: For a particular query q, the IIP 
  * problem is to find pairs of indexes (c,d) that interact w.r.t. q; i.e., doi_q(c,d) >= delta.
  * 
  * {\bf Optimize}: The pair of indexes c and d must be relevant to the given input query q 
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
		
    
    public InteractionBIP(double delta)
    {
        this.delta = delta;
        cachedInteractIndexName = new HashMap<String, Integer>();
    }
	/**
	 * Find all pairs of indexes from the given candidate index-set that interact with each other
	 *  
	 * @throws IOException 
	 * @throws IloException 
	 */
    @Override
    public BIPOutput solve() throws SQLException, IOException
    {   
        // Store indexes into the {@code poolIndexes}         
        logger.onLogEvent(LogListener.EVENT_PREPROCESS);
        
        // 1. Communicate with INUM 
        // to derive the query plan description including internal cost, index access cost,
        // index at each slot, etc.  
        this.populatePlanDescriptionForStatements();
        logger.onLogEvent(LogListener.EVENT_INUM_POPULATING);
        
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
        int pos_c, pos_d;
        
        for (int ic = 0; ic < investigatingDesc.getNumberOfSlots(); ic++)    {
            // Note: the last index in each slot is a full table scan
            pos_c = 0;
            for (Index indexc : this.investigatingDesc.getListIndexesWithoutFTSAtSlot(ic)) {
               for (int id = ic; id < investigatingDesc.getNumberOfSlots(); id++){
                   pos_d = 0;
                   for (Index indexd : this.investigatingDesc.getListIndexesWithoutFTSAtSlot(id)) {
                       if (ic == id && pos_c >= pos_d) {   
                           continue;
                       }        
                        
                       if (checkInCache(indexc, indexd) == true){
                            continue;
                       }
                       System.out.println("*** Investigating pair of " + indexc.getFullyQualifiedName()
                                            + " vs. " + indexd.getFullyQualifiedName()
                                            + " using statement " + this.investigatingDesc.getStatementID()
                                            + "****");
                        // 1. initializeBIP()
                        // Set variables correspondingly
                       this.restrictIIP = new RestrictIIPParam(delta, ic, id, indexc, indexd);
                       initializeBuffer();
                        
                        // 2. Construct BIP
                       buildBIP();
                       logger.onLogEvent(LogListener.EVENT_BIP_FORMULATING);
                        
                       // 3. Solve the first BIP
                       if (solveBIP() == true) {
                            this.storeInteractIndexes(indexc, indexd);
                       } else {
                            // formulate the alternative BIP
                           buildAlternativeBIP();
                            
                            // In this case we need to call CPLEX directly
                            if (cplex.solve()){
                                this.storeInteractIndexes(indexc, indexd);
                            } else {
                                System.out.println(" NO INTERACTION ");
                            }
                       } 
                       logger.onLogEvent(LogListener.EVENT_BIP_SOLVING);
                       pos_d++;
                    }
               }
               pos_c++;
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
     * if this problem formulation does not return any solution    * 
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
            CPlexBuffer.concat(this.buf.getLpFileName(), buf.getObjFileName(), buf.getConsFileName(), buf.getBinFileName());
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
                BIPVariable var = poolVariables.createAndStore(theta, IIPVariablePool.VAR_Y, k, 0, 0);
                listVarTheta.add(var.getName());
            }
           
            // var x
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {              
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {        
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        BIPVariable var = poolVariables.createAndStore(theta, IIPVariablePool.VAR_X, k, i, index.getId());
                        listVarTheta.add(var.getName());
                    }
                }
            }       
            
            // var s
            for (Index index : candidateIndexes) {
                BIPVariable var = poolVariables.createAndStore(theta, IIPVariablePool.VAR_S, 0, 0, index.getId());
                listVarTheta.add(var.getName());
            }
            
            // var u
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) {
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        BIPVariable var =  poolVariables.createAndStore(theta, IIPVariablePool.VAR_U, t, i, index.getId()); 
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
        System.out.println(" Number of template plans: " + investigatingDesc.getNumberOfTemplatePlans());
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {         
            String ctheta = "";
            List<String> linListElement = new ArrayList<String>();          
            List<String> linList = new ArrayList<String>();
            
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                String var = poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0, 0).getName();
                String element = Double.toString(investigatingDesc.getInternalPlanCost(k)) + var;
                linList.add(element);
                linListElement.add(element);
                coefVarTheta.get(theta).add(new Double(investigatingDesc.getInternalPlanCost(k)));   
            }
            ctheta  = StringConcatenator.concatenate(" + ", linList);
                        
            // Index access cost
            linList.clear();
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {             
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {                 
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        String var = poolVariables.get(theta, IIPVariablePool.VAR_X, k, i, index.getId()).getName();
                        String element = Double.toString(investigatingDesc.getAccessCost(k, index)) + var;
                        linList.add(element);
                        linListElement.add(element);
                        coefVarTheta.get(theta).add(new Double(investigatingDesc.getAccessCost(k, index)));
                    }
                }
            }       
            ctheta = ctheta + " + " + StringConcatenator.concatenate(" + ", linList);
            
            // Record @ctheta, element of @ctheta, and @variables
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
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                linList.add(poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0, 0).getName());
            }
            buf.getCons().println("atomic_3a_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) + " = 1");
            numConstraints++;
            
            // (3b) \sum_{a \in S+_i \cup I_{\emptyset}} x(theta, k, i, a) = y(theta, k)
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                String var_y = poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0, 0).getName();
                
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    linList.clear();
                    for (Index index : investigatingDesc.getListIndexesAtSlot(i)) {
                        String var_x = poolVariables.get(theta, IIPVariablePool.VAR_X, k, i, index.getId()).getName();
                        linList.add(var_x);
                        String var_s = poolVariables.get(theta, IIPVariablePool.VAR_S, 0, 0, index.getId()).getName();
                        
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
                     || (theta == IND_CD && (i == restrictIIP.getPosRelContainC() || i == restrictIIP.getPosRelContainD()))
                     )
                // special case to handle later    
                {
                    continue;
                }
                
                // - not consider full table scan
                // - it could be possible s^{theta}_{a} = 1 and s^{theta}_{full table scan} = 1
                // - in this case full table scan is used instead of a for the optimal cost
                for (int a = 0; a < investigatingDesc.getListIndexesAtSlot(i).size() - 1; a++) {
                    Index index = investigatingDesc.getListIndexesAtSlot(i).get(a);
                    linList.add(poolVariables.get(theta, IIPVariablePool.VAR_S, 0, 0, index.getId()).getName());      
                }
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
            for (Index index : this.investigatingDesc.getListIndexesWithoutFTSAtSlot(restrictIIP.getPosRelContainC())) {
                if (index.equalsContent(restrictIIP.getIndexC()) == false) {
                    linList.add(poolVariables.get(theta, IIPVariablePool.VAR_S, 0, 0, index.getId()).getName());
                }
            }
            // since we  do not consider FTS, the list can be empty
            if (linList.size() > 0) {
                buf.getCons().println("atomic_4c_" +  numConstraints + ":" 
                                    + StringConcatenator.concatenate(" + ", linList) 
                                    + " <= 1");
                numConstraints++;
            }
        }
        
        
        /**
         * Constraint (6): exception of the relation containing index d 
         */
        
        for (int theta = IND_D; theta <= IND_CD; theta++) {         
            List<String> linList = new ArrayList<String>();
            // not consider full table scan 
            for (Index index : this.investigatingDesc.getListIndexesWithoutFTSAtSlot(restrictIIP.getPosRelContainD())){
                if (index.equalsContent(restrictIIP.getIndexD()) == false) {
                    linList.add(poolVariables.get(theta, IIPVariablePool.VAR_S, 0, 0, index.getId()).getName());
                }
            }
            // since we  do not consider FTS, the list can be empty
            if (linList.size() > 0) {
                buf.getCons().println("atomic_4d_" + numConstraints + ":" 
                                    + StringConcatenator.concatenate(" + ", linList) 
                                    + "  <= 1");
                numConstraints++;
            }
        }
        
        
        /**
         * Constraint (5): constraining for the indexes c and d not appear in Xstar
         */
        buf.getCons().println("atomic_5_" + numConstraints + ":" 
                            + poolVariables.get(IND_EMPTY, IIPVariablePool.VAR_S, 0, 0, restrictIIP.getIndexC().getId()).getName() + " = 0 "); // For s^{empty}_c = 0     
        numConstraints++;
        buf.getCons().println("atomic_5_" + numConstraints + ":" 
                + poolVariables.get(IND_D, IIPVariablePool.VAR_S, 0, 0, restrictIIP.getIndexC().getId()).getName() + " = 0 "); // For s^{d}_c = 0  
        numConstraints++;

        buf.getCons().println("atomic_5_" + numConstraints + ":" 
                            + poolVariables.get(IND_EMPTY, IIPVariablePool.VAR_S, 0, 0, restrictIIP.getIndexD().getId()).getName() + " = 0 "); // For s^{empty}_d = 0
        numConstraints++;       
        buf.getCons().println("atomic_5_" + numConstraints + ":"
                            + poolVariables.get(IND_C, IIPVariablePool.VAR_S, 0, 0, restrictIIP.getIndexD().getId()).getName() + " = 0 "); // For s^{c}_d = 0
        numConstraints++;
    }
    
    /**
     * Build local optimal formula: C^opt_t 
     * Add variables of type VAR_U into the list of variables
     * 
     */
    private void buildLocalOptimal()
    {   
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) {
                List<String> linList = new ArrayList<String>();     
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    for (Index index : this.investigatingDesc.getListIndexesAtSlot(i)) {
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, t, i, index.getId()).getName();       
                        linList.add(Double.toString(investigatingDesc.getAccessCost(t, index)) + var_u);                                              
                    }
                }
                // Constraint (6b)
                buf.getCons().println("optimal_6b_" + numConstraints + ":" 
                        + CTheta.get(theta) + " - " 
                        + StringConcatenator.concatenate(" - ", linList)        
                        + " <=  " + Double.toString(investigatingDesc.getInternalPlanCost(t)));
                numConstraints++;
                
            }
        }
        
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {          
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) {                         
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    for (Index index : this.investigatingDesc.getListIndexesAtSlot(i)) {
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, t, i, index.getId()).getName();                       
                        if ( (theta == IND_C && index.equalsContent(this.restrictIIP.getIndexC()))  
                            || (theta == IND_D &&  index.equalsContent(this.restrictIIP.getIndexD()))
                            || (theta == IND_C && index.equalsContent(this.restrictIIP.getIndexC()))  
                            || (theta == IND_D &&  index.equalsContent(this.restrictIIP.getIndexD()))        
                            )
                        {
                            /*
                            // Constraint (9b) - TODO: don't need this constraint
                            buf.getCons().println("optimal_9b_" + numConstraints + ":" 
                                                + var_u + " <= 1 ");
                            numConstraints++;
                            */
                        } 
                        else if ( (theta == IND_EMPTY && index.equalsContent(this.restrictIIP.getIndexC())) 
                                || (theta == IND_EMPTY && index.equalsContent(this.restrictIIP.getIndexD()))                        
                                || (theta == IND_C && index.equalsContent(this.restrictIIP.getIndexD()))
                                || (theta == IND_D && index.equalsContent(this.restrictIIP.getIndexC()))
                        )
                        {                           
                            buf.getCons().println("optimal_7c_" + numConstraints + ":" 
                                    + var_u + " = 0 ");
                            numConstraints++;
                            
                        }
                        else if (i != restrictIIP.getPosRelContainC() && i != restrictIIP.getPosRelContainD())
                        {                           
                            // Constraint (7b): u^{theta}_{tia} <= sum_{theta}_{a}
                            // TODO: does not constraint the full table scan index ??? 
                            int ga = index.getId();
                            buf.getCons().println("optimal_9a_" + numConstraints + ":" 
                                        + var_u 
                                        + " - " + poolVariables.get(IND_EMPTY, IIPVariablePool.VAR_S, 0, 0, ga)
                                        + " - " + poolVariables.get(IND_C, IIPVariablePool.VAR_S, 0, 0, ga)
                                        + " - " + poolVariables.get(IND_D, IIPVariablePool.VAR_S, 0, 0, ga)
                                        + " - " + poolVariables.get(IND_CD, IIPVariablePool.VAR_S, 0, 0, ga)
                                        + " <= 0 ");
                            numConstraints++;
                        } 
                    }                   
                }           
                        
            }
        }
    }
    
    /**
     * Implement the optimal constraints on variable {@code u} in the RHS of (7a), (8), and (9)
     * 
     */
    private void buidOptimalConstraints()
    {
        // Constraint (7a), (8), (9)
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {             
            for (int t = 0; t < investigatingDesc.getNumberOfTemplatePlans(); t++) {
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    
                    // Sort index access cost
                    List<SortableIndexAcessCost> listSortedIndex  = new ArrayList<SortableIndexAcessCost>();
                    for (Index index : this.investigatingDesc.getListIndexesAtSlot(i)) {
                        SortableIndexAcessCost sac = new SortableIndexAcessCost(investigatingDesc.getAccessCost(t, index), index);
                        listSortedIndex.add(sac);                       
                    }                   
                    // sort in the increasing order of the index access cost
                    Collections.sort(listSortedIndex);
                    List<String> linList = new ArrayList<String>();
                    
                    for (SortableIndexAcessCost sac : listSortedIndex){     
                        Index index = sac.getIndex();
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, t, i, index.getId()).getName();
                        linList.add(var_u);
                        String LHS = StringConcatenator.concatenate(" + ", linList);
                        
                        if ( i == restrictIIP.getPosRelContainC() && index.equalsContent(this.restrictIIP.getIndexC())){
                            // --- \sum >= 1
                            // because this sum is also <= 1 (due to atomic constraint)
                            // therefore, we optimizer to write \sum = 1
                            if (theta == IND_C || theta == IND_CD) {
                                buf.getCons().println("optimal_9_" + numConstraints + ":" 
                                        + LHS  
                                        + " = 1");
                                numConstraints++;
                            }
                        }
                        else if (i == restrictIIP.getPosRelContainD() && index.equalsContent(this.restrictIIP.getIndexD())) {
                            // --- \sum >= 1
                            if (theta == IND_D || theta == IND_CD) {
                                buf.getCons().println("optimal_9_" + numConstraints + ":" 
                                        + LHS  
                                        + " = 1");
                                numConstraints++;
                            }
                        }
                        else {                      
                            for (int thetainternal = IND_EMPTY; thetainternal <= IND_CD; thetainternal++) {
                                // Constraint (8)
                                buf.getCons().println("optimal_8_" + numConstraints + ":" 
                                        + LHS + " - "
                                        + poolVariables.get(thetainternal, IIPVariablePool.VAR_S, 0, 0, index.getId()).getName()
                                        + " >= 0");
                                numConstraints++;
                            }
                        }                                       
                    }   
                    // Constraint (7a) 
                    buf.getCons().println("optimal_7a_" + numConstraints + ":" 
                                        + StringConcatenator.concatenate(" + ", linList) + " = 1");
                    numConstraints++;
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
            realCoef = coef * coefVarTheta.get(IND_CD).get(p);
            listcd.add(Double.toString(realCoef) + element);            
        }
        
        buf.getCons().println("interaction_10a:" + CTheta.get(IND_EMPTY) 
                              + " + " + StringConcatenator.concatenate(" + ", listcd) 
                              + " -  " + StringConcatenator.concatenate(" - ", elementCTheta.get(IND_C))
                              + " -  " + StringConcatenator.concatenate(" - ", elementCTheta.get(IND_D))
                              + " <= 0");
    }
    
    /**
     * Replace the index interaction constraint by the following:
     * cost(X,c) + cost(X,d) - cost(X) - (1-delta) cost(X,c,d)  <= 0
     * <p>
     * <ol> 
     *  <li> Remove the last constraint in the CPEX </li> 
     *  <li> Retrieve the matrix, and assign the value for the coefficient in the matrix</li>
     *  <li> Add the alternative index interaction constraint </li>
     * </ol>
     * </p>
     * @throws IloException 
     * 
     */
    public void buildAlternativeBIP() throws IloException
    {
        matrix = getMatrix(cplex);
        vars = matrix.getNumVars();
        
        // Remove the last constraint for the index interaction
        // and replace by the alternative one
        int last_row_id = matrix.getNrows() - 1;        
        matrix.removeRow(last_row_id);      
        
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
        
        double[] listCoef = new double[vars.length];        
        for (int i = 0; i < vars.length; i++) {
            IloNumVar var = vars[i];
            double coef = 0.0;
            Object found = mapVarCoef.get(var.getName());
            if (found != null) {
                coef = ((Double)found).doubleValue();
            }
                  
            listCoef[i] = coef;
        }
        cplex.addLe(cplex.scalProd(listCoef, vars), 0); 
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
    private void storeInteractIndexes(Index indexc, Index indexd)
    {
        IndexInteraction pairIndexes = new IndexInteraction(indexc, indexd);
        interactionOutput.addPairIndexInteraction(pairIndexes);                               
        String combinedName = indexc.getFullyQualifiedName() + "+" + indexd.getFullyQualifiedName();        
        cachedInteractIndexName.put(combinedName, 1);
        combinedName = indexd.getFullyQualifiedName()+ "+" + indexc.getFullyQualifiedName();        
        cachedInteractIndexName.put(combinedName, 1);
        // TODO: this method might be removed
        computeQueryCostTheta();
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
		if (cachedInteractIndexName.get(key) != null){
			return true;
		}
				
		return false;
	}
		
	
	/**
     * Check the correctness
     */
    private void computeQueryCostTheta()
    {
        Map<String, Double> mapVarVal = new HashMap<String, Double>(); 
        try {
            matrix = getMatrix(cplex);
            vars = matrix.getNumVars();
            System.out.println("Number of vars: " + vars.length);
            
            for (int i = 0; i < vars.length; i++) {
                IloNumVar var = vars[i];
                mapVarVal.put(var.getName(), new Double(cplex.getValue(var)));
            }
        }
        catch (Exception e) {
            System.out.println(" CPLEX error: " + e.getMessage());
        }
        
        for (int theta = IND_EMPTY; theta <= IND_CD; theta++) {                     
            double ctheta = 0.0;        
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {
                String var = poolVariables.get(theta, IIPVariablePool.VAR_Y, k, 0, 0).getName();
                double eleVar = mapVarVal.get(var);
                ctheta += investigatingDesc.getInternalPlanCost(k) * eleVar;             
            }
            
            for (int k = 0; k < investigatingDesc.getNumberOfTemplatePlans(); k++) {             
                for (int i = 0; i < investigatingDesc.getNumberOfSlots(); i++) {
                    for (Index index : this.investigatingDesc.getListIndexesAtSlot(i)) {
                        String var = poolVariables.get(theta, IIPVariablePool.VAR_X, k, i, index.getId()).getName();
                        double eleVar = mapVarVal.get(var);
                        ctheta += investigatingDesc.getAccessCost(k, index) * eleVar;
                    }
                }
            }       
            System.out.println(" Ctheta " + theta + " : " + ctheta);
        }
    }
}
 
