package edu.ucsc.dbtune.bip.interactions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.caliper.internal.guava.collect.Sets;
import edu.ucsc.dbtune.bip.core.BIPVariable;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * The class implements the BIP to solve an instance of a RestrictIIP problem:
 * <p>
 * <ol>
 *      <li> The threshold delta to determine the interaction; i.e., if doi_q(c,d) >= delta, 
 *      then c and d interact with each other. </li>
 *      <li> The relation identifier that contains index c (resp. d)  </li>
 *      <li> Indexes that are being investigated 
 * </ol>
 * </p>

 * @author Quoc Trung Tran 
 *
 */
public class RestrictIIP 
{
    public static final int IND_EMPTY = 0;
    public static final int IND_C     = 1;
    public static final int IND_D     = 2;
    public static final int IND_CD    = 3;
    
    protected final List<Integer> listTheta = 
                    Arrays.asList(IND_EMPTY, IND_C, IND_D, IND_CD);     
    
    protected Map<Integer, String>       CTheta;
    protected Map<Integer, List<String>> elementCTheta;    
    protected Map<Integer, List<String>> varTheta;
    protected Map<String, Double>        coefVarYXTheta;
    protected IIPVariablePool            poolVariables;    
    protected Map<String, Index>         mapVarSIndex;
    
    protected QueryPlanDesc    desc;
    protected CPlexBuffer      buf;
    protected CPlexInteraction cplex;
    protected Set<Index>       candidateIndexes;
    
	protected double delta;		
	protected int    ic;
	protected int    id; 
	protected Index  indexc;
	protected Index  indexd;
	protected int    numConstraints;
	protected double doiOptimizer;
	protected double doiBIP;
	
	protected LogListener logger;
	protected Environment environment = Environment.getInstance();
	
	
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
	public RestrictIIP( final QueryPlanDesc desc, 
	                    final LogListener logger, 
	                    final double delta, final Index indexc, final Index indexd, 
	                    final Set<Index> candidateIndexes,
	                    final int ic, final int id)
	{
		this.delta            = delta;
		this.ic               = ic;
		this.id               = id;
		this.indexc           = indexc;
		this.indexd           = indexd;
		this.desc             = desc;
		this.logger           = logger;
		this.candidateIndexes = candidateIndexes;
		
		this.cplex            = new CPlexInteraction();
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
	 */
	public boolean solve(SQLStatement sql, Optimizer optimizer)
	{
	    // 2. Construct BIP
        logger.setStartTimer();
        initializeBuffer();
        buildBIP();
        logger.onLogEvent(LogListener.EVENT_FORMULATING_BIP);
        
        // 3. Solve the first BIP        
        logger.setStartTimer();
        boolean isInteracting          = false;
        boolean isSolveAlternativeOnly = false;
        
        // 
        // One optimization.
        //
        // If indexes belong the same relation then the first index interaction constraint
        // is false, since Aempty + Acd >= Ac + Ad. 
        // 
        // We invoke the alternative interaction constraint instead.
        // 
        if (ic != id)           
            isInteracting = cplex.solve(buf.getLpFileName());
        else
            isSolveAlternativeOnly = true;
        
        if (!isInteracting)
           // formulate the alternative BIP
            isInteracting = solveAlternativeBIP(isSolveAlternativeOnly, buf.getLpFileName()); 
        
        // clear the model
        cplex.clearModel();
        
        logger.onLogEvent(LogListener.EVENT_SOLVING_BIP);
        /*
        if (isInteracting) {
            doiBIP = computeDoi(mapVarVal);
            doiOptimizer = computeDoiOptimizer(mapVarVal, sql, optimizer);
        }
        */
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
	    return indexc;
	}
	
	/**
	 * Retrieve the second of index in the given pair of index
	 * 
	 * @return
	 *     an index object
	 */
	public Index getSecondIndex()
	{
	    return indexd; 
	}
	
	/**
     * Initialize empty buffer files that will store the Binary Integer Program
     *  
     *      
     */
    protected void initializeBuffer()
    { 
        String prefix = "wl.sql";
        String name = environment.getTempDir() + "/" + prefix;
        try {
            buf = new CPlexBuffer(name);
        }
        catch (IOException e) {
            System.out.println(" Error in opening files " + e.toString());          
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
     * <b> Note </b>: The alternative index interaction constraint is considered only 
     * if this problem formulation does not return any solution     
     */
    protected void buildBIP() 
    {
        numConstraints = 0;
        
        // Construct variable
        constructBinaryVariables();
        
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
        
        try {
            buf.writeToLpFile();
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
    protected void constructBinaryVariables()
    {   
        poolVariables = new IIPVariablePool();
        varTheta      = new HashMap<Integer, List<String>>();
        mapVarSIndex  = new HashMap<String, Index>();        
        int q         = desc.getStatementID();
        
        for (int theta : listTheta) {            
            
            List<String> listVarTheta = new ArrayList<String>();
            
            // var y
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                
                BIPVariable var = poolVariables.createAndStore(theta, IIPVariablePool.VAR_Y, q, k, 0);
                listVarTheta.add(var.getName());
            }
           
            // var x
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {    
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        
                        BIPVariable var = poolVariables.createAndStore
                                                   (theta, IIPVariablePool.VAR_X, q, k, index.getId());
                        listVarTheta.add(var.getName());
                    }
                }
            }       
            
            // var s
            for (Index index : candidateIndexes) {
                
                BIPVariable var = poolVariables.createAndStore
                                                (theta, IIPVariablePool.VAR_S, q, 0, index.getId());
                listVarTheta.add(var.getName());
                mapVarSIndex.put(var.getName(), index);
            }
            
            // var u
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        
                        BIPVariable var =  poolVariables.createAndStore(theta, 
                                                         IIPVariablePool.VAR_U, q, t, index.getId()); 
                        listVarTheta.add(var.getName());
                    }
                }
            }
            
            // record the list of variables of each theta
            varTheta.put(theta, listVarTheta);
        }
    }
    
    /**
     * The method clears all data structures
     */
    public void clear()
    {
        CTheta.clear();
        elementCTheta.clear();    
        varTheta.clear();
        coefVarYXTheta.clear();
        poolVariables.clear();    
        mapVarSIndex.clear();
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
    protected void buildQueryExecutionCost()
    {   
        int    q;
        double cost;
        String element;
        
        CTheta         = new HashMap<Integer, String>();
        elementCTheta  = new HashMap<Integer, List<String>>();
        coefVarYXTheta = new HashMap<String, Double>(); 
        q              = desc.getStatementID();
        
        for (int theta : listTheta) {
            
            String ctheta = "";
            List<String> linListElement = new ArrayList<String>();          
            List<String> linList = new ArrayList<String>();
            
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                
                String var = poolVariables.get(theta, IIPVariablePool.VAR_Y, q, k, 0).getName();
                cost = desc.getInternalPlanCost(k);
                element = cost + var;
                linList.add(element);
                linListElement.add(element);
                coefVarYXTheta.put(var, cost);   
            }
            ctheta  = Strings.concatenate(" + ", linList);
                        
            // Index access cost
            linList.clear();
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        
                        String var = poolVariables.get(theta, IIPVariablePool.VAR_X, 
                                                       q, k, index.getId()).getName();
                        cost = desc.getAccessCost(k, index);
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
    protected void atomicConstraintForINUM()
    {
        int q = desc.getStatementID();
        
        for (int theta : listTheta) {
            
            List<String> linList = new ArrayList<String>();
            // (3a) \sum_{k \in [1, Kq]}y^{theta}_k = 1
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                linList.add(poolVariables.get(theta, IIPVariablePool.VAR_Y, q, k, 0).getName());
            
            buf.getCons().add("atomic_3a_" + numConstraints + ": " + 
                    Strings.concatenate(" + ", linList) + " = 1");
            numConstraints++;
            
            // (3b) \sum_{a \in S+_i \cup I_{\emptyset}} x(theta, k, i, a) = y(theta, k)
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                
                String var_y = poolVariables.get(theta, IIPVariablePool.VAR_Y, q, k, 0).getName();
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    linList.clear();
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        String var_x = poolVariables.get(theta, IIPVariablePool.VAR_X, 
                                                         q, k, index.getId()).getName();
                        linList.add(var_x);
                        String var_s = poolVariables.get(theta, IIPVariablePool.VAR_S, 
                                                         q, 0, index.getId()).getName();
                        
                        // (4a) s_a^{theta} \geq x_{kia}^{theta}
                        buf.getCons().add("atomic_4a_" + numConstraints + ":" 
                                            + var_x + " - " 
                                            + var_s
                                            + " <= 0 ");
                        numConstraints++;
                    }
                    buf.getCons().add("atomic_3b_" + numConstraints  
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
    protected void atomicConstraintAtheta()
    {
        int q = desc.getStatementID();
        
        for (int theta : listTheta) {
            
            for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                
                List<String> linList = new ArrayList<String>();
                if ( (theta == IND_C && i == ic) 
                     || (theta == IND_D && i == id)
                     || (theta == IND_CD && i == ic)
                     || (theta == IND_CD && i == id)                     
                     )
                // special case to handle later
                    continue;
                
                
                // - not consider full table scan
                // - it could be possible s^{theta}_{a} = 1 and s^{theta}_{full table scan} = 1
                // - in this case full table scan is used instead of a for the optimal cost
                for (Index index : desc.getIndexesWithoutFTSAtSlot(i))
                    linList.add(poolVariables.get(theta, IIPVariablePool.VAR_S, q, 0, index.getId())
                                             .getName());      
                
                if (linList.size() > 0){
                    buf.getCons().add("atomic_4b_" +  numConstraints + ":" 
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
            for (Index index : desc.getIndexesWithoutFTSAtSlot(ic)) {
                
                if (!index.equals(indexc)) 
                    linList.add(poolVariables.get(theta, IIPVariablePool.VAR_S, q, 0, index.getId())
                                             .getName());
                
            }
            // since we  do not consider FTS, the list can be empty
            if (linList.size() > 0) {
                buf.getCons().add("atomic_4b_" +  numConstraints + ":" 
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
            for (Index index : desc.getIndexesWithoutFTSAtSlot(id)){
                
                if (!index.equals(indexd)) 
                    linList.add(poolVariables.get(theta, IIPVariablePool.VAR_S, q, 0, index.getId())
                                             .getName());
                
            }
            // since we  do not consider FTS, the list can be empty
            if (linList.size() > 0) {
                buf.getCons().add("atomic_4b_" + numConstraints + ":" 
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
    protected void interactionPrecondition()
    {   
        int q = desc.getStatementID();
        
        buf.getCons().add("atomic_4c_" + numConstraints + ":" 
                            + poolVariables.get(IND_EMPTY, IIPVariablePool.VAR_S, q, 0, 
                                                indexc.getId()).getName() 
                                                + " = 0 "); // For s^{empty}_c = 0     
        numConstraints++;
        buf.getCons().add("atomic_4c_" + numConstraints + ":" 
                            + poolVariables.get(IND_D, IIPVariablePool.VAR_S, q, 0, 
                                                indexc.getId()).getName() 
                                                + " = 0 "); // For s^{d}_c = 0  
        numConstraints++;

        buf.getCons().add("atomic_4c_" + numConstraints + ":" 
                            + poolVariables.get(IND_EMPTY, IIPVariablePool.VAR_S, q, 0, 
                                                indexd.getId()).getName() 
                                                + " = 0 "); // For s^{empty}_d = 0
        numConstraints++;       
        buf.getCons().add("atomic_4c_" + numConstraints + ":"
                            + poolVariables.get(IND_C, IIPVariablePool.VAR_S, q, 0, 
                                                indexd.getId()).getName() 
                                                + " = 0 "); // For s^{c}_d = 0
        numConstraints++;
    }
    
    
    
    /**
     * This set of constraints ensure {@code cost(q, Atheta \cup Atheta} is 
     * not greater than the local optimal cost of using any template plan.  
     * 
     */
    protected void localOptimal()
    {   
        int q = desc.getStatementID();
        
        // construct C^opt_t
        for (int theta : listTheta) {
            
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
                
                List<String> linList = new ArrayList<String>();     
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, q, t, 
                                                        index.getId()).getName();       
                        linList.add(desc.getAccessCost(t, index) + var_u);                                              
                    }
                }
                // Constraint (5)
                buf.getCons().add("optimal_5_" + numConstraints + ":" 
                        + CTheta.get(theta) + " - " 
                        + Strings.concatenate(" - ", linList)        
                        + " <=  " + desc.getInternalPlanCost(t));
                numConstraints++;
            }
        }
    }
    
    /**
     * The set of atomic constraints on variable of type {@code VAR_U}
     */
    protected void atomicConstraintLocalOptimal()
    {
        int q = desc.getStatementID();
        
        for (int theta : listTheta) {
            
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    List<String> linList = new ArrayList<String>();
                    for (Index index : desc.getIndexesAtSlot(i)) {
                        
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, q, t, 
                                                        index.getId()).getName();       
                        linList.add(var_u);                                              
                    }
                    // Atomic constraint (6a) 
                    buf.getCons().add("optimal_6a_" + numConstraints + ":" 
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
    protected void presentVariableLocalOptimal()
    {   
        int q = desc.getStatementID();
        
        for (int theta : listTheta) {
            
            for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) { 
                
                for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                    
                    // no need to constraint FTS
                    // since u_{FTS} <= 1
                    for (Index index : desc.getIndexesWithoutFTSAtSlot(i)) {
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, q, t, 
                                                        index.getId()).getName();                       
                        if ( (theta == IND_C && index.equals(indexc))  
                            || (theta == IND_D &&  index.equals(indexd))
                            || (theta == IND_CD && index.equals(indexc))  
                            || (theta == IND_CD &&  index.equals(indexd))        
                            )
                        {
                            // u^{theta}_{c,d} <= 1 (it is implies by binary variable constraint)
                        } 
                        else if ( (theta == IND_EMPTY && index.equals(indexc)) 
                                || (theta == IND_EMPTY && index.equals(indexd))                        
                                || (theta == IND_C && index.equals(indexd))
                                || (theta == IND_D && index.equals(indexc))
                        )
                        {                           
                            buf.getCons().add("optimal_6c_" + numConstraints + ":" 
                                                 + var_u + " = 0 ");
                            numConstraints++;
                            
                        }                        
                        else {                           
                            // Constraint (6b): u^{theta}_{tia} <= sum_{theta}_{a}                             
                            int ga = index.getId();
                            buf.getCons().add("optimal_6b_" + numConstraints + ":" 
                                        + var_u 
                                        + " - " + poolVariables.get(IND_EMPTY, 
                                                                    IIPVariablePool.VAR_S, q, 0, ga)
                                                                    .getName()
                                        + " - " + poolVariables.get(IND_C, 
                                                                    IIPVariablePool.VAR_S, q, 0, ga)
                                                                    .getName()
                                        + " - " + poolVariables.get(IND_D, 
                                                                    IIPVariablePool.VAR_S, q, 0, ga)
                                                                    .getName()
                                        + " - " + poolVariables.get(IND_CD, 
                                                                    IIPVariablePool.VAR_S, q, 0, ga)
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
    protected void selectingIndexAtEachSlot()
    {  
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
                    List<String> linList = new ArrayList<String>();
                    
                    for (SortableIndexAcessCost sac : listSortedIndex){  
                        
                        Index index = sac.getIndex();
                        String var_u = poolVariables.get(theta, IIPVariablePool.VAR_U, q, t, 
                                                        index.getId()).getName();
                        linList.add(var_u);
                        String LHS = Strings.concatenate(" + ", linList);
                        
                        if (index.equals(indexc)){
                            // --- \sum >= 1 (for the case of theta = IND_C || IND_D
                            // because this sum is also <= 1 (due to atomic constraint)
                            // therefore, we optimizer to write \sum = 1
                            if (theta == IND_C || theta == IND_CD) {
                                buf.getCons().add("optimal_7c_" + numConstraints + ":" 
                                                       + LHS  
                                                       + " = 1");
                                numConstraints++;
                            }
                        }
                        else if (index.equals(indexd)) {
                            // --- \sum >= 1
                            if (theta == IND_D || theta == IND_CD) {
                                buf.getCons().add("optimal_7d_" + numConstraints + ":" 
                                        + LHS  
                                        + " = 1");
                                numConstraints++;
                            }
                        }
                        else {                
                            // Full table scan: must use
                            if (index.getId() == idFTS) {
                                
                                buf.getCons().add("optimal_7_FTS_" + numConstraints + ":" 
                                        + LHS + " = 1");
                                numConstraints++;
                            } else {
                                
                                for (int thetainternal = IND_EMPTY; thetainternal <= IND_CD; 
                                     thetainternal++) {
                                    // Constraint (8)
                                    buf.getCons().add("optimal_7_" + numConstraints + ":" 
                                            + LHS + " - "
                                            + poolVariables.get(thetainternal, IIPVariablePool.VAR_S, 
                                                            q, 0, index.getId()).getName()
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
    protected void indexInteractionConstraint1()
    {    
        ArrayList<String> listcd = new ArrayList<String>();
        double coef, realCoef;
        coef = 1 + delta;
        Object found;
        for (String var : varTheta.get(IND_CD)) {
            found = coefVarYXTheta.get(var);
            if (found != null) {
                realCoef = (Double) found * coef;
                listcd.add(realCoef + var);
            }   
        }
        
        buf.getCons().add("interaction_8a:" 
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
     *      {@code true} if CPLEX returns a solution,
     *      {@code false} otherwise
     */
    protected boolean solveAlternativeBIP(boolean isSolveAlternativeOnly, String inputFile)
    {   
        Map<String,Double> mapVarCoef = new HashMap<String, Double>();
        double deltaCD = delta - 1; 
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
        
        return cplex.solveAlternativeInteractionConstraint(mapVarCoef, 
                                                           isSolveAlternativeOnly,
                                                           inputFile);
    }
    
    /**
     * Output the list of variables into @bin file to constraint
     * binary variables
     * 
     */
    protected void binaryVariableConstraints()
    {
        buf.getBin().add(poolVariables.enumerateList(10));
    }
    
    /**
     * Compute the actual query execution cost derived by the CPLEX solver.
     * This method is implemented to check the correctness of our BIP solution. 
     */
    protected double computeDoi(Map<String, Integer> mapVarVal)
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
            
            listCTheta.add(ctheta);
        }
        
        double doi = Math.abs(listCTheta.get(IND_EMPTY) + listCTheta.get(IND_CD)
                              - listCTheta.get(IND_C) - listCTheta.get(IND_D)) 
                     / listCTheta.get(IND_CD);
        
        return doi;
    }
    
    /**
     * This function computes the degree of interaction from the actual
     * optimizer
     * 
     * @param mapVarVal
     *     The result from CPLEX solver that maps variables to their assigned values
     * @param sql
     *     The SQLStatement to compute doi       
     * @param optimizer
     *     A conventional optimizer (e.g., DB2Optimizer)
     * 
     */
    protected double computeDoiOptimizer(Map<String, Integer> mapVarVal,
                                       SQLStatement sql,
                                       Optimizer optimizer)
    {   
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
            e.printStackTrace();
        }
        
        return ioo.getDoiOptimizer();
    }
}

