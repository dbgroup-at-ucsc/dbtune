package edu.ucsc.dbtune.bip.sim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.Connector;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.bip.util.LogListener;


public class SimLinGenerator 
{
    public static final int  VAR_Y = 0;
	public static final int  VAR_X = 1;
	public static final int  VAR_S = 2;
	public static final int  VAR_PRESENT = 3;
	public static final int  VAR_CREATE = 4;
	public static final int  VAR_DROP = 5;
	public static final int  VAR_DEFAULT = 100;

	private CPlexBuffer buf;
    private String[] strHeaderVariable = {"y", "x", "s", "present", "create", "drop"};
	private List<String> listCwq;
	private List<String> listVar;
	private List<SimQueryPlanDesc> listQueryPlan;
	private int W;
	private List<Double> B;
	private int numConstraints;
	
	
	SimLinGenerator(final String prefix, final List<SimQueryPlanDesc> listQueryPlan, final int W, final List<Double> B)
	{		
		this.listQueryPlan = listQueryPlan;
		this.W = W;
		this.B = B;
		
		listCwq = new ArrayList<String>();	
		listVar = new ArrayList<String>();
		
		try {
			this.buf = new CPlexBuffer(prefix);
		}
		catch (IOException e) {
			System.out.println(" Error in opening files " + e.toString());			
		}
	}
	
	/**
	 * The function builds the BIP for the SIM problem, includes three sets of constraints:
	 * <p>
	 * <ol> 
	 * 	<li> Atomic constraints </li> 
	 * 	<li> Well-behaved schedule constraints </li>
	 * 	<li> Index present constraints </li>
	 *  <li> Space constraints </li>
	 * </ol>
	 * </p>	  
	 * 
	 * 
	 * @param listener
	 * 		Log the building process
	 * 
	 * @throws IOException
	 */
	public final void build(final LogListener listener) throws IOException 
	{
    	listener.onLogEvent(LogListener.BIP, "Building IIP program...");
    	numConstraints = 0;
    	
    	// Construct the query cost at each window time
    	buildQueryCostWindow();
    	
    	// 1. Atomic constraints
    	buildAtomicConstraints();    	
    	
    	// 2. Each index is created/dropped one time
    	buildWellBehavedScheduleConstraints();
    	
    	// 3. Index interaction (12)
    	buildIndexPresentConstraints();
    	
    	// 4. Space constraints
    	buildSpaceConstraints();
    	
    	// 5. Optimal constraint
    	buildObjective();
    	
    	// 4. binary variables
    	binaryVariableConstraints();
    	
    	buf.close();
    	    	
        listener.onLogEvent(LogListener.BIP, "Built IIP program");
    }
	
	/**
	 * Build cost function of each query in each window w
	 * Cqw = \sum_{k \in [1, Kq]} \beta_{qk} y(w,q,k) + 
	 *      + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x(w,q,k,i,a) \gamma_{q,k,i,a}
	 */
	private void buildQueryCostWindow()
	{
		List<String> linList = new ArrayList<String>();
		int w, q, k, i, a;
		String var, element;
		String Cwq;
		
		for (q = 0; q < listQueryPlan.size(); q++) {
			SimQueryPlanDesc desc = listQueryPlan.get(q);
			 
			for (w = 0; w < W; w++) {
				// Internal plan
				linList.clear();
			
				for (k = 0; k < desc.getNumPlans(); k++) {
					var = constructVariableName(VAR_Y, w, q, k, 0, 0);
					element = Double.toString(desc.getInternalPlanCost(k)) + var;
					linList.add(element);				
					listVar.add(var);
				}
				Cwq  = Connector.join(" + ", linList);			
						
				// Index access cost
				linList.clear();			
				for (k = 0; k < desc.getNumPlans(); k++) {				
					for (i = 0; i < desc.getNumSlots(); i++) {	
						for (a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
							var = constructVariableName(VAR_X, w, q, k, i, a);
							element = Double.toString(desc.getIndexAccessCost(k, i, a)) + var;
							linList.add(element);						
							listVar.add(var);
						}
					}
				}		
				Cwq = Cwq + " + " + Connector.join(" + ", linList);
				listCwq.add(Cwq);
			}
		}
	}
	
	/**
	 * Standard set of atomic constraint of INUM
	 * 
	 */
	private void buildAtomicConstraints()
	{
		List<String> linList = new ArrayList<String>();
		int w, q, k, i, a, ga;
		String var_y, var_x, var_s;		
		SimQueryPlanDesc desc = new SimQueryPlanDesc();
		
		for (q = 0; q < listQueryPlan.size(); q++) {
			desc = listQueryPlan.get(q);
			for (w = 0; w < W; w++) {
				linList.clear();
				// (1) \sum_{k \in [1, Kq]}y^{theta}_k = 1
				for (k = 0; k < desc.getNumPlans(); k++) {
					linList.add(constructVariableName(VAR_Y, w, q, k, 0, 0));
				}
				buf.getCons().println("atomic_16_" + numConstraints + ": " + Connector.join(" + ", linList) + " = 1");
				numConstraints++;
			
				// (2) \sum_{a \in S_i} x(theta, k, i, a) = y(theta, k)
				for (k = 0; k < desc.getNumPlans(); k++) {
					var_y = constructVariableName(VAR_Y, w, q, k, 0, 0);
					
					for (i = 0; i < desc.getNumSlots(); i++) {
						linList.clear();
						for (a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
							var_x = constructVariableName(VAR_X, w, q, k, i, a);
							linList.add(var_x);
							IndexInSlot iis = new IndexInSlot(q,i,a);
							ga = MatIndexPool.getIndexGlobalId(iis);
							var_s = constructVariableName(VAR_S, w, ga, 0, 0, 0);
						
							// (3) s_a^{theta} \geq x_{kia}^{theta}
							buf.getCons().println("atomic_4_" + numConstraints + ":" 
												+ var_x + " - " 
												+ var_s
												+ " <= 0 ");
							numConstraints++;
						}
						buf.getCons().println("atomic_4_" + numConstraints  
											+ ": " + Connector.join(" + ", linList) 
											+ " - " + var_y
											+ " = 0");
						numConstraints++;
					}
				}		
			}
		}
	}
	
	/**
	 * A well-behaved schedule satisfies the following three conditions:
	 * <p>
	 * <ol> 
	 * 	<li> Indexes in Sremain remain in the DBMS </li>
	 * 	<li> Indexes in Sin are created one time </li>
	 *  <li> Indexes in Sout are dropped one time </li>
	 * </ol>
	 * 
	 * </b> Note </b> Add variables of type CREATE and DROP into the list of variables
	 * </p>
	 * 	
	 */
	private void buildWellBehavedScheduleConstraints()
	{
		List<String> linList = new ArrayList<String>();
		String var;
		
		// for TYPE_CREATE index
		for (int idx = MatIndexPool.getStartPosCreateIndexType(); idx < MatIndexPool.getStartPosDropIndexType(); idx++) {
			linList.clear();
			for (int w = 0; w < W; w++) {
				var = constructVariableName(VAR_CREATE, w, idx, 0, 0, 0);
				listVar.add(var);
				linList.add(var);
			}
			buf.getCons().println("well_behaved_" + numConstraints  
					+ ": " + Connector.join(" + ", linList) 					
					+ " = 1");
			numConstraints++;			
		}
		
		// for TYPE_DROP index
		for (int idx = MatIndexPool.getStartPosDropIndexType(); idx < MatIndexPool.getStartPosRemainIndexType(); idx++) {
			linList.clear();
			for (int w = 0; w < W; w++) {
				var = constructVariableName(VAR_DROP, w, idx, 0, 0, 0);
				listVar.add(var);
				linList.add(var);
			}
			buf.getCons().println("well_behaved_" + numConstraints  
					+ ": " + Connector.join(" + ", linList) 					
					+ " = 1");
			numConstraints++;			
		}
	}
	
	
	/**
	 * The present of an index I at a particular window w is defined as follows:
	 * <p>
	 * <ol> 
	 * 	<li> If I is a created index, then I is present at w if and only w i HAS been 
	 * created at SOME window point between 0 and w </li>
	 * 
	 * 	<li> If I is a dropped index, then I is present at w if and only w i has NOT been 
	 * dropped at ALL window points between 0 and w </li>	 
	 * </ol>
	 * </p>
	 * 
	 * </b> Note </b> Add variables of type PRESENT into the list of variables
	 * 	
	 */
	private void buildIndexPresentConstraints()
	{
		List<String> linList = new ArrayList<String>();
		String var_present = "", var_mat = "", var_s;
		
		// for TYPE_CREATE index
		for (int idx = MatIndexPool.getStartPosCreateIndexType(); idx < MatIndexPool.getStartPosDropIndexType(); idx++) {
			
			for (int w = 0; w < W; w++) {
				var_present = constructVariableName(VAR_PRESENT, w, idx, 0, 0, 0);
				listVar.add(var_present);
				linList.clear();
				for (int j = 0; j <= w; j++) {
					var_mat = constructVariableName(VAR_CREATE, j, idx, 0, 0, 0);
					linList.add(var_mat);
				}
				
				buf.getCons().println("index_present_" + numConstraints  
						+ ": " + Connector.join(" + ", linList) 					
						+ " - " + var_present + " = 0 ");
				numConstraints++;	
			}
					
		}
		
		// for TYPE_DROP index
		for (int idx = MatIndexPool.getStartPosDropIndexType(); idx < MatIndexPool.getStartPosRemainIndexType(); idx++) {
			
			for (int w = 0; w < W; w++) {
				var_present = this.constructVariableName(VAR_PRESENT, w, idx, 0, 0, 0);
				listVar.add(var_present);
				linList.clear();
				for (int j = 0; j <= w; j++) {
					var_mat = constructVariableName(VAR_DROP, j, idx, 0, 0, 0);
					linList.add(var_mat);
				}
				buf.getCons().println("index_present_" + numConstraints  
						+ ": " + Connector.join(" + ", linList) 					
						+ " + " + var_present + " = 1 ");
				numConstraints++;					
			}
					
		}
		
		// s(w,ai) <= present(w,i)
		for (int idx = MatIndexPool.getStartPosCreateIndexType(); idx < MatIndexPool.getStartPosRemainIndexType(); idx++) {
			
			for (int w = 0; w < W; w++) {
				var_present = constructVariableName(VAR_PRESENT, w, idx, 0, 0, 0);
				var_s = constructVariableName(VAR_S, w, idx, 0, 0, 0);
				buf.getCons().println("index_present_" + numConstraints  
						+ ": " + var_s + " - " + var_present + " <= 0 ");
				numConstraints++;	
			}
		}
	}
	
	/**
	 * Impose space constraint on the materialized indexes at all window times
	 * 
	 */
	private void buildSpaceConstraints()
	{
		List<String> linList = new ArrayList<String>();
		String var_present = "";
		double sizeindx, space, spaceRemain;
		int idx, w;
		
		// Take into account the size of indexes in Sremain
		space = 0.0;
		for (idx = MatIndexPool.getStartPosRemainIndexType(); idx < MatIndexPool.totalIndex; idx++) {	
			sizeindx = MatIndexPool.getMatIndex(idx).getMatSize();
			space += sizeindx;
		}
		spaceRemain = space;		
		
		// for TYPE_CREATE and DROP index
		for (w = 0; w < W; w++) {
			
			space = B.get(w) - spaceRemain;
			linList.clear();
			for (idx = MatIndexPool.getStartPosCreateIndexType(); idx < MatIndexPool.getStartPosRemainIndexType(); idx++) {
				var_present = this.constructVariableName(VAR_PRESENT, w, idx, 0, 0, 0);
				sizeindx = MatIndexPool.getMatIndex(idx).getMatSize();
				linList.add(Double.toString(sizeindx) + var_present);
			}
			buf.getCons().println("space_constraint" + numConstraints  
					+ " : " + Connector.join(" + ", linList) 					
					+ " <= " + space);
			numConstraints++;				
		}
						
		
	}
	
	/**
	 * The accumulated total cost function
	 */
	private void buildObjective()
	{
		buf.getObj().println(Connector.join(" + ", listCwq));
	}
	
	
	/**
	 * Constraints all variables to be binary ones
	 * 
	 */
	private void binaryVariableConstraints()
	{
		String lineVars = "";
		int countVar = 0;
		int NUM_VAR_PER_LINE = 10;
		
		for (String var : listVar) {
			lineVars += var;
			lineVars += " ";
			countVar++;
			if (countVar >= NUM_VAR_PER_LINE) {
				countVar = 0;
				buf.getBin().println(lineVars);
				lineVars = "";					
			}
		}
		
		if (countVar > 0) {
			buf.getBin().println(lineVars);
		}		
	}
	
	
	/**
	 * 
	 * Construct the variable name in the form: y(w, q, k), x(w,q, k,i,a), s(w, a), present(w,a),  create(w,a), drop(w,a)
	 *  
	 *
	 * @param typeVarible
	 * 		The type of variable, the value is in the set {y, x, s, present, mat, drop}, 
	 * @param window
	 * 		The window time in the materialized schedule
	 * @param queryId
	 * 	 	The identifier of the processing query if @typeVariable = VAR_Y, VAR_X; 
	 * 		Or the identifier of the index if @typeVariable = VAR_S, VAR_PRESENT, VAR_MAT, or VAR_DROP
	 * @param k
	 * 		The template plan identifier
	 * @param i 
	 * 		The position of slot in the template plan
	 * 		Only enable when @typeVariable = VAR_X
	 * @param a 
	 * 		The position of the index in the corresponding slot
	 * 		Only enable when @typeVariable = VAR_X
	 * 
	 * @return
	 *  	The variable name
	 */
	private String constructVariableName(final int typeVariable, final int window, final int queryId, final int k, final int i, final int a)
	{
		String result = "";
		result = result.concat(strHeaderVariable[typeVariable]);
		result = result.concat("(");
		
		List<String> nameComponent = new ArrayList<String>();
		
		nameComponent.add(Integer.toString(window));
		nameComponent.add(Integer.toString(queryId));
		if (typeVariable == VAR_X || typeVariable == VAR_Y) {
			nameComponent.add(Integer.toString(k));
		}
		
		if (typeVariable == VAR_X) {
			nameComponent.add(Integer.toString(i));
			nameComponent.add(Integer.toString(a));
		}
		
		result = result.concat(Connector.join(",", nameComponent));
		result = result.concat(")");
				
		return result;	
	}
	
	public static int getVarType(final String name)
	{
		if (name.contains("y(")) {
			return VAR_Y;
		}

		if (name.contains("x(")) {
			return VAR_X;
		}

		if (name.contains("s(")) {
			return VAR_S;
		}
		
		if (name.contains("create(")) {
			return VAR_CREATE;
		}
		
		if (name.contains("drop(")) {
			return VAR_DROP;
		}
		
		if (name.contains("present(")) {
			return VAR_PRESENT;
		}
		return VAR_DEFAULT;		
	}
	
	/**
	 * The window time is the string between the first '(' and the first ','
	 * The index id is after ',' and before the last character ')'
	 * @param name
	 * 		Variable name
	 * 
	 * @return
	 * 		The corresponding @MatIndex with the value of window time
	 */
	public static MatIndex deriveMatIndex(String name)
	{
		int type = getVarType(name);
		MatIndex index = null;
		if (type == VAR_CREATE || type == VAR_CREATE) { 
			int openBracket, comma;
			openBracket = name.indexOf("(");
			comma = name.indexOf(",");
			String strW = name.substring(openBracket + 1, comma);
			int w = Integer.parseInt(strW);
			String strId = name.substring(comma + 1, name.length() - 1);
			int Id = Integer.parseInt(strId);
			index = MatIndexPool.getMatIndex(Id);
			index.setMatWindow(w);
		}
		
		return index; 
	}
}
