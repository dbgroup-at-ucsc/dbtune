package edu.ucsc.dbtune.bip.interactions;

import edu.ucsc.dbtune.bip.util.Connector;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;

import java.io.IOException;
import java.util.*;

/**
 * The class is responsible for generating linear constraints for the Interaction Constraint problem
 * 
 * @author tqtrung@soe.ucsc.edu (Quoc Trung Tran)
 *
 */
public class IIPLinGenerator 
{	
	public static final int  VAR_Y = 0;
	public static final int  VAR_X = 1;
	public static final int  VAR_S = 2;
	public static final int  VAR_U = 3;
	public static final int  VAR_DEFAULT = 100;
	
	public static final int IND_EMPTY = 0;
	public static final int IND_C = 1;
	public static final int IND_D = 2;
	public static final int IND_CD = 3;
	
	public static final int NUM_THETA = 4;
	
	private CPlexBuffer buf;
	private int theta; 
	private int numConstraints;
	
	private String[] strTheta = {"empty", "c", "d", "cd"};
	private String[] strHeaderVariable = {"y", "x", "s", "u"};
	private String[] CTheta;

	private List< List <String> > elementCTheta, varTheta;
	private List< List <Integer> > countVarTheta; 
	private RestrictIIPParam restrictIIP;
	private IIPQueryPlanDesc desc;
	private int totalVar; 	
	private List<List<Double>> coefVarTheta;
	
	IIPLinGenerator(RestrictIIPParam restrictIIP, IIPQueryPlanDesc desc, String prefix)
	{		
		this.restrictIIP = restrictIIP;	
		this.desc = desc;
	
		CTheta = new String[NUM_THETA];
		elementCTheta = new ArrayList<List<String>>(); 		
		varTheta = new ArrayList<List<String>>();
		try {
			buf = new CPlexBuffer(prefix);
		}
		catch (IOException e) {
			System.out.println(" Error in opening files " + e.toString());			
		}
		
		numConstraints = 0;
		
		coefVarTheta = new ArrayList<List<Double>> ();	
		countVarTheta = new ArrayList<List<Integer>>();		
		for (theta = 0; theta < NUM_THETA; theta++) {
			List <Integer> cVar = new ArrayList<Integer>();
			for (int type = 0; type < strHeaderVariable.length; type++) {
				cVar.add(new Integer (0));
			}
			countVarTheta.add(cVar);
			
			List <Double> coefV = new ArrayList<Double>();
			coefVarTheta.add(coefV);
		}
		
	}
	
	/**
	 * The function builds the BIP for the Restrict IIP problem, includes three sets of constraints:
	 * <p>
	 * <ol> 
	 * 	<li> Atomic constraints, including (4) - (7) </li> 
	 * 	<li> Optimal constraints, including (8) - (11) </li>
	 * 	<li> One alternative of index interaction constraint, (12) </li>
	 * </ol>
	 * </p>	  
	 * 
	 * <b> Note </b>: The alternative index interaction constraint, (13), will be considered only 
	 * if this problem formulation does not return any solution
	 * 
	 * @param listener
	 * 		Log the building process
	 * 
	 * @throws IOException
	 */
	public void build(LogListener listener) throws IOException 
	{
    	listener.onLogEvent(LogListener.BIP, "Building IIP program...");
    	
    	// Construct the formula of @C{theta} in (3)
    	buildCTheta();
    	
    	// 1. Atomic constraints (4) - (7)
    	buildAtomicConstraints();
    	
    	// 2. Optimal constraints (8) - (11)
    	buildOptimalConstraints();
    	buidOptimalConstraintRHS();
    	
    	// 3. Index interaction (12)
    	buildIndexInteractionConstraint1();
    	
    	// 4. binary variables
    	binaryVariableConstraints();
    	
    	buf.close();
    	    	
        listener.onLogEvent(LogListener.BIP, "Built IIP program");
    }
	
	

	/**
	 * Construct the formula of C{theta} in (3) and add variables name into the corresponding list
	 * 
	 * C(theta)=  sum_{k \in [1, Kq] } beta_k y_{k,theta}
	 *		   + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x_{kia, theta}
	 *
	 */
	private void buildCTheta()
	{
		String ctheta, var, element;
		int i, a, k;
		List<String> linList = new ArrayList<String>();
		ArrayList<String> varList;
		int countX, countY;
				
		for (theta = IND_EMPTY; theta <= IND_CD; theta++) {			
			ctheta = "";
			List<String> linListElement = new ArrayList<String>();			
			varList = new ArrayList<String>();
			
			// Internal plan
			linList.clear();
			
			countY = 0;
			for (k = 0; k < desc.getNumPlans(); k++) {
				var = constructVariableName(theta, VAR_Y, k, 0, 0);
				element = Double.toString(desc.getInternalPlanCost(k)) + var;
				linList.add(element);
				linListElement.add(element);
				varList.add(var);				
				coefVarTheta.get(theta).add(new Double(desc.getInternalPlanCost(k)));					
				countY++;
			}
			ctheta  = Connector.join(" + ", linList);
			countVarTheta.get(theta).set(VAR_Y, countY);
						
			// Index access cost
			linList.clear();
			countX = 0;
			for (k = 0; k < desc.getNumPlans(); k++) {				
				for (i = 0; i < desc.getNumSlots(); i++) {					
					for (a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
						var = constructVariableName(theta, VAR_X, k, i, a);
						countX++;
						element = Double.toString(desc.getIndexAccessCost(k, i, a)) + var;
						linList.add(element);
						linListElement.add(element);
						varList.add(var);
						coefVarTheta.get(theta).add(new Double(desc.getIndexAccessCost(k, i, a)));
					}
				}
			}		
			ctheta = ctheta + " + " + Connector.join(" + ", linList);
			countVarTheta.get(theta).set(VAR_X, countX);
			
			// Record @ctheta, element of @ctheta, and @variables
			CTheta[theta] = ctheta;			
			elementCTheta.add(linListElement);
			varTheta.add(varList);			
		}
	}
		
	/**
	 * Atomic constraints (4) - (7)
	 * 
	 */
	private void buildAtomicConstraints()
	{	
		List<String> linList = new ArrayList<String>();
		int k = 0, i = 0, a = 0, ga;
		String var_y, var_x, var_s;		
		
		/**
		 * Constraint (4): atomic configuration for each C{theta} 
		 */
		for (theta = IND_EMPTY; theta <= IND_CD; theta++) {
			linList.clear();
			// (1) \sum_{k \in [1, Kq]}y^{theta}_k = 1
			for (k = 0; k < desc.getNumPlans(); k++) {
				linList.add(constructVariableName(theta, VAR_Y, k, 0, 0));
			}
			buf.getCons().println("atomic_4_" + numConstraints + ": " + Connector.join(" + ", linList) + " = 1");
			numConstraints++;
			
			// (2) \sum_{a \in S_i} x(theta, k, i, a) = y(theta, k)
			for (k = 0; k < desc.getNumPlans(); k++) {
				var_y = constructVariableName(theta, VAR_Y, k, 0, 0);
				
				for (i = 0; i < desc.getNumSlots(); i++) {
					linList.clear();
					for (a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
						var_x = constructVariableName(theta, VAR_X, k, i, a);
						linList.add(var_x);
						ga = desc.globalIndex(i,a);
						var_s = constructVariableName(theta, VAR_S, ga, 0, 0);
						varTheta.get(theta).add(var_s);
						
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
		
		/**
		 * Constraint (5): an index is not selected if it has not been used
		 * in any slot   
		 */
		for (theta = IND_EMPTY; theta <= IND_CD; theta++) {
			
			for (i = 0; i < desc.getNumSlots(); i++) {
				for (a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
					ga = desc.globalIndex(i,a);
					linList.clear();
					for (k = 0; k < desc.getNumPlans(); k++){
						linList.add(constructVariableName(theta, VAR_X, k, i, a));
					}
				
					if (theta == IND_EMPTY // Empty case (5a)
						|| (theta == IND_C && i != restrictIIP.getPosRelContainC()) // For S_{c} (5b)
						|| (theta == IND_D && i != restrictIIP.getPosRelContainD()) // For S_{d} (5c)
						|| (theta == IND_CD && i != restrictIIP.getPosRelContainC() 
											&& i != restrictIIP.getPosRelContainD())) 
						// For S_{cd} (5d)
					{ 							
						buf.getCons().println("atomic_5_" + numConstraints + ":"
								+ constructVariableName(theta, VAR_S, ga , 0, 0)
								+ " - "
								+ Connector.join(" - ", linList)
								+ " <= 0");
						numConstraints++;
					}
				}				
			}			
		}
		

		/**
		 * Constraint (6): the exception case for the relation that contain index c
		 * 
		 *  
		 */				
		for (theta = IND_C; theta <= IND_CD; theta++) 	{
			
			if (theta == IND_D) { 
				continue;
			}
			
			linList.clear();
			for (a = 0; a < desc.getNumIndexesEachSlot(restrictIIP.getPosRelContainC()); a++) {				
				if (a != restrictIIP.getLocalPosIndexC()) {
					ga = desc.globalIndex(restrictIIP.getPosRelContainC(), a);
					linList.add(constructVariableName(theta, VAR_S, ga, 0, a));
				}
			}
			buf.getCons().println("atomic_6_" +  numConstraints + ":" 
								+ Connector.join(" + ", linList) 
								+ " <= 1");
			numConstraints++;
		}
		
		
		/**
		 * Constraint (6): exception of the relation containing index d 
		 */
		
		for (theta = IND_D; theta <= IND_CD; theta++) {
			
			linList.clear();
			for (a = 0; a < desc.getNumIndexesEachSlot(restrictIIP.getPosRelContainD()); a++) {				
				if (a != restrictIIP.getLocalPosIndexD()) {
					ga = desc.globalIndex(restrictIIP.getPosRelContainD(), a);
					linList.add(constructVariableName(theta, VAR_S, ga, 0, a));
				}
			}
			buf.getCons().println("atomic_6_" + numConstraints + ":" 
								+ Connector.join(" + ", linList) 
								+ "  <= 1");
			numConstraints++;
		}
		
		
		/**
		 * Constraint (7): constraining for the indexes c and d not appear in Xstar
		 */
		
		buf.getCons().println("atomic_7_" + numConstraints + ":" 
							+ constructVariableName(IND_EMPTY, VAR_S, 
							desc.globalIndex(restrictIIP.getPosRelContainC(), restrictIIP.getLocalPosIndexC()), 0, 0)
											+ " = 0 "); // For s^{empty}_c = 0
		numConstraints++;
		buf.getCons().println("atomic_7_" + numConstraints + ":" 
							+ constructVariableName(IND_EMPTY, VAR_S, 
							desc.globalIndex(restrictIIP.getPosRelContainD(), restrictIIP.getLocalPosIndexD()), 0, 0)
											+ " = 0 "); // For s^{empty}_d = 0
		numConstraints++;
		buf.getCons().println("atomic_7_" + numConstraints + ":"
							+ constructVariableName(IND_C, VAR_S, 
							desc.globalIndex(restrictIIP.getPosRelContainD(), restrictIIP.getLocalPosIndexD()), 0, 0)
											+ " = 0 "); // For s^{c}_d = 0
		numConstraints++;
		buf.getCons().println("atomic_7_" + numConstraints + ":" 
							+ constructVariableName(IND_D, VAR_S, 
							desc.globalIndex(restrictIIP.getPosRelContainC(), restrictIIP.getLocalPosIndexC()), 0, 0)
											+ " = 0 "); // For s^{d}_c = 0	
		numConstraints++;
		
	}
	
	/**
	 * Build optimal constraints for C(theta) to be the optimal cost among all possible configuration
	 * Implement constraints (8) - (11) 
	 * Add variables of type VAR_U into the list of variables
	 * 
	 */
	private void buildOptimalConstraints()
	{
		int theta, i, a, t, ga;
		String var_u;
		List<String> linList = new ArrayList<String>();
		
		for (theta = IND_EMPTY; theta <= IND_CD; theta++) {
			
			for (t = 0; t < desc.getNumPlans(); t++) {
				
				linList.clear();			
				for (i = 0; i < desc.getNumSlots(); i++) {
					for (a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
						var_u = constructVariableName(theta, VAR_U, t, i, a);					
						varTheta.get(theta).add(var_u);
						linList.add(Double.toString(desc.getIndexAccessCost(t, i, a)) + var_u);						 						 
					}
				}
				// Constraint (8)
				buf.getCons().println("optimal_8_" + numConstraints + ":" 
						+ CTheta[theta] + " - " 
						+ Connector.join(" - ", linList) 		
						+ " <=  " + Double.toString(desc.getInternalPlanCost(t)));
				numConstraints++;
				
			}
		}
		
		for (theta = IND_EMPTY; theta <= IND_CD; theta++) {			 
			for (t = 0; t < desc.getNumPlans(); t++) {							
				for (i = 0; i < desc.getNumSlots(); i++) {
					for (a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
						
						var_u = constructVariableName(theta, VAR_U, t, i, a);						
						if ( (theta == IND_C && a == restrictIIP.getLocalPosIndexC() 
								&& i == restrictIIP.getPosRelContainC())  
							|| (theta == IND_D && a == restrictIIP.getLocalPosIndexD() 
								&& i == restrictIIP.getPosRelContainD()) 
							|| (theta == IND_CD && a == restrictIIP.getLocalPosIndexC() 
								&& i == restrictIIP.getPosRelContainC())
							|| (theta == IND_CD && a == restrictIIP.getLocalPosIndexD() 
								&& i == restrictIIP.getPosRelContainD())
						)
						{
							/*
							// Constraint (9b) - TODO: don't need this constraint
							buf.getCons().println("optimal_9b_" + numConstraints + ":" 
												+ var_u + " <= 1 ");
							numConstraints++;
							*/
						} 
						else if ( (theta == IND_EMPTY && a == restrictIIP.getLocalPosIndexC() 
								&& i == restrictIIP.getPosRelContainC()) 
								|| (theta == IND_EMPTY && a == restrictIIP.getLocalPosIndexD() 
										&& i == restrictIIP.getPosRelContainD())						
								|| (theta == IND_C && a == restrictIIP.getLocalPosIndexD() 
										&& i == restrictIIP.getPosRelContainD())
								|| (theta == IND_D && a == restrictIIP.getLocalPosIndexC() 
										&& i == restrictIIP.getPosRelContainC())
						)
						{							
							buf.getCons().println("optimal_9b_" + numConstraints + ":" 
									+ var_u + " = 0 ");
							numConstraints++;
							
						}
						else if (i != restrictIIP.getPosRelContainC() && i != restrictIIP.getPosRelContainD())
						{							
							// Constraint (9a): u^{theta}_{tia} <= sum_{theta}_{a}
							ga = desc.globalIndex(i, a);
							buf.getCons().println("optimal_9a_" + numConstraints + ":" 
									+ var_u 
									+ " - " + constructVariableName(IND_EMPTY, VAR_S, ga, 0, 0)
									+ " - " + constructVariableName(IND_C, VAR_S, ga, 0, 0)
									+ " - " + constructVariableName(IND_D, VAR_S, ga, 0, 0)
									+ " - " + constructVariableName(IND_CD, VAR_S, ga, 0, 0)
									+ " <= 0 ");
							numConstraints++;
							
						} 
					} 					
				}			
						
			}
		}
	}
	
	/**
	 * Implement the optimal constraints on variable @u in the RHS of (10) and (11)
	 * 
	 */
	private void buidOptimalConstraintRHS()
	{
		
		int theta, i, t, ga, p, thetainternal;
		String var_u;
		List<String> linList = new ArrayList<String>();
		
		// Constraint (10)
		for (theta = IND_EMPTY; theta <= IND_CD; theta++) {				
			for (t = 0; t < desc.getNumPlans(); t++) {
				for (i = 0; i < desc.getNumSlots(); i++) {	
					// Sort index access cost
					List<SortableIndexAcessCost> listSortedIndex  = new 
											ArrayList<SortableIndexAcessCost> ();
					for (p = 0; p < desc.getNumIndexesEachSlot(i); p++){
						SortableIndexAcessCost sac = new SortableIndexAcessCost
											(desc.getIndexAccessCost(t, i, p), p);
						listSortedIndex.add(sac);						
					}		
					Collections.sort(listSortedIndex);
					linList.clear();
					
					for (SortableIndexAcessCost sac : listSortedIndex){						
						p = sac.getPosition();												
						ga = desc.globalIndex(i, p);
						var_u = constructVariableName(theta, VAR_U, t, i, p);
						linList.add(var_u);
						String LHS = Connector.join(" + ", linList);
						
						if ( i == restrictIIP.getPosRelContainC() && p == restrictIIP.getLocalPosIndexC()) {
							// --- \sum >= 1
							if (theta == IND_C || theta == IND_CD) {
								// Constraint (11)
								buf.getCons().println("optimal_11_" + numConstraints + ":" 
										+ LHS  
										+ " = 1");
								numConstraints++;
							}
						}
						else if (i == restrictIIP.getPosRelContainD() && p == restrictIIP.getLocalPosIndexD()) {
							// --- \sum >= 1
							if (theta == IND_D || theta == IND_CD)
							{
								// Constraint (11)
								buf.getCons().println("optimal_11_" + numConstraints + ":" 
										+ LHS  
										+ " = 1");
								numConstraints++;
							}
						}
						else {						
							for (thetainternal = IND_EMPTY; thetainternal <= IND_CD; thetainternal++) {
								// Constraint (10a)
								buf.getCons().println("optimal_10a_" + numConstraints + ":" 
										+ LHS + " - "
										+ constructVariableName(thetainternal, VAR_S, ga, 0, 0)
										+ " >= 0");
								numConstraints++;
							}
						}										
					}	
					// Constraint (10b) 
					buf.getCons().println("optimal_10b_" + numConstraints + ":" 
										+ Connector.join(" + ", linList) + " = 1");
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
		
		buf.getCons().println("interaction_13:" + CTheta[IND_EMPTY] 
		                      + " + " + Connector.join(" + ", listcd) 
		                      + " -  " + Connector.join(" - ", elementCTheta.get(IND_C))
		                      + " -  " + Connector.join(" - ", elementCTheta.get(IND_D))
		                      + " <= 0");
		                      
	}
	
	
	
	/**
	 * Build the alternative index interaction constraint (12):
	 * cost(X,c) + cost(X,d) - cost(X) - (1-delta) cost(X,c,d)  <= 0
	 * 
	 * Different from buildIndexInteractionConstraint1(), which returns a formula
	 * This function returns a hash map of (variable, coefficient) to 
	 * concatenate with the variables derived from the BIP model
	 *
	 * @return 
	 * 		A mapping table that maps variable to the coefficient attached to this variable
	 * 		(ONLY variabes of type y and x)
	 * 
	 */
	public Map<String, Double> buildIndexInteractionConstraint2()
	{
		Map<String,Double> mapVarCoef = new HashMap<String, Double>();
		List<String> listVar = new ArrayList<String>(); 
		int theta = 1, p;
		double coef = 0.0, existingCoef;
		double deltaCD = restrictIIP.getDelta() - 1;		
		List<Integer> totalXY = new ArrayList<Integer>();
		
		// Compute the total number of variables of type @y and @x
		for (theta = IND_EMPTY; theta <= IND_CD; theta++) {
			totalXY.add(new Integer(countVarTheta.get(theta).get(VAR_Y) 
									+ countVarTheta.get(theta).get(VAR_X)));
		}
		
		for (theta = IND_EMPTY; theta <= IND_CD; theta++) {
			listVar = varTheta.get(theta);	
			
			for (p = 0; p < listVar.size(); p++) {
				// Variables of types @u and @s have coef = 0
				if (p >= totalXY.get(theta)){
					coef = 0.0;
				}
				else {
					existingCoef = coefVarTheta.get(theta).get(p);
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
				mapVarCoef.put(listVar.get(p), new Double(coef));				
			}			
		}
		
		return mapVarCoef;
	}
	
	/**
	 * Output the list of variables into @bin file to constraint
	 * binary variables
	 * 
	 */
	private void binaryVariableConstraints()
	{
		String lineVars = "";
		int countVar = 0;
		int numVarOneLine = 10;
		
		for (List<String> listVar : varTheta) {
			for (String var : listVar) {
				lineVars += var;
				lineVars += " ";
				countVar++;
				if (countVar >= numVarOneLine) {
					countVar = 0;
					buf.getBin().println(lineVars);
					lineVars = "";					
				}
			}
		}
		
		if (countVar > 0) {
			buf.getBin().println(lineVars);
		}		
	}
	
	
	/**
	 * 
	 * Construct the variable name in the form: y(empty, k), x(c,k,i,a), s(d, a), or u(cd, k,i,a).
	 *  
	 * @param theta
	 * 		The value of @theta in the set of {empty, c, d, cd}
	 * @param typeVarible
	 * 		The type of variable, the value is in the set {y, x, u, s}
	 * @param k
	 * 	 	The identifier of the corresponding template plan if @typeVariable = VAR_Y, VAR_X, VAR_U; 
	 * 		Or the identifier of the index if @typeVariable = VAR_S
	 * @param i 
	 * 		The position of slot in the template plan
	 * 		Only enable when @typeVariable = VAR_X, VAR_U
	 * @param a 
	 * 		The position of the index in the corresponding slot
	 * 		Only enable when @typeVariable = VAR_X, VAR_U
	 * 
	 * @return
	 *  	The variable name
	 */
	private String constructVariableName(int theta, int typeVariable, int k, int i, int a)
	{
		String result = "";
		result = result.concat(strHeaderVariable[typeVariable]);
		result = result.concat("(");
		
		List<String> nameComponent = new ArrayList<String>();
		
		nameComponent.add(strTheta[theta]);
		nameComponent.add(Integer.toString(k));
		
		if (typeVariable == VAR_X || typeVariable == VAR_U) {			
			nameComponent.add(Integer.toString(i));
			nameComponent.add(Integer.toString(a));
		}
		
		result = result.concat(Connector.join(",", nameComponent));
		result = result.concat(")");
				
		return result;	
	}
	
	
	/**
	 * 
	 * @return (total number of variables used in this BIP problem)
	 */
	public int getTotalVar()
	{
		return totalVar;
	}
	
	/**
	 * 
	 * @return (total number of constraints formulated in this BIP problem)
	 */
	public int getTotalConstraints()
	{
		return numConstraints;
	}
		
	public static int getVarType(String name)
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
		
		if (name.contains("u(")) {
			return VAR_U;
		}
		
		return VAR_DEFAULT;
		
	}
	
	/**
	 * Compute the value of C{theta}
	 * @param mapVarVal
	 */
	public void computeC(Map<String, Double> mapVarVal)
	{
		int theta, k, i, a;
		String var;
		double ctheta = 0.0, eleVar;
		for (theta = IND_EMPTY; theta <= IND_CD; theta++) {						
			ctheta = 0.0;		
			for (k = 0; k < desc.getNumPlans(); k++) {
				var = constructVariableName(theta, VAR_Y, k, 0, 0);
				eleVar = mapVarVal.get(var);
				ctheta += desc.getInternalPlanCost(k) * eleVar;				
			}
			
			for (k = 0; k < desc.getNumPlans(); k++) {				
				for (i = 0; i < desc.getNumSlots(); i++) {					
					for (a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
						var = constructVariableName(theta, VAR_X, k, i, a);
						eleVar = mapVarVal.get(var);
						ctheta += desc.getIndexAccessCost(k, i, a) * eleVar;
					}
				}
			}		
			System.out.println(" Ctehta " + theta + " : " + ctheta);
		}
	}
}
