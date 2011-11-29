

package edu.ucsc.dbtune.bip;

import java.io.IOException;
import java.util.*;

/**
 * The class is responsible for generating linear constraints for the Interaction Constraint problem
 * 
 * @author tqtrung@soe.ucsc.edu (Quoc Trung Tran)
 *
 */
public class IIPLinGenerator {	
	
	public static final int  VAR_Y = 0;
	public static final int  VAR_X = 1;
	public static final int  VAR_S = 2;
	public static final int  VAR_U = 3;
	
	public static final int IND_EMPTY = 0;
	public static final int IND_C = 1;
	public static final int IND_D = 2;
	public static final int IND_CD = 3;
	
	public static final int NUM_THETA = 4;
	
	private CPlexBuffer buf;
	private int theta;
	
	private String[] strTheta = {"emtpy", "c", "d", "cd"};
	private String[] strHeaderVariable = {"y", "x", "s", "u"};
	private String[] CTheta;
	
	private ArrayList<ArrayList <String> > elementCTheta, varTheta;
	private RestrictIIPParam restrictIIP;
	private int totalVar;
	
	IIPLinGenerator(RestrictIIPParam restrictIIP){		
		this.restrictIIP = restrictIIP;	
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
	public void build(LogListener listener) throws IOException {
    	listener.onLogEvent(LogListener.BIP, "Building IIP program...");
    	
    	// Construct the formula of @C{theta} in (3)
    	buildCTheta();
    	
    	// 1. Atomic constraints (4) - (7)
    	buildAtomicConstraints();
    	
    	// 2. Optimal constraints (8) - (11)
    	buildOptimalConstraints();
    	
    	// 3. Index interaction (12)
    	buildIndexInteractionConstraint1();
    	
        listener.onLogEvent(LogListener.BIP, "Built IIP program");
    }

	/**
	 * Construct the formula of C{theta} in (3) and add variables name into the corresponding list
	 * 
	 * C(theta)=  sum_{k \in [1, Kq] } beta_k y_{k,theta}
	 *		   + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x_{kia, theta}
	 *
	 */
	private void buildCTheta(){
		String ctheta, var;
		int i, a, k;
		ArrayList<String> linList = new ArrayList<String>();
		ArrayList<String> varList = new ArrayList<String>();		
		
		for (theta = 0; theta < NUM_THETA; theta++)
		{						
			varList.clear();
			
			// Internal plan
			linList.clear();
			for (k = 0; k < restrictIIP.desc.Kq; k++)
			{
				var = constructVariableName(theta, VAR_Y, k, 0, 0);
				linList.add(Double.toString(restrictIIP.desc.beta[k]) + var);
				varList.add(var);
			}
			ctheta  = Connector.join(" + ", linList);
			
			// Index access cost
			linList.clear();
			for (k = 0; k < restrictIIP.desc.Kq; k++)
			{
				for (i = 0; i < restrictIIP.desc.n; i++)
				{
					for (a = 0; a < restrictIIP.desc.S[i]; a++)
					{
						var = constructVariableName(theta, VAR_X, k, i, a);
						linList.add(Double.toString(restrictIIP.desc.gamma[k][i][a]) + var);
						varList.add(var);
					}
				}
			}		
			ctheta = ctheta + " + " + Connector.join(" + ", linList);
			
			// Record @ctheta, element of @ctheta, and @variables
			CTheta[theta] = ctheta;			
			elementCTheta.add(linList);
			varTheta.add(varList);
		}
	}
		
	/**
	 * Atomic constraints (4) - (7)
	 * 
	 */
	private void buildAtomicConstraints(){
		
		ArrayList<String> linList = new ArrayList<String>();
		int k = 0, i = 0, a = 0, ga;
		String var_y, var_x;		
		
		/**
		 * Constraint (4): atomic configuration for each C{theta} 
		 */
		for (theta = 0; theta < NUM_THETA; theta++)
		{
			linList.clear();
			// (1) \sum_{k \in [1, Kq]}y^{theta}_k = 1
			for (k = 0; k < restrictIIP.desc.Kq; k++)
			{
				linList.add(constructVariableName(theta, VAR_Y, k, i, a));
			}
			buf.getCons().println("atomic_4" + theta + ": " + Connector.join(" + ", linList) + " - 1 = 0");
			
			// (2) \sum_{a \in S_i} x(theta, k, i, a) = y(theta, k)
			for (k = 0; k < restrictIIP.desc.Kq; k++)
			{
				var_y = constructVariableName(theta, VAR_Y, k, i, a);
				for (i = 0; i < restrictIIP.desc.n; i++)
				{
					linList.clear();
					for (a = 0; a < restrictIIP.desc.S[i]; a++)
					{
						var_x = constructVariableName(theta, VAR_X, k, i, a);
						linList.add(var_x);
						ga = restrictIIP.desc.globalIndex(i,a);
						
						// (3) s_a^{theta} \geq x_{kia}^{theta}
						buf.getCons().println("atomic_4" + theta + ":" 
											+ var_x + " - " 
											+ constructVariableName(theta, VAR_S, ga, i, a)
											+ " <= 0 ");
					}
					buf.getCons().println("atomic_4" + theta  
											+ ": " + Connector.join(" + ", linList) 
											+ " - " + var_y
											+ " = 0");
				}
			}		
		}
		
		/**
		 * Constraint (5): an index is not selected if it has not been used
		 * in any slot   
		 */
		for (theta = 0; theta < NUM_THETA; theta++)
		{
			for (i = 0; i < restrictIIP.desc.n; i++)
			{
				for (a = 0; a < restrictIIP.desc.S[i]; a++)
				{
					ga = restrictIIP.desc.globalIndex(i,a);
					linList.clear();
					for (k = 0; k < restrictIIP.desc.Kq; k++)
					{
						linList.add(constructVariableName(theta, VAR_X, k, i, a));
					}
					if (theta == IND_EMPTY // Empty case (5a)
						|| (theta == IND_C && i != restrictIIP.ic) // For S_{c} (5b)
						|| (theta == IND_D && i != restrictIIP.id) // For S_{d} (5c)
						|| (theta == IND_CD && i != restrictIIP.ic && i != restrictIIP.id)) 
						// For S_{cd} (5d)
					{ 							
						buf.getCons().println("atomic_5" + ":"
								+ constructVariableName(theta, VAR_S, ga , i, a)
								+ " - "
								+ Connector.join(" - ", linList)
								+ " <= 0");
					}
				}				
			}			
		}
		

		/**
		 * Constraint (6): the exception case for the relation that contain index c
		 * 
		 *  
		 */
		for (theta = IND_C; theta < NUM_THETA; theta++)
		{
			if (theta == IND_D)
			{ 
				continue;
			}
			linList.clear();
			for (a = 0; a < restrictIIP.desc.S[restrictIIP.ic]; a++)
			{
				if (a != restrictIIP.pos_c)
				{
					ga = restrictIIP.desc.globalIndex(restrictIIP.ic, a);
					linList.add(constructVariableName(theta, VAR_S, ga, 0, a));
				}
			}
			buf.getCons().println("atomic_6:" 
								+ Connector.join(" + ", linList) 
								+ " -  1 <= 0");
		}
		
		/**
		 * Constraint (6): exception of the relation containing index d 
		 */
		for (theta = IND_D; theta < NUM_THETA; theta++)
		{				
			linList.clear();
			for (a = 0; a < restrictIIP.desc.S[restrictIIP.id]; a++)
			{
				if (a != restrictIIP.pos_d)
				{
					ga = restrictIIP.desc.globalIndex(restrictIIP.id, a);
					linList.add(constructVariableName(theta, VAR_S, ga, 0, a));
				}
			}
			buf.getCons().println("atomic_6:" 
								+ Connector.join(" + ", linList) 
								+ " -  1 <= 0");
		}
		
		
		/**
		 * Constraint (7): contraining for the indexes c and d not appear in Xstar
		 */
		buf.getCons().println("atomic_7:" + constructVariableName(IND_EMPTY, VAR_S, 
							restrictIIP.desc.globalIndex(restrictIIP.ic, restrictIIP.pos_c), 0, 0)
											+ " = 0 "); // For s^{empty}_c = 0
		buf.getCons().println("atomic_7:" + constructVariableName(IND_EMPTY, VAR_S, 
							restrictIIP.desc.globalIndex(restrictIIP.id, restrictIIP.pos_d), 0, 0)
											+ " = 0 "); // For s^{empty}_d = 0
		buf.getCons().println("atomic_7:" + constructVariableName(IND_C, VAR_S, 
							restrictIIP.desc.globalIndex(restrictIIP.id, restrictIIP.pos_d), 0, 0)
											+ " = 0 "); // For s^{c}_d = 0
		buf.getCons().println("atomic_7:" + constructVariableName(IND_D, VAR_S, 
							restrictIIP.desc.globalIndex(restrictIIP.ic, restrictIIP.pos_c), 0, 0)
											+ " = 0 "); // For s^{d}_c = 0	
		
	}
	
	/**
	 * Build optimal constraints for C(theta) to be the optimal cost among all possible configuration
	 * Implement constraints (8) - (11) 
	 * Add variables of type VAR_U into the list of variables
	 * 
	 */
	private void buildOptimalConstraints(){
		int theta, thetainternal, i, j, a, t, ga, p;
		String var_u;
		ArrayList<String> linList = new ArrayList<String>();
		
		for (theta = IND_EMPTY; theta < IND_CD; theta++)
		{			
			for (t = 0; t < restrictIIP.desc.Kq; t++)
			{					
				linList.clear();
				linList.add(Double.toString(restrictIIP.desc.beta[t]));
				for (i = 0; i < restrictIIP.desc.n; i++)
				{
					for (a = 0; a < restrictIIP.desc.S[i]; a++)
					{
						var_u = constructVariableName(theta, VAR_U, t, i, a);
						linList.add(Double.toString(restrictIIP.desc.gamma[t][i][a]) + var_u);
						(varTheta.get(theta)).add(var_u);
						
						if ( (theta == IND_C && a == restrictIIP.pos_c && i == restrictIIP.ic)  
							|| (theta == IND_D && a == restrictIIP.pos_d && i == restrictIIP.id) 
							|| (theta == IND_CD && a == restrictIIP.pos_c && i == restrictIIP.ic)
							|| (theta == IND_CD && a == restrictIIP.pos_d && i == restrictIIP.id)
						)
						{
							// Constraint (9b)
							buf.getCons().println("optimal_9b:" + var_u + " <= 1 ");
						} 
						else if (i != restrictIIP.ic && i != restrictIIP.id)
						{
							// Constraint (9a): u^{theta}_{tia} <= sum_{theta}_{a}
							ga = restrictIIP.desc.globalIndex(i, a);
							buf.getCons().println("optimal_9a:" + var_u 
									+ " - " + constructVariableName(IND_EMPTY, VAR_S, ga, 0, 0)
									+ " - " + constructVariableName(IND_C, VAR_S, ga, 0, 0)
									+ " - " + constructVariableName(IND_D, VAR_S, ga, 0, 0)
									+ " - " + constructVariableName(IND_CD, VAR_S, ga, 0, 0)
									+ " <= 0 ");
						} 
					}
				}
				// Constraint (8)
				buf.getCons().println("optimal_8:" + CTheta[theta] + " - " 
						+ Connector.join(" - ", linList) 		
						+ " <= 0 ");
			}
		}
		
		
		// Constraint (10)
		for (theta = IND_EMPTY; theta < IND_CD; theta++)
		{			
			for (t = 0; t < restrictIIP.desc.Kq; t++)
			{
				for (i = 0; i < restrictIIP.desc.n; i++)
				{	
					linList.clear();					
					for (p = 0; p < restrictIIP.desc.S[i]; p++)
					{
						ga = restrictIIP.desc.globalIndex(i, p);
						for (j = 0; j <= p; j++)
						{
							var_u = constructVariableName(theta, VAR_U, t, i, j);
							linList.add(var_u);
						}
						String LHS = Connector.join(" + ", linList);
						for (thetainternal = IND_EMPTY; thetainternal < IND_CD; thetainternal++)
						{
							// Constraint (10a)
							buf.getCons().println("optimal_10a:" + LHS + " - "
									+ constructVariableName(thetainternal, VAR_S, ga, 0, 0)
									+ " >= 0");
						}
						// Constraint (10b) 
						buf.getCons().println("optimal_10b:" + LHS + " - 1 <= 0");
						
					}
				}
			}			
		}
		
		// Constraint (11)
		for (theta = IND_C; theta < IND_CD; theta++)
		{		
			// the right handside corresponds to Xstar \cup S_{\theta}
			if (theta == IND_C || theta == IND_CD)
			{
				for (t = 0; t < restrictIIP.desc.Kq; t++)
				{
					linList.clear();
					for (j = 0; j < restrictIIP.pos_c; j++)
					{				
						var_u = constructVariableName(theta, VAR_U, t, restrictIIP.ic, j);
						linList.add(var_u);
					}	
					buf.getCons().println("optimal_11:" + Connector.join(" + ", linList) 
											+ " - 1 >= 0");		
				}						
			}
			
			if (theta == IND_D || theta == IND_CD)
			{
				for (t = 0; t < restrictIIP.desc.Kq; t++)
				{
					linList.clear();
					for (j = 0; j < restrictIIP.pos_d; j++)
					{				
						var_u = constructVariableName(theta, VAR_U, t, restrictIIP.id, j);
						linList.add(var_u);
					}	
					buf.getCons().println("optimal_11:" + Connector.join(" + ", linList) 
											+ " - 1 >= 0");		
				}						
			}
			
		}
	}
	
	/**
	 * Build the first index interaction constraint (12)
	 * cost(X,c) + cost(X,d) - cost(X) - (1-delta) cost(X,c,d)  <= 0
	 *  	
	 */
	private void buildIndexInteractionConstraint1(){		
		 
		ArrayList<String> listcd = new ArrayList<String>();
		double coef = 1 - restrictIIP.delta;
		for (Iterator<String> iterator = elementCTheta.get(IND_CD).iterator(); iterator.hasNext();) {
			String element = (String) iterator.next();
			listcd.add(Double.toString(coef) + element);
		}
		buf.getCons().println("interaction_12:" + CTheta[IND_C] 
		                      + " + " + CTheta[IND_D] 
		                      + " -  " + Connector.join(" - ", elementCTheta.get(IND_EMPTY)) 
		                      + " - " + Connector.join(" - ", listcd)
		                      + " <= 0");
		                      
	}
	
	
	/**
	 * Build the alternative index interaction constraint (13):
	 * cost(X) + (1 + delta) cost(X, c,d ) - cost(X,c) - cost(X,d) <= 0
	 * Different from buildIndexInteractionConstraint1(), which returns a formula
	 * This function returns a hash map of (variable, coefficient) to 
	 * concatenate with the variables derived from the BIP model
	 *
	 * @return 
	 * 		A mapping table that maps variable to the coefficient attached to this variable
	 * 
	 */
	public HashMap<String, Double> buildIndexInteractionConstraint2(){
		HashMap<String,Double> mapVarCoef = new HashMap<String, Double>();
		ArrayList<String> listVar = new ArrayList<String>(); 
		int theta = 1;
		double coef = 0.0;
 
		for (theta = IND_EMPTY; theta < IND_CD; theta++){
			listVar = varTheta.get(theta);
			
			switch(theta){
			case IND_EMPTY:
				coef = 1;
				break;
			case IND_C:
				coef = -1;
				break;
			case IND_D:
				coef = -1;
				break;
			case IND_CD:
				coef = 1 + restrictIIP.delta;
				break;
			}
			
			for (Iterator<String> iter = listVar.iterator(); iter.hasNext(); ){
				mapVarCoef.put(iter.next(), new Double(coef));
			}			
		}
				
		return mapVarCoef;
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
	private String constructVariableName(int theta, int typeVariable, int k, int i, int a){
		String result = "";
		result.concat(strHeaderVariable[typeVariable]);
		result.concat("(");
		
		ArrayList<String> nameComponent = new ArrayList<String>();
		
		nameComponent.add(strTheta[theta]);
		nameComponent.add(Integer.toString(k));
		
		if (typeVariable == VAR_X || typeVariable == VAR_U){
			nameComponent.add(Integer.toString(i));
			nameComponent.add(Integer.toString(a));
		}
		
		result.concat(Connector.join(",", nameComponent));
		result.concat(")");
				
		return result;	
	}
	
	
	/**
	 * 
	 * @return (total number of variables used in this BIP problem)
	 */
	public int getTotalVar(){
		return totalVar;
	}	
		
}
