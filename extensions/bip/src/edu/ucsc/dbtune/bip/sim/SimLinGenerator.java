package edu.ucsc.dbtune.bip.sim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.util.BIPVariableCreator;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.Connector;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.MatIndex;
import edu.ucsc.dbtune.bip.util.MatIndexPool;
import edu.ucsc.dbtune.bip.util.MultiQueryPlanDesc;

public class SimLinGenerator 
{
    private CPlexBuffer buf;
	private List<String> listCwq;
	private List<String> listVar;
	private List<MultiQueryPlanDesc> listQueryPlanDescs;
	private int W;
	private double B;
	private int numConstraints;
	private BIPVariableCreator varCreator;
	
	SimLinGenerator(final String prefix, final List<MultiQueryPlanDesc> listQueryPlanDecs, final int W, final double B)
	{		
		this.listQueryPlanDescs = listQueryPlanDecs;
		this.W = W;
		this.B = B;
		this.varCreator = new BIPVariableCreator();		
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
	 *  <li> Well-behaved schedule constraints </li>
     *  <li> Index present constraints </li>
	 * 	<li> Atomic constraints </li> 
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
    	
    	// 1. Each index is created/dropped one time
        buildWellBehavedScheduleConstraints();
        
        // 2. Index interaction (12)
        buildIndexPresentConstraints();
        
    	// 3. Construct the query cost at each window time
    	buildQueryCostWindow();
    	
    	// 4. Atomic constraints
    	buildAtomicConstraints();    	
    	
    	// 5. Space constraints
    	buildSpaceConstraints();
    	
    	// 6. Optimal constraint
    	buildObjective();
    	
    	// 7. binary variables
    	binaryVariableConstraints();
    	
    	buf.close();
    	    	
        listener.onLogEvent(LogListener.BIP, "Built IIP program");
    }
	/**
     * A well-behaved schedule satisfies the following three conditions:
     * <p>
     * <ol> 
     *  <li> Indexes in Sremain remain in the DBMS </li>
     *  <li> Indexes in Sin are created one time </li>
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
        for (int idx = MatIndexPool.getStartPosCreateType(); idx < MatIndexPool.getStartPosDropType(); idx++) {
            linList.clear();
            for (int w = 0; w < W; w++) {
                var = varCreator.constructVariableName(BIPVariableCreator.VAR_CREATE, w, idx, 0, 0, 0);
                listVar.add(var);
                linList.add(var);
            }
            buf.getCons().println("well_behaved_11a_" + numConstraints  
                    + ": " + Connector.join(" + ", linList)                     
                    + " = 1");
            numConstraints++;           
        }
        
        // for TYPE_DROP index
        for (int idx = MatIndexPool.getStartPosDropType(); idx < MatIndexPool.getStartPosRemainType(); idx++) {
            linList.clear();
            for (int w = 0; w < W; w++) {
                var = varCreator.constructVariableName(BIPVariableCreator.VAR_DROP, w, idx, 0, 0, 0);
                listVar.add(var);
                linList.add(var);
            }
            buf.getCons().println("well_behaved_11b_" + numConstraints  
                    + ": " + Connector.join(" + ", linList)                     
                    + " = 1");
            numConstraints++;           
        }
        // for TYPE_REMAIN: we do not create variables of this types
    }
    
    /**
     * The present of an index I at a particular window w is defined as follows:
     * <p>
     * <ol> 
     *  <li> If I is a created index, then I is present at w if and only w i HAS been 
     * created at SOME window point between 0 and w </li>
     * 
     *  <li> If I is a dropped index, then I is present at w if and only w i has NOT been 
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
        String var_present = "", var_create = "";
        
        // for TYPE_CREATE index
        for (int idx = MatIndexPool.getStartPosCreateType(); idx < MatIndexPool.getStartPosDropType(); idx++) {
            
            for (int w = 0; w < W; w++) {
                var_present = varCreator.constructVariableName(BIPVariableCreator.VAR_PRESENT, w, idx, 0, 0, 0);
                listVar.add(var_present);
                linList.clear();
                for (int j = 0; j <= w; j++) {
                    var_create = varCreator.constructVariableName(BIPVariableCreator.VAR_CREATE, j, idx, 0, 0, 0);
                    linList.add(var_create);
                }
                
                buf.getCons().println("index_present_12a_" + numConstraints  
                        + ": " + Connector.join(" + ", linList)                     
                        + " - " + var_present + " = 0 ");
                numConstraints++;   
            }       
        }
        
        // for TYPE_DROP index
        for (int idx = MatIndexPool.getStartPosDropType(); idx < MatIndexPool.getStartPosRemainType(); idx++) {
            for (int w = 0; w < W; w++) {
                var_present = varCreator.constructVariableName(BIPVariableCreator.VAR_PRESENT, w, idx, 0, 0, 0);
                listVar.add(var_present);
                linList.clear();
                for (int j = 0; j <= w; j++) {
                    var_create = varCreator.constructVariableName(BIPVariableCreator.VAR_DROP, j, idx, 0, 0, 0);
                    linList.add(var_create);
                }
                buf.getCons().println("index_present_12b_" + numConstraints  
                        + ": " + Connector.join(" + ", linList)                     
                        + " + " + var_present + " = 1 ");
                numConstraints++;                   
            }       
        }
        
        // present_{a,w} = 1 for $a \in Sremain$, which we do not enforce here
    }
    
	/**
	 * Build cost function of each query in each window w
	 * Cqw = \sum_{k \in [1, Kq]} \beta_{qk} y(w,q,k) + 
	 *      + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x(w,q,k,i,a) \gamma_{q,k,i,a}
	 *      
	 *  {\b Note} Add index of type Y, X, and S into the list of variables    
	 */
	private void buildQueryCostWindow()
	{
		List<String> linList = new ArrayList<String>();
		int w, q, k, i, a;
		String var, element;
		String Cwq;
		
		for (MultiQueryPlanDesc desc : listQueryPlanDescs){
		    q = desc.getId();
		    
			for (w = 0; w < W; w++) {
				// Internal plan
				linList.clear();
			
				for (k = 0; k < desc.getNumPlans(); k++) {
					var = varCreator.constructVariableName(BIPVariableCreator.VAR_Y, w, q, k, 0, 0);
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
							var = varCreator.constructVariableName(BIPVariableCreator.VAR_X, w, q, k, i, a);
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
		
		for (int ga = 0; ga < MatIndexPool.getTotalIndex(); ga++) {
            for (w = 0; w < W; w++) {
                String var_s = varCreator.constructVariableName(BIPVariableCreator.VAR_S, w, ga, 0, 0, 0);
                listVar.add(var_s);
            }
        }
	}
	
	/**
	 * Standard set of atomic constraint of INUM
	 * 
	 * {\b Note} Add variables of type S into the list of variables
	 * 
	 */
	private void buildAtomicConstraints()
	{
		List<String> linList = new ArrayList<String>();
		int w, q, k, i, a, ga;
		String var_y, var_x, var_s, var_present;		
		
		for (MultiQueryPlanDesc desc : listQueryPlanDescs){
            q = desc.getId();
		
            for (w = 0; w < W; w++) {
				linList.clear();
				// (1) \sum_{k \in [1, Kq]}y^{theta}_k = 1
				for (k = 0; k < desc.getNumPlans(); k++) {
					linList.add(varCreator.constructVariableName(BIPVariableCreator.VAR_Y, w, q, k, 0, 0));
				}
				buf.getCons().println("atomic_13a_" + numConstraints + ": " + Connector.join(" + ", linList) + " = 1");
				numConstraints++;
			
				// (2) \sum_{a \in S_i} x(theta, k, i, a) = y(theta, k)
				for (k = 0; k < desc.getNumPlans(); k++) {
					var_y = varCreator.constructVariableName(BIPVariableCreator.VAR_Y, w, q, k, 0, 0);
					
					for (i = 0; i < desc.getNumSlots(); i++) {
					    
					    if (desc.isReferenced(i) == false) {
					        continue;
					    }
					    
						linList.clear();
						for (a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
							var_x = varCreator.constructVariableName(BIPVariableCreator.VAR_X, w, q, k, i, a);
							linList.add(var_x);
							IndexInSlot iis = new IndexInSlot(q,i,a);
							ga = MatIndexPool.getGlobalIdofIndexInSlot(iis);
							var_s = varCreator.constructVariableName(BIPVariableCreator.VAR_S, w, ga, 0, 0, 0);
							
							// (3) s_a^{theta} \geq x_{kia}^{theta}
							buf.getCons().println("atomic_14a_" + numConstraints + ":" 
												+ var_x + " - " 
												+ var_s
												+ " <= 0 ");
							numConstraints++;
						}
						buf.getCons().println("atomic_13b_" + numConstraints  
											+ ": " + Connector.join(" + ", linList) 
											+ " - " + var_y
											+ " = 0");
						numConstraints++;
					}
				}		
			}
		}
		
		// s(w,ai) <= present(w,i)
        for (int idx = MatIndexPool.getStartPosCreateType(); idx < MatIndexPool.getStartPosRemainType(); idx++) {
            for ( w = 0; w < W; w++) {
                var_present = varCreator.constructVariableName(BIPVariableCreator.VAR_PRESENT, w, idx, 0, 0, 0);
                var_s = varCreator.constructVariableName(BIPVariableCreator.VAR_S, w, idx, 0, 0, 0);
                buf.getCons().println("atomic_14b_" + numConstraints  
                                        + ": " + var_s + " - " + var_present + " <= 0 ");
                numConstraints++;   
            }
        }
        // s(w,a) <= present(w,a) for a \in Sremain is obvious
        // since present(w,a) = 1. Thus, we don't not impose these constraints
	}
	
	
	
	/**
	 * Impose space constraint on the materialized indexes at all window times
	 * 
	 */
	private void buildSpaceConstraints()
	{
		List<String> linList = new ArrayList<String>();
		String var_create = "";
		int idx, w;
	
		// for TYPE_CREATE
		for (w = 0; w < W; w++) {
			linList.clear();
			for (idx = MatIndexPool.getStartPosCreateType(); idx < MatIndexPool.getStartPosDropType(); idx++) {
				var_create = varCreator.constructVariableName(BIPVariableCreator.VAR_CREATE, w, idx, 0, 0, 0);
				double sizeindx = MatIndexPool.getMatIndex(idx).getMatSize();
				linList.add(Double.toString(sizeindx) + var_create);
			}
			buf.getCons().println("space_constraint" + numConstraints  
					+ " : " + Connector.join(" + ", linList) 					
					+ " <= " + B);
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
		int type = BIPVariableCreator.getVarType(name);
		MatIndex index = null;
		if (type == BIPVariableCreator.VAR_CREATE || type == BIPVariableCreator.VAR_DROP) { 
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
