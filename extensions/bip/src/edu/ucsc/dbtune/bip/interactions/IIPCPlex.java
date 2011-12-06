package edu.ucsc.dbtune.bip;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ilog.concert.*;
import ilog.cplex.*;

import edu.ucsc.dbtune.metadata.Index;


/** 
 * The class is responsible for solving the RestrictIIP problem: 
 * 		For a particular query q, the IIP problem is to find 
 * 		pairs of indexes (c,d) that interact w.r.t. q; i.e., doi_q(c,d) >= delta.
 * 		The pair of indexes c and d must be relevant to the given input query q (?)   
 */

public class CPlex {

	private IloCplex cplex;
	private IloLPMatrix matrix;
	private IloNumVar [] vars;
	private IIPLinGenerator genIIP; 
	
	public static final Pattern patternIndexVariable = Pattern.compile("s*");
	public Map values;	
	
	
	/**
	 * Find pairs of indexes from the pool of candidate indexes that interact with each other
	 * 
	 * @param desc
	 *     Query plan description including (internal plan, access costs) derived from INUM
	 * @param candidateIndexes
	 *     A pool of candidate indexes  
	 * 
	 * @return
	 * 		The set of pairs of indexes that interact 
	 */
	public ArrayList<ArrayList<Index>> run(QueryDescPlan desc, ArrayList<Index> candidateIndexes)  {
		
		LogListener listener = new LogListener() {
            public void onLogEvent(String component, String logEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
        
        ArrayList<ArrayList<Index>> interactIndexes = new ArrayList< ArrayList<Index> >();
		String workloadName = "testwl", cplexFile = "", binFile = "", consFile = "";
		RestrictIIPParam restrictIIP;
		
		for (Iterator<Index> iterc = candidateIndexes.iterator(); iterc.hasNext(); )
		{
			for (Iterator<Index> iterd = candidateIndexes.iterator(); iterd.hasNext();)
			{
				if (iterc.hashCode() == iterd.hashCode())
				{
					continue;
				}
				
				// TODO: for each pair of (@iterc, @iterd), 
				// derive @ic, @id, @pos_c, @pos_d and store in @restrictIIP
				// Optimization: Check if @iterc, @iterd) has been already in the result
				// If yes, simply skip this pair of indexes
				try {
					restrictIIP = new RestrictIIPParam();
					restrictIIP.desc = desc;
					genIIP = new IIPLinGenerator(restrictIIP);
					
					// Build BIP for a particular (c,d, @desc)
					genIIP.build(listener);        
				
					cplexFile = workloadName + ".lp";
					binFile = workloadName + ".bin";
					consFile = workloadName + ".cons";
		        
		        	concat(cplexFile, workloadName+".obj", consFile, binFile);
		        }
				catch(IOException e){
		        	System.out.println("Error " + e);
		        }
				
				
				//  Load the corresponding CPLEX problem from the corresponding text file
		        try {		        
		            IloCplex cplex = new IloCplex(); 
		            cplex.setParam(IloCplex.IntParam.RootAlg, IloCplex.Algorithm.Auto);
		                      
		            // Read model from file with name @cplexFile into cplex optimizer object
		            cplex.importModel(cplexFile); 
		            
		            // Get the matrix and variable
		            matrix = getMatrix(cplex);
		            vars = matrix.getNumVars();

		            // Solve the model and record the solution into @candidateIndexes 
		            // if one was found
		            if ( cplex.solve() ) {
		               System.out.println("Solution status = " + cplex.getStatus() 
		            		   + " INTERACT (the FIRST interaction constraint.");
		               
		               // Add pair of index interaction into the result
		               ArrayList<Index> pairIndex = new ArrayList<Index>();
		               pairIndex.add( (Index)iterc);
		               pairIndex.add( (Index)iterd);
		               interactIndexes.add(pairIndex);
		               /*
		               IloNumVar []vars = getCostAndIndexVariables(cplex);
		               double[] vals = cplex.getValues(vars);
		               values = new HashMap();
		               for (int i = 0; i < vals.length; i++) {
		                   double value = vals[i];
		                   String name = vars[i].getName();

		                   if(value > 0)
		                       values.put(name, value);
		               }
		               */
		            }
		            else 
		            {   
		            	// Remove constraint the first constraint on index interaction (12)
		            	// Add the alternative constraint (13)

		            	int last_row_id = matrix.getNrows() - 1;		            	
		            	matrix.removeRow(last_row_id);		            	
		            	  
		            	double[] objvals = alternativeConstraintIndexInteraction();            	
		            	cplex.addLe(cplex.scalProd(vars, objvals), 0);
		            	
		            	if (cplex.solve()){
		            		System.out.println("Solution status = " + cplex.getStatus() 
		            				+ " INTERACT (the SECOND interaction constraint.");
		            		
		            		
				            ArrayList<Index> pairIndex = new ArrayList<Index>();
				            pairIndex.add( (Index)iterc.next());
				            pairIndex.add( (Index)iterd.next());
				            interactIndexes.add(pairIndex);
		            	} else {
		            		System.out.println("NO INTERACTION");
		            	}
		            }
		            cplex.end();
		         }
		         catch (IloException e) {
		            System.err.println("Concert exception caught: " + e);
		         }
			}
		}	
        
		return interactIndexes;
	}
	
	
	/**
	 * Derive the coefficients for the last constraint (13)
	 * 
	 * @return  
	 * 		An array of coefficient corresponding variables of the BIP matrix
	 */
	private double[] alternativeConstraintIndexInteraction(){
		HashMap<String,Double> mapVarCoef = genIIP.buildIndexInteractionConstraint2();
		double[] listCoef = new double[genIIP.getTotalVar()];
		
		IloNumVar var;
		int i;
		double coef;
		for (i = 0; i < vars.length; i++) {
            var = vars[i];
            coef = (Double)mapVarCoef.get(var.getName());
            listCoef[i] = coef;
		}
		return listCoef;		
	}
	               	               
	
	/**
	 * Determine the cost and index variables (?)
	 * 
	 * @return 
	 * 		An array of IloNumVar representing index variables
	 */
	private IloNumVar[] getCostAndIndexVariables() throws IloException {
        ArrayList<IloNumVar> variables = new ArrayList();
        int i;
        // @vars: list of variables in the problem definitions
        // @matrix: the linear programming matrix
	    // Both @vars and @matrix have been extracted in @run method,
        // after the model from the file is loaded
        
        for (i = 0; i < vars.length; i++) {
            IloNumVar var = vars[i];
            if(patternIndexVariable.matcher(var.getName()).matches()) {
                variables.add(var);
            }
        }

        return variables.toArray(new IloNumVar[variables.size()]);
    }

	/**
	 * Determine the matrix used in the BIP problem
	 * 
	 * @param cplex
	 * 		The model of the BIP problem
	 * @return
	 * 	    The matrix of @cplex	 * 
	 */	
	public IloLPMatrix getMatrix(IloCplex cplex) throws IloException {
        Iterator iter = cplex.getModel().iterator();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (o instanceof IloLPMatrix) {
                IloLPMatrix matrix = (IloLPMatrix) o;
                return matrix;
            }
        }
        return null;
    }

	
	/**
	 * Concatenate the contents from multiple input files into an output file:
	 *    Placing row by row in each input file into the output one
	 * 
	 * @param target
	 * 	    The name of the output file
	 * @param files
	 * 		The array of strings representing the name of the input files
	 * @throws IOException
	 */
	public static void concat(String target, String... files) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(target));
        for (int i = 0; i < files.length; i++) {
            String file = files[i];
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while((line = reader.readLine()) != null) {
                writer.println(line);
            }
            reader.close();
        }
        writer.close();
	}	
}
