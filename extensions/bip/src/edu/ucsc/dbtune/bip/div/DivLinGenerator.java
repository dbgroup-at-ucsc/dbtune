package edu.ucsc.dbtune.bip.div;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.util.MatIndexPool;
import edu.ucsc.dbtune.bip.util.MatIndex;
import edu.ucsc.dbtune.bip.util.MultiQueryPlanDesc;
import edu.ucsc.dbtune.bip.util.BIPVariableCreator;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.Connector;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.bip.util.LogListener;

public class DivLinGenerator 
{
    private CPlexBuffer buf;
    private List<String> listCrq;
    private List<String> listVar;
    private List<MultiQueryPlanDesc> listQueryPlanDescs;
    private int Nreplicas;
    private int loadfactor;
    private double B;
    private int numConstraints;
    private BIPVariableCreator varCreator;
    
    
    DivLinGenerator(final String prefix, final List<MultiQueryPlanDesc> listQueryPlanDecs, 
                    final int Nreplicas, final int loadfactor, final double B)
    {       
        this.listQueryPlanDescs = listQueryPlanDecs;
        this.Nreplicas = Nreplicas;
        this.B = B;
        this.loadfactor = loadfactor;
        this.varCreator = new BIPVariableCreator();     
        listCrq = new ArrayList<String>();  
        listVar = new ArrayList<String>();
        
        try {
            this.buf = new CPlexBuffer(prefix);
        }
        catch (IOException e) {
            System.out.println(" Error in opening files " + e.toString());          
        }
    }
    
    /**
     * The function builds the BIP Divergent Index Tuning Problem:
     * <p>
     * <ol>
     *  <li> Atomic constraints </li>
     *  <li> Top-m best cost constraints </li>
     *  <li> Space constraints </li>
     * </ol>
     * </p>   
     * 
     * @param listener
     *      Log the building process
     * 
     * @throws IOException
     */
    public final void build(final LogListener listener) throws IOException 
    {
        listener.onLogEvent(LogListener.BIP, "Building IIP program...");
        numConstraints = 0;
        
        // 1. Construct the query cost at each replica
        buildQueryCostReplica();
        
        // 2. Atomic constraints
        buildAtomicConstraints();       
        
        // 3. Top-m best cost 
        buildTopmBestCostConstraints();
        
        // 4. Space constraints
        buildSpaceConstraints();
        
        // 6. Optimal constraint
        buildObjective();
        
        // 7. binary variables
        binaryVariableConstraints();
        
        buf.close();
                
        listener.onLogEvent(LogListener.BIP, "Built IIP program");
    }
    
    
    /**
     * Build cost function of each query in each window w
     * Cqr = \sum_{k \in [1, Kq]} \beta_{qk} y(r,q,k) + 
     *      + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x(r,q,k,i,a) \gamma_{q,k,i,a}
     * {\b Note}: Add variables of type Y, X, S into the list of variables     
     */
    private void buildQueryCostReplica()
    {
        List<String> linList = new ArrayList<String>();
                
        for (MultiQueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
            
            for (int r = 0; r < Nreplicas; r++) {
                // Internal plan
                linList.clear();
            
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    String var = varCreator.constructVariableName(BIPVariableCreator.VAR_Y, r, q, k, 0, 0);
                    String element = Double.toString(desc.getInternalPlanCost(k)) + var;
                    linList.add(element);               
                    listVar.add(var);
                }
                String Cwq  = Connector.join(" + ", linList);          
                        
                // Index access cost
                linList.clear();            
                for (int k = 0; k < desc.getNumPlans(); k++) {              
                    for (int i = 0; i < desc.getNumSlots(); i++) {  
                        for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
                            String var = varCreator.constructVariableName(BIPVariableCreator.VAR_X, r, q, k, i, a);
                            String element = Double.toString(desc.getIndexAccessCost(k, i, a)) + var;
                            linList.add(element);                       
                            listVar.add(var);
                        }
                    }
                }       
                Cwq = Cwq + " + " + Connector.join(" + ", linList);
                listCrq.add(Cwq);
            }
        }
        
        for (int ga = 0; ga < MatIndexPool.getTotalIndex(); ga++) {
            for (int r = 0; r < Nreplicas; r++) {
                String var_s = varCreator.constructVariableName(BIPVariableCreator.VAR_S, r, ga, 0, 0, 0);
                listVar.add(var_s);
            }
        }
    }
    
    /**
     * Atomic configuration constraints:
     *     - (2a) is different from the standard of INUM
     * 
     */
    private void buildAtomicConstraints()
    {
        List<String> linList = new ArrayList<String>();
        
        for (MultiQueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
        
            for (int r = 0; r < Nreplicas; r++) {
                linList.clear();
                // (1) \sum_{k \in [1, Kq]}y^{r}_{qk} <= 1
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    linList.add(varCreator.constructVariableName(BIPVariableCreator.VAR_Y, r, q, k, 0, 0));
                }
                buf.getCons().println("atomic_2a_" + numConstraints + ": " + Connector.join(" + ", linList) + " <= 1");
                numConstraints++;
            
                // (2) \sum_{a \in S_i} x(r, q, k, i, a) = y(r, q, k)
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    String var_y = varCreator.constructVariableName(BIPVariableCreator.VAR_Y, r, q, k, 0, 0);
                    
                    for (int i = 0; i < desc.getNumSlots(); i++) {
                        
                        if (desc.isReferenced(i) == false) {
                            continue;
                        }
                        
                        linList.clear();
                        for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
                            String var_x = varCreator.constructVariableName(BIPVariableCreator.VAR_X, r, q, k, i, a);
                            linList.add(var_x);
                            IndexInSlot iis = new IndexInSlot(q,i,a);
                            int ga = MatIndexPool.getGlobalIdofIndexInSlot(iis);
                            String var_s = varCreator.constructVariableName(BIPVariableCreator.VAR_S, r, ga, 0, 0, 0);
                            
                            // (3) s_a^{r} \geq x_{qkia}^{r}
                            buf.getCons().println("atomic_2c_" + numConstraints + ":" 
                                                + var_x + " - " 
                                                + var_s
                                                + " <= 0 ");
                            numConstraints++;
                        }
                        buf.getCons().println("atomic_2b_" + numConstraints  
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
     * Top-m best cost constraints
     * 
     */
    private void buildTopmBestCostConstraints()
    {
        List<String> linList = new ArrayList<String>();
        
        for (MultiQueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
        
            linList.clear();
            for (int r = 0; r < Nreplicas; r++) {
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    linList.add(varCreator.constructVariableName(BIPVariableCreator.VAR_Y, r, q, k, 0, 0));
                }
            }
            buf.getCons().println("topm_3a_" + numConstraints + ": " + Connector.join(" + ", linList) + " = " + loadfactor);
            numConstraints++;
        }
    }
    
    /**
     * Impose space constraint on the materialized indexes at all window times
     * 
     */
    private void buildSpaceConstraints()
    {
        List<String> linList = new ArrayList<String>();
        
        for (int r = 0; r < Nreplicas; r++) {
            linList.clear();
            for (int idx = 0; idx < MatIndexPool.getTotalIndex(); idx++) {
                String var_create = varCreator.constructVariableName(BIPVariableCreator.VAR_S, r, idx, 0, 0, 0);
                double sizeindx = MatIndexPool.getMatIndex(idx).getMatSize();
                linList.add(Double.toString(sizeindx) + var_create);
            }
            buf.getCons().println("space_constraint_4" + numConstraints  
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
        buf.getObj().println(Connector.join(" + ", listCrq));
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
     * Each variable related to an index is in the form: s(replica, global id): 
     *   + The replica is the string between the first '(' and the first ','
     *   + The index id is after ',' and before the last character ')'
     *   
     * @param name
     *      Variable name
     * 
     * @return
     *      The corresponding @MatIndex with the value of replica
     */
    public static MatIndex deriveMatIndex(String name)
    {
        int type = BIPVariableCreator.getVarType(name);
        MatIndex index = null;
        if (type == BIPVariableCreator.VAR_S) { 
            int openBracket, comma;
            openBracket = name.indexOf("(");
            comma = name.indexOf(",");
            String strReplica = name.substring(openBracket + 1, comma);
            int replicaID = Integer.parseInt(strReplica);
            String strId = name.substring(comma + 1, name.length() - 1);
            int idxID = Integer.parseInt(strId);
            index = MatIndexPool.getMatIndex(idxID);
            index.setReplicaID(replicaID);
        }
        
        return index; 
    }
}
