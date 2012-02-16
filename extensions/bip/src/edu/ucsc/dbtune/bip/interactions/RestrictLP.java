package edu.ucsc.dbtune.bip.interactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.InumQueryPlanDesc;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.workload.SQLStatement;

public class RestrictLP extends RestrictIIP 
{
    private List<QueryPlanDesc> listDescs;
    private String              prefixIndStatement  = "Ind_";
    private String              prefixInteractionVar = "ii_";
    private String              indStatement1;
    private String              indStatement2;    
    private String              interactionVar1;
    private String              interactionVar2;
    private List<String>        listIndStatements;
    private List<Integer>       listRelationC;
    private List<Integer>       listRelationD;
    
    
    public RestrictLP(QueryPlanDesc desc, LogListener logger, double delta,
                     Index indexc, Index indexd, 
                     Set<Index> candidateIndexes, int ic, int id) 
    {
        super(desc, logger, delta, indexc, indexd, candidateIndexes, ic, id);    
        listIndStatements = new ArrayList<String>();
    }
    
    
    public void setListQueryDescriptions(List<QueryPlanDesc> listDescs,
                                         List<Integer> listRelationC,
                                         List<Integer> listRelationD)
    {
        this.listDescs     = listDescs;
        this.listRelationC = listRelationC;
        this.listRelationD = listRelationD;
    }
    
    public boolean solve()
    {
        // 2. Construct BIP
        logger.setStartTimer();
        initializeBuffer();
        buildBIP();
        logger.onLogEvent(LogListener.EVENT_FORMULATING_BIP);
        
        // 3. Solve the first BIP        
        logger.setStartTimer();        
        boolean isInteracting = cplex.solve(buf.getLpFileName());
        logger.onLogEvent(LogListener.EVENT_SOLVING_BIP);
        
        // clear the model
        cplex.clearModel();
        
        return isInteracting;
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
    @Override
    protected void buildBIP() 
    {
        numConstraints = 0;
        
        for (int i = 0; i < listDescs.size(); i++) {
            
            desc = listDescs.get(i);
            ic   = listRelationC.get(i);
            id   = listRelationD.get(i);
            
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
            
            boolean isBothInteraction = true;
            
            // 3. Index interaction 
            if (ic != id)
                indexInteractionConstraint1();
            else 
                isBothInteraction = false;
            
            indexInteractionConstraint2(isBothInteraction);
            
            // 4. Binary variables
            binaryVariableConstraints(isBothInteraction);
        }
        
        // add one more constraint for the ind variable
        buf.getCons().add(Strings.concatenate(" + ", listIndStatements)                        
                        + " = " + (listIndStatements.size() - 1));
        
        try {
            buf.writeToLpFile();
        }
        catch (IOException e) {
            throw new RuntimeException("Cannot concantenate text files that store BIP.");
        }
    }
    
    protected void binaryVariableConstraints(boolean isBothInteraction)
    {
        if (isBothInteraction)
            buf.getBin().add(poolVariables.enumerateList(10) + " "
                             + indStatement1 + " "
                             + indStatement2 + "\n"); 
        else 
            buf.getBin().add(poolVariables.enumerateList(10)
                            + " " + indStatement2 + "\n");
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
        
        interactionVar1 = prefixInteractionVar + desc.getStatementID() + "_1";
        buf.getCons().add(CTheta.get(IND_EMPTY) 
                              + " + " + Strings.concatenate(" + ", listcd) 
                              + " - " + Strings.concatenate(" - ", elementCTheta.get(IND_C))
                              + " - " + Strings.concatenate(" - ", elementCTheta.get(IND_D))
                              + " - " + interactionVar1
                              + " = 0 ");
        
        buf.getBound().add(interactionVar1 + " >= - " + InumQueryPlanDesc.BIP_MAX_VALUE);
    }
    
    /**
     * Replace the index interaction constraint by the following:
     * cost(X,c) + cost(X,d) - cost(X) + (delta - 1) cost(X,c,d)  <= 0
     * 
     * @return
     *      {@code true} if CPLEX returns a solution,
     *      {@code false} otherwise
     */
    protected void indexInteractionConstraint2(boolean isBothInteraction)
    {   
        String connectorCD = delta - 1 > 0 ? " + " : " - ";
        ArrayList<String> listcd = new ArrayList<String>();
        Object found;
        
        double coef = Math.abs(delta - 1);
        double realCoef;
        
        for (String var : varTheta.get(IND_CD)) {
            found = coefVarYXTheta.get(var);
            if (found != null) {
                realCoef = (Double) found * coef;
                listcd.add(realCoef + var);
            }   
        }
        
        interactionVar2 = prefixInteractionVar + desc.getStatementID() + "_2";
            
        buf.getCons().add(
                CTheta.get(IND_C)
                + " + " + CTheta.get(IND_D)
                + " - " + Strings.concatenate(" - ", elementCTheta.get(IND_EMPTY))
                + connectorCD + Strings.concatenate(connectorCD, listcd) 
                + " - " + interactionVar2 + " = 0 ");
        buf.getBound().add(interactionVar2 + " >= - " + InumQueryPlanDesc.BIP_MAX_VALUE);
        indStatement2 = prefixIndStatement + desc.getStatementID() + "_2";
        
        if (isBothInteraction) {
            // We add two more constraints:
            //
            // interactionVar1 - INF * indStatement1 <= 0
            indStatement1 = prefixIndStatement + desc.getStatementID() + "_1";
            
            buf.getCons().add(interactionVar1 + " - " + InumQueryPlanDesc.BIP_MAX_VALUE 
                              + indStatement1 + " <= 0");
            listIndStatements.add(indStatement1);
        }
        
        // interactionVar2 - INF * indStatement2 <= 0
        buf.getCons().add(interactionVar2 + " - " + InumQueryPlanDesc.BIP_MAX_VALUE 
                + indStatement2 + " <= 0");
        listIndStatements.add(indStatement2);
    }
}
