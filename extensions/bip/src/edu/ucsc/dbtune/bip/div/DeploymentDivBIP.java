package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;

import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeVal;

public class DeploymentDivBIP extends DivBIP 
{
    public static final String OPTIMIZE_TOTAL_COST = "optimize.total.cost";
    public static final String OPTIMIZE_DEPLOY_COST = "optimize.deploy.cost";
    
    private double  upperDeployCost;
    private double  upperTotalCost;
    
    private String typeOptimize;
    
    /**
     * Type of optimize constraints: either optimize total cost or deployment cost
     * 
     * @param type
     */
    public DeploymentDivBIP(String type)
    {
        typeOptimize = type;
    }
    
    /**
     * todo
     * @param cost
     */
    public void setUpperDeployCost(double cost) 
    {
        upperDeployCost = cost;
    }
    
    
    /**
     * todo
     * @param cost
     */
    public void setUpperTotalCost(double cost) 
    {
        upperTotalCost = cost;
    }
    
    @Override
    protected void buildBIP() 
    {   
        if (typeOptimize.equals(OPTIMIZE_TOTAL_COST))
            constraintDeployCost();
        else if (typeOptimize.equals(OPTIMIZE_DEPLOY_COST))
            constraintTotalCost();
    }
    
    /**
     * The deployment cost is constraint
     */
    protected void constraintDeployCost()
    {
        IloLinearNumExpr expr;
        numConstraints = 0;
        super.buildBIP();
        
        try {            
            expr = super.deploymentConstraint();
            cplex.addLe(expr, upperDeployCost, "deployment_" + numConstraints);
            numConstraints++;
        }     
        catch (IloException e) {
            e.printStackTrace();
        }
    }
    
    
    /**
     * The deployment cost is constraint
     */
    protected void constraintTotalCost()
    {
        IloLinearNumExpr expr;
        numConstraints = 0;

        
        try {            
            UtilConstraintBuilder.cplex = cplex;
            
            // 1. Add variables into list
            constructVariables();
            super.createCplexVariable(poolVariables.variables());
            
            // 2. Construct the query cost at each replica
            totalCostConstraint();
            
            // 3. Atomic constraints
            atomicConstraints();
            
            // 4. Use index constraint
            usedIndexConstraints();
            
            // 5. Top-m best cost 
            topMBestCostConstraints();
            
            // 6. Space constraints
            spaceConstraints();
            
            // 7. minimize deployment cost
            expr = super.deploymentConstraint();
            cplex.addMinimize(expr);
            numConstraints++;
        }     
        catch (IloException e) {
            e.printStackTrace();
        }
    }
    
    
    /**
     * Build the total cost formula, which is the summation of the costs of all replicas.      
     *     
     * @throws IloException 
     *      when there is error in calling {@code IloCplex} class
     */
    protected void totalCostConstraint() throws IloException
    {         
        IloLinearNumExpr total = cplex.linearNumExpr();
        
        for (int r = 0; r < nReplicas; r++) 
            total.add(replicaCost(r));
        
        cplex.addLe(total, upperTotalCost, "total_cost");        
    }
    
    /**
     * Retrieve the total cost
     * @return
     * @throws Exception
     */
    public double getTotalCost() throws Exception
    {
        IloLinearNumExpr total = cplex.linearNumExpr();
        
        for (int r = 0; r < nReplicas; r++) 
            total.add(replicaCost(r));
        
        return computeVal(total);
    }
}
