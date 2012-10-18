package edu.ucsc.dbtune.bip.div;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Permutations;
import edu.ucsc.dbtune.util.Rt;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloLinearNumExprIterator;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

/**
 * This class contains common constraints that are used in DivBIP
 * 
 * @author Quoc Trung Tran
 *
 */
public class UtilConstraintBuilder 
{
    public static IloCplex cplex;
    public static List<IloNumVar> cplexVar;
    public static int numConstraints = 0;
    public static double constantRHSImbalanceConstraint = 0;
    
    /**
     * Compute coefficient for the total cost without failure,
     * computed as {@code (1 - alpha)^N}. Scale 
     * by a factor (1 - alpha) (n - 1)
     * 
     *  
     * @param alpha
     *      The failure factor
     * @param N
     *      The number of replica
     *      
     * @return
     *      The coefficient
     */
    public static double computeCoefCostWithoutFailure(double alpha, int N)
    {
        //return Math.pow(1 - alpha, N);
        return (1 - alpha);
    }
    
    public static double scaleDownFactor(double alpha, int N)
    {
        //return Math.pow(1 - alpha, N - 1);
        return 1;
    }
    
    /**
     * Compute coefficient for the total cost with 
     * at most one failure, 
     * computed as {@code alpha (1 - alpha)^(N-1)}
     *  
     * @param alpha
     *      The failure factor
     * @param N
     *      The number of replica
     *      
     * @return
     *      The coefficient
     */
    public static double computeCoefCostWithFailure(double alpha, int N)
    {
        //return alpha * Math.pow(1 - alpha, N - 1);
        return alpha;
    }
    
    /**
     * Round the given value into up to two decimal
     * 
     * @param num
     *      The given value     
     *       
     * @return
     *      The double value after round-up 2 digits
     */
    public static double round2(double num) 
    {
        double result = num * 100;
        result = Math.round(result);
        result = result / 100;
        return result;
    }
    
    /**
     * Impose the set of constraints when replacing the produce of two binary variables 
     * {@code idFirst} and {@code idSecond} by {@code idCombine}.
     *  
     * @param idCombine
     *      The ID of the combined variable
     * @param idFirst
     *      The ID of the first variable
     * @param idSecond
     *      The ID of the second variable
     *      
     * @throws IloException
     */
    public static void constraintCombineVariable(int idCombine, int idFirst, int idSecond)
                throws IloException
    {
        IloLinearNumExpr expr;
        
        // combine <= idFirst
        expr = cplex.linearNumExpr();
        expr.addTerm(1, cplexVar.get(idCombine));            
        expr.addTerm(-1, cplexVar.get(idFirst));
        cplex.addLe(expr, 0, "combine_" + numConstraints);
        numConstraints++;
        
        // combine <= idSecond
        expr = cplex.linearNumExpr();
        expr.addTerm(1, cplexVar.get(idCombine));            
        expr.addTerm(-1, cplexVar.get(idSecond));
        cplex.addLe(expr, 0, "combine_" + numConstraints);
        numConstraints++;
        
        // combine >= idFirst + idSecond - 1
        expr = cplex.linearNumExpr();
        expr.addTerm(1, cplexVar.get(idCombine));            
        expr.addTerm(-1, cplexVar.get(idFirst));
        expr.addTerm(-1, cplexVar.get(idSecond));
        cplex.addGe(expr, -1, "combine_" + numConstraints);
        numConstraints++;
    }
    
    
    /**
     * Add the constraint on the imbalance for the given two expressions 
     * (usually the load at two replicas)
     * 
     * @param expr1
     *      The first expression     
     * @param expr2
     *      The second expression
     * @param factor
     *      The imbalance factor.
     */
    public static void imbalanceConstraint(IloLinearNumExpr expr1, IloLinearNumExpr expr2, 
                                          double beta)
                   throws IloException
    {
        IloLinearNumExpr expr;
        
        // r1 - \beta x r2 <= constantRHS
        expr = cplex.linearNumExpr();
        expr.add(expr1); 
        expr.add(modifyCoef(expr2, -beta));
        cplex.addLe(expr, constantRHSImbalanceConstraint, "imbalance_replica_" + numConstraints);
        numConstraints++;
        
        // r2 - \beta x r1 <= constantRHS
        expr = cplex.linearNumExpr();
        expr.add(expr2);
        expr.add(modifyCoef(expr1, -beta));
        cplex.addLe(expr, constantRHSImbalanceConstraint, "imbalance_replica_" + numConstraints);
        numConstraints++;
    }
    
    /**
     * Add the constraint on the imbalance for the given two expressions 
     * (usually the load at two replicas): expr1 <= beta * expr2
     * 
     * @param expr1
     *      The first expression     
     * @param expr2
     *      The second expression
     * @param factor
     *      The imbalance factor.
     */
    public static void imbalanceConstraintOrder(IloLinearNumExpr expr1, IloLinearNumExpr expr2, 
                                           double beta)
                   throws IloException
    {
        IloLinearNumExpr expr = cplex.linearNumExpr();
        expr.add(expr1); 
        expr.add(modifyCoef(expr2, -beta));
        cplex.addLe(expr, 0, "imbalance_replica_" + numConstraints);
        numConstraints++;
    }
    
    /**
     * Modify all the coefficient in the formula by multiply with given factor
     * 
     * @param expr
     *      The expression that is modified
     * @param factor
     *      The factor to multiple with existing coefficient
     *      
     * @return
     *      The expression with modifying coefficient
     * @throws IloException
     */
    public static IloLinearNumExpr modifyCoef(IloLinearNumExpr expr, double factor) throws IloException
    {
        // not modify the coefficient
        if (factor == 1.0)
            return expr;
        
        IloNumVar var;
        double coef;
        IloLinearNumExprIterator iter;
        IloLinearNumExpr result = cplex.linearNumExpr();
        
        iter = expr.linearIterator();
        while (iter.hasNext()) {
            var = iter.nextNumVar();
            coef = iter.getValue();
            result.addTerm(var, round2(factor * coef));
        }
        
        return result;
    }
    
    /**
     * Compute the value of the given expression
     * @param expr
     *      the given expression
     *      
     * @return
     *      The value
     *      
     * @throws Exception 
     */
    public static double computeVal(IloLinearNumExpr expr) throws Exception
    {
        IloLinearNumExprIterator iter;
        
        IloNumVar var;
        double coef;
        double cost;
        
        cost = 0.0;        
        iter = expr.linearIterator();
        
        while (iter.hasNext()) {
            var = iter.nextNumVar();
            coef = iter.getValue(); 
            
            if (coef > 0.0 && cplex.getValue(var) > 0)
                cost += coef * cplex.getValue(var);
            
        }
        
        // issue #281: some variables are assigned value small value (e.g., 3.5976098620712736E-12)
        // workaround: cast very small value to 0
        if (cost < 1.0)
            cost = 0.0;
        
        return cost;
    }
    
    /**
     * Compute the value of the given expression
     * @param expr
     *      the given expression
     *      
     * @return
     *      The value
     *      
     * @throws Exception 
     */
    public static double showComputeVal(IloLinearNumExpr expr) throws Exception
    {
        IloLinearNumExprIterator iter;
        
        IloNumVar var;
        double coef;
        double cost;
        
        cost = 0.0;        
        iter = expr.linearIterator();
        
        while (iter.hasNext()) {
            var = iter.nextNumVar();
            coef = iter.getValue(); 
            
            if (coef > 0.0 && cplex.getValue(var) > 0) {
                cost += coef * cplex.getValue(var);
                Rt.p(" coef: " + coef + " var: " + var.getName()
                     + " value from CPLEX: " + cplex.getValue(var));
            }
            
        }
        
        // issue #281: some variables are assigned value small value (e.g., 3.5976098620712736E-12)
        // workaround: cast very small value to 0
        if (cost < 1.0)
            cost = 0.0;
        
        return cost;
    }
    
    /**
     * Compute the maximum ratio between any two elements in the list.
     * 
     * @param costs
     *      The list of value
     * @return
     *      The maximum ratio
     */
    public static double maxRatioInList(List<Double> costs)
    {
        double maxRatio = -1;
        double ratio;
        
        for (int r1 = 0; r1 < costs.size(); r1++)
            for (int r2 = r1 + 1; r2 < costs.size(); r2++) {
                
                ratio = (double) costs.get(r1) / costs.get(r2);
                if (ratio > maxRatio)
                    maxRatio = ratio;
                
                ratio = (double) costs.get(r2) / costs.get(r1);
                if (ratio > maxRatio)
                    maxRatio = ratio;
            }
        
        return maxRatio;
    }
    
    
    /**
     * Compute the cost to deploy from the {@code oldConf} to {@code newConf}. 
     * 
     * @param sourceConf
     *      The old configuration.
     * @param destinationConf
     *      The new configuration to deploy.
     * 
     * @return
     *      Deployment cost
     */
    /*
    public static double computeDeploymentCost(DivConfiguration sourceConf, 
                                               DivConfiguration destinationConf)
         
    {
        double deploymentCost;
        
        deploymentCost = 0.0;
        
        //System.out.println(" Source: \n" + sourceConf.briefDescription() + "\n"
          //                + " Destination: \n" + destinationConf.briefDescription());
        // The algorithm works as follows
        // For each configuration {@code target} of {@code destinationConf} 
        //     + Find the most ``match'' {@code source} in {@code sourceConf}
        //     + Compute the cost to transfer {@code source} to {@code target|
        // If (destiationConf.size() > sourceconf.size())
        //     + The remaining replica is computed based on its creation cost
        
        
        // Our current assumption: destinationConfg.size() < sourceConf.size()
        // i.e., shrinking replicas
        // TODO: handle expanding replicas
        /*
        List<Integer> listHasMatchedID;
        List<Integer> listTargetIDDecreaseNumberIndexes;
        List<SortableObject> lso;
        int sourceID;
        
        listHasMatchedID = new ArrayList<Integer>();
        listTargetIDDecreaseNumberIndexes = new ArrayList<Integer>();
        lso = new ArrayList<SortableObject>();
        
        
        // sort each replica in terms of the number of replicas that each has.
        for (int targetID = 0; targetID < destinationConf.getNumberReplicas(); targetID++){
            
            SortableObject o = new SortableObject(targetID, 
                                    destinationConf.indexesAtReplica(targetID).size());
            lso.add(o);
        }
        Collections.sort(lso);
        
        for (int i = lso.size() - 1; i > -1; i--) 
            listTargetIDDecreaseNumberIndexes.add(lso.get(i).getID());
        
        
        for (int targetID : listTargetIDDecreaseNumberIndexes){
         
            sourceID = findMatchReplica(targetID, listHasMatchedID, 
                                             sourceConf, destinationConf);
            
            if (sourceID == -1)
                throw new RuntimeException("Haven't handled the scenarios " +
                        "with expanding replicas yet.");
            
            //System.out.println(" source ID = " + sourceID
              //                  + " \n " + " target ID = " + targetID);
            deploymentCost += computeTransferCost(sourceConf.indexesAtReplica(sourceID), 
                                                  destinationConf.indexesAtReplica(targetID));
            
            listHasMatchedID.add(sourceID);
        }
                    
        return deploymentCost;
        
    }
    */
    public static double computeDeploymentCost(DivConfiguration sourceConf, 
            DivConfiguration destinationConf)

    {
        double deploymentCost;
        deploymentCost = 0.0;
        int sourceID;
        
        double minCost = 9999999999.0;
        
        System.out.println("L292 (Utils constraint builder), " +
        		            " source conf: " + sourceConf.getNumberReplicas()
                            + " desc. conf: " + destinationConf.getNumberReplicas());
        
        // find all permuation of a set [0,1,... sourceConf.size() - 1]
        Permutations<Integer> per;
        List<Integer> sourceIDs = new ArrayList<Integer>();
        List<Integer> ids;
       
        for (int i = 0; i < sourceConf.getNumberReplicas(); i++)
            sourceIDs.add(i);
        
        per = new Permutations<Integer>(sourceIDs, destinationConf.getNumberReplicas());
        
        while (per.hasNext()) {
       
            ids = per.next();
            for (int i = 0; i < ids.size(); i++) {
                sourceID = ids.get(i);
                deploymentCost += computeTransferCost(sourceConf.indexesAtReplica(sourceID)
                                    , destinationConf.indexesAtReplica(i));           
            }
            
            if (minCost > deploymentCost)
                minCost = deploymentCost;
            
        }
    
        return minCost;
    }
        
    
    /**
     * Find the matching ID for the given {@code targetID} replica.
     * 
     * @param targetID
     *      The target replica that needs to be matched.           
     * @param listHasMatchedID
     *      A list of replicas in the source that have been matched to some replica     
     *      of the target
     * @param sourceConf
     *      The source configuration
     * @param destinationConf
     *      The target configuration
     *      
     * @return
     *      The matching replica ID of source, 
     *      or -1, if we cannot find.
     */
    /*
    private static int findMatchReplica(int targetID, List<Integer> listHasMatchedID, 
                                 DivConfiguration sourceConf, 
                                 DivConfiguration destinationConf)
    {
        int idMaxMatching;
        int numMaxMatching; 
        
        numMaxMatching = -1;
        idMaxMatching = -1;
        Set<Index> intersection;
        Set<Index> destination;
        
        destination = new HashSet<Index>(destinationConf.indexesAtReplica(targetID));
        
        for (int sourceID = 0; sourceID < sourceConf.getNumberReplicas(); sourceID++) {
             
            if (listHasMatchedID.contains(sourceID))
                continue;
            
            intersection = new HashSet<Index>(sourceConf.indexesAtReplica(sourceID));
            intersection.retainAll(destination);
            
            if (intersection.size() > 0 && 
                   intersection.size() > numMaxMatching) {
                numMaxMatching = intersection.size();
                idMaxMatching = sourceID;
            }
            
        }
        
        return idMaxMatching;
    }
    */
    
    /**
     * Compute the transfer cost from the {@code source} configuration to {@code destination}
     * configuration.
     *  
     * @param source
     *      The source configuration
     * @param destination
     *      The destination configuration, on which we are going to deploy
     *       
     * @return
     *      The transfer cost
     */
    private static double computeTransferCost(Set<Index> source, Set<Index> destination)
    {
        // There are three types of operations
        // 
        //   + Index remains in the system: cost = 0
        //   + Index is dropped           : cost = 0
        //   + Index is materialized      : cost = createCost(index)
        // 
        double transferCost = 0.0;        
        Set<Integer> retain = new HashSet<Integer>();
        
        for (Index index : destination)
            retain.add(index.getId());
        
        for (Index index : source)
            retain.remove(index.getId());
        
        for (Index index : destination) 
            if (retain.contains(index.getId()))
                transferCost += index.getCreationCost();
        
        /*    
        System.out.println( " source " + source.size()
                         + " dest: " + destination.size()
                         + " mat: " + retain.size()
                         + " cost: " + transferCost );
         */
        return transferCost;
    }
    
    
}
