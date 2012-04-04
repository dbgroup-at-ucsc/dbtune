package edu.ucsc.dbtune.bip.div;

import java.util.List;

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
        
        // r1 - \beta x r2 <= 0
        expr = cplex.linearNumExpr();
        expr.add(expr1); 
        expr.add(modifyCoef(expr2, -beta));
        cplex.addLe(expr, 0, "imbalance_replica_" + numConstraints);
        numConstraints++;
        
        // r2 - \beta x r1 <= 0
        expr = cplex.linearNumExpr();
        expr.add(expr2);
        expr.add(modifyCoef(expr1, -beta));
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
        IloNumVar        var;
        double           coef;
        IloLinearNumExprIterator iter;
        IloLinearNumExpr result = cplex.linearNumExpr();
        
        iter = expr.linearIterator();
        while (iter.hasNext()) {
            var = iter.nextNumVar();
            coef = iter.getValue();
            result.addTerm(var, factor * coef);
        }
        
        return result;
    }
    
}
