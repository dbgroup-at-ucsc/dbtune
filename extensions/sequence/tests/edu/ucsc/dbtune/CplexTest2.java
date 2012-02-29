package edu.ucsc.dbtune;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.seq.utils.Rt;
import ilog.concert.*;
import ilog.cplex.*;

public class CplexTest2 {
    public static void main2(String[] args) throws Exception {
        System.loadLibrary("cplex122");
        LogListener logger = LogListener.getInstance();

        IloCplex cplex = new IloCplex();
        // allow the solution differed 5% from the actual optimal value
        cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
        // not output the log of CPLEX
        // cplex.setOut(null);
        // not output the warning
        // cplex.setWarning(null);

        IloNumVar[] x = cplex.numVarArray(3, -100.0, 100.0);
        IloNumExpr expr = cplex.sum(x[0], cplex.prod(2.0, x[1]), cplex.prod(
                3.0, x[2]));
        IloLinearNumExpr expr2 = cplex.linearNumExpr();
        expr2.addTerm(1.0, x[0]);
        expr2.addTerm(2.0, x[1]);
        expr2.addTerm(3.0, x[2]);

        IloObjective obj = cplex.minimize(expr);
        cplex.add(obj);
        // cplex.addMinimize(expr);
        cplex.addLe(cplex.sum(cplex.negative(x[0]), x[1], x[2]), 20);
        cplex.solve();
        Object objVal = cplex.getObjValue();
        double[] xval = cplex.getValues(x);
        for (int i = 0; i < xval.length; i++) {
            Rt.p(xval[i]);
        }
    }

    public static void main(String[] args) throws Exception {
        main2(args);
        System.exit(0);
        try {
            IloCplex cplex = new IloCplex();
            double[] lb = { 0.0, 0.0, 0.0 };
            double[] ub = { 40.0, Double.MAX_VALUE, Double.MAX_VALUE };
            IloNumVar[] x = cplex.numVarArray(3, lb, ub);
            double[] objvals = { 1.0, 2.0, 3.0 };
            cplex.addMaximize(cplex.scalProd(x, objvals));
            cplex.addLe(cplex.sum(cplex.prod(-1.0, x[0]),
                    cplex.prod(1.0, x[1]), cplex.prod(1.0, x[2])), 20.0);
            cplex.addLe(cplex.sum(cplex.prod(1.0, x[0]),
                    cplex.prod(-3.0, x[1]), cplex.prod(1.0, x[2])), 30.0);
            if (cplex.solve()) {
                cplex.output().println("Solution status = " + cplex.getStatus());
                cplex.output().println("Solution value = " + cplex.getObjValue());
                double[] val = cplex.getValues(x);
                int ncols = cplex.getNcols();
                for (int j = 0; j < ncols; ++j)
                    cplex.output().println("Column: " + j + " Value = " + val[j]);
            }
            cplex.end();
        } catch (IloException e) {
            System.err.println("Concert exception '" + e + "' caught");
        }
    }

}
