package edu.ucsc.dbtune.deployAware;

import ilog.cplex.*;
import ilog.concert.*;

public class CPlexWrapper {
    IloCplex cplex;

    public CPlexWrapper() throws IloException {
        cplex = new IloCplex();
        cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
        cplex.setOut(null);
        cplex.setWarning(null);
    }

    public static void invokeCplex() throws IloException {
        // force to show the warning, so it won't mess up with later outputs
        IloCplex cplex = new IloCplex();
        cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
        cplex.setOut(null);
        cplex.setWarning(null);
        IloNumVar var = cplex.intVar(0, 1);
        IloLinearNumExpr expr = cplex.linearNumExpr();
        expr.addTerm(1, var);
        IloObjective obj = cplex.minimize(expr);
        cplex.add(obj);
        cplex.solve();
    }

    void addVar(IloNumVar var) {
        // int size = iloVar.length;
        // iloVar = Arrays.copyOf(iloVar, size + 1);
        // iloVar[size] = var;
    }

    //
    void addVars(IloNumVar[] vars) {
        // int size = iloVar.length;
        // iloVar = Arrays.copyOf(iloVar, size + vars.length);
        // System.arraycopy(vars, 0, iloVar, size, vars.length);
    }

    public int getValue(IloNumVar var) throws IloException {
        double value = cplex.getValue(var);
        if (Math.abs(value - 1) < 1E-5) {
            return 1;
        } else if (Math.abs(value) < 1E-5) {
            return 0;
        } else {
            throw new Error();
        }
    }

    public IloNumVar createBinaryVar() throws IloException {
        return cplex.intVar(0, 1);
    }

    public IloNumVar[] createBinaryVars(int size) throws IloException {
        IloNumVarType[] type = new IloNumVarType[size];
        double[] lb = new double[size];
        double[] ub = new double[size];
        for (int i = 0; i < size; i++) {
            type[i] = IloNumVarType.Int;
            lb[i] = 0.0;
            ub[i] = 1.0;
        }
        return cplex.numVarArray(size, lb, ub, type);
    }

    public IloLinearNumExpr linearNumExpr() throws IloException {
        return cplex.linearNumExpr();
    }

    public IloConstraint addLe(IloNumExpr arg0, double arg1)
            throws IloException {
        return cplex.addLe(arg0, arg1);
    }

    public IloConstraint addLe(IloNumExpr arg0, IloNumExpr arg1)
            throws IloException {
        return cplex.addLe(arg0, arg1);
    }

    public IloConstraint addEq(IloNumExpr arg0, double arg1)
            throws IloException {
        return cplex.addEq(arg0, arg1);
    }

    public IloConstraint addEq(IloNumExpr arg0, IloNumExpr arg1)
            throws IloException {
        return cplex.addEq(arg0, arg1);
    }

    public IloObjective minimize(IloNumExpr arg0) throws IloException {
        return cplex.minimize(arg0);
    }

    public void add(IloObjective arg0) throws IloException {
        cplex.add(arg0);
    }

    public boolean solve() throws IloException {
        return cplex.solve();
    }
}
