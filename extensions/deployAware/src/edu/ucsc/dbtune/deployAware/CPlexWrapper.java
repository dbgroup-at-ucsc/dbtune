package edu.ucsc.dbtune.deployAware;

import edu.ucsc.dbtune.util.Rt;
import ilog.cplex.*;
import ilog.concert.*;

public class CPlexWrapper {
    IloCplex cplex;

    public CPlexWrapper() throws IloException {
        this(0.05);
    }

    public CPlexWrapper(double gap) throws IloException {
        cplex = new IloCplex();
        cplex.setParam(IloCplex.DoubleParam.EpGap, gap);
        Rt.p("gap = " + gap);
        // -- Trung's hardcoding (maximum 120 seconds to run a BIP)
        cplex.setParam(IloCplex.DoubleParam.TiLim, 150);
        //cplex.setOut(null);
        cplex.setWarning(null);
    }

    public void setEpGap(double gap) throws IloException {
        cplex.setParam(IloCplex.DoubleParam.EpGap, gap);
    }

    public void close() throws IloException {
        cplex.clearModel();
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

    int varCount = 0;
    int constraintCount = 0;

    public IloNumVar createBinaryVar() throws IloException {
        varCount++;
        return cplex.intVar(0, 1);
    }

    public IloNumVar[] createBinaryVars(int size) throws IloException {
        varCount += size;
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

    public IloConstraint addLe(IloNumExpr arg0, double arg1) throws IloException {
        constraintCount++;
        return cplex.addLe(arg0, arg1);
    }

    public IloConstraint addLe(IloNumExpr arg0, IloNumExpr arg1) throws IloException {
        constraintCount++;
        return cplex.addLe(arg0, arg1);
    }

    public IloConstraint addEq(IloNumExpr arg0, double arg1) throws IloException {
        constraintCount++;
        return cplex.addEq(arg0, arg1);
    }

    public IloConstraint addEq(IloNumExpr arg0, IloNumExpr arg1) throws IloException {
        constraintCount++;
        return cplex.addEq(arg0, arg1);
    }

    public IloObjective minimize(IloNumExpr arg0) throws IloException {
        return cplex.minimize(arg0);
    }

    public void add(IloObjective arg0) throws IloException {
        cplex.add(arg0);
    }

    public double getObjValue() throws IloException {
        return cplex.getObjValue();
    }

    public boolean solve() throws IloException {
        return cplex.solve();
    }
}
