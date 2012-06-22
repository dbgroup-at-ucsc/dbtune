package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloObjective;
import ilog.cplex.IloCplex;

import java.util.Arrays;
import java.util.Comparator;

import edu.ucsc.dbtune.deployAware.DAT.Window;
import edu.ucsc.dbtune.seq.utils.Rt;

public class MKPBip {
    double[] bins;
    double[] binWeights;
    double[] items;
    double[] profits;
    double profit;
    int[] belongs;
    int cannotFitIn = 0;
    double cannotFitWeight = 0;
    IloNumVar[][] vars;

    public MKPBip(double[] bins, double[] binWeights, double[] items,
            double[] profits, int maxItemsPerBin) throws IloException {
        this.bins = Arrays.copyOf(bins, bins.length);
        this.binWeights = binWeights;
        this.items = items;
        this.profits = profits;
        this.belongs = new int[items.length];
        IloCplex cplex = new IloCplex();
        cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
        cplex.setOut(null);
        cplex.setWarning(null);
        vars = new IloNumVar[bins.length][items.length];
        for (IloNumVar[] vs : vars) {
            for (int i = 0; i < vs.length; i++)
                vs[i] = cplex.intVar(0, 1);
        }
        for (int i = 0; i < items.length; i++) {
            IloLinearNumExpr expr = cplex.linearNumExpr();
            for (int j = 0; j < bins.length; j++) {
                expr.addTerm(1, vars[j][i]);
            }
            cplex.addLe(expr, 1);
        }
        for (int i = 0; i < bins.length; i++) {
            IloLinearNumExpr expr = cplex.linearNumExpr();
            for (int j = 0; j < items.length; j++) {
                expr.addTerm(this.items[j], vars[i][j]);
            }
            cplex.addLe(expr, this.bins[i]);
            if (maxItemsPerBin > 0) {
                expr = cplex.linearNumExpr();
                for (int j = 0; j < items.length; j++) {
                    expr.addTerm(1, vars[i][j]);
                }
                cplex.addLe(expr, maxItemsPerBin);
            }
        }
        IloLinearNumExpr expr = cplex.linearNumExpr();
        for (int i = 0; i < bins.length; i++) {
            for (int j = 0; j < items.length; j++) {
                expr.addTerm(this.profits[j] * this.binWeights[i], vars[i][j]);
            }
        }
        IloObjective obj = cplex.maximize(expr);
        cplex.add(obj);
        if (!cplex.solve())
            throw new Error();
        Arrays.fill(belongs, -1);
        for (int i = 0; i < bins.length; i++) {
            for (int j = 0; j < items.length; j++) {
                double d = cplex.getValue(vars[i][j]);
                if (Math.abs(d - 1) < 0.1) {
                    if (belongs[j] >= 0)
                        throw new Error();
                    belongs[j] = i;
                }
            }
        }
        profit = 0;
        for (int i = 0; i < items.length; i++) {
            if (belongs[i] >= 0)
                profit += profits[i] * binWeights[belongs[i]];
            else {
                cannotFitIn++;
                cannotFitWeight += items[i];
            }
        }
    }

    public static void main(String[] args) throws IloException {
        double[] bins = { 100, 100, 100 };
        double[] items = { 10, 20, 30, 70, 90 };
        double[] profits = { 10, 20, 30, 70, 900 };
        double[] binWeights = { 3, 2, 1 };
        MKPBip greedy = new MKPBip(bins, binWeights, items, profits,0);
        Rt.p(greedy.profit);
        for (int i : greedy.belongs) {
            System.out.print(i + " ");
        }
        System.out.println();
    }

}
