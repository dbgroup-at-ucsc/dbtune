package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.concert.IloObjective;
import ilog.cplex.IloCplex;
import ilog.cplex.IloCplex.UnknownObjectException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

import edu.ucsc.dbtune.seq.bip.SebBIPOutput;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.*;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;

/**
 * @author Rui Wang
 */
public class DAT extends AbstractBIPSolver {
    class Slot {
        InumPlan plan;
        SeqInumSlot s;
        SeqInumIndex[] indexes;
        IloNumVar[] useIndex;

        public Slot(InumPlan plan, SeqInumSlot s) throws IloException {
            this.plan = plan;
            this.s = s;
            useIndex = new IloNumVar[s.costs.size() + 1];
            indexes = new SeqInumIndex[s.costs.size()];
            for (int i = 0; i < useIndex.length; i++) {
                useIndex[i] = createBinaryVar();
                String name = "INDEX_" + s.plan.query.id + "_" + s.plan.id;
                if (i < s.costs.size()) {
                    indexes[i] = s.costs.get(i).index;
                    name += "_" + indexes[i].id;
                } else
                    name += "_FTS";
                useIndex[i].setName(name);
            }
            addVars(useIndex);
        }

        public void addObjective(IloLinearNumExpr expr, double coefficient)
                throws IloException {
            for (int i = 0; i < s.costs.size(); i++) {
                expr.addTerm(s.costs.get(i).cost * coefficient, useIndex[i]);
            }
            expr.addTerm(s.fullTableScanCost * coefficient, useIndex[s.costs
                    .size()]);
        }

        public double getCost() throws IloException {
            double cost = 0;
            for (int i = 0; i < s.costs.size(); i++) {
                cost += s.costs.get(i).cost * getValue(useIndex[i]);
            }
            cost += s.fullTableScanCost * getValue(useIndex[s.costs.size()]);
            return cost;
        }

        public void addConstriant() throws IloException {
            IloLinearNumExpr expr = cplex.linearNumExpr();
            for (IloNumVar var : useIndex)
                expr.addTerm(1, var);
            cplex.addEq(expr, plan.active);
            if (showFormulas)
                Rt.p(expr.toString() + "==" + plan.active);
            for (int i = 0; i < indexes.length; i++) {
                expr = cplex.linearNumExpr();
                expr.addTerm(1, useIndex[i]);
                expr.addTerm(-1, plan.query.window.index2present
                        .get(indexes[i]));
                cplex.addLe(expr, 0);
                if (showFormulas)
                    Rt.p(expr.toString() + "<=0");
            }
        }
    }

    class InumPlan {
        Query query;
        SeqInumPlan p;
        IloNumVar active;
        Slot[] slots;

        public InumPlan(Query query, SeqInumPlan p) throws IloException {
            this.query = query;
            this.p = p;
            this.active = createBinaryVar();
            active.setName("PLAN_" + p.query.id + "_" + p.id);
            addVar(active);
            slots = new Slot[p.slots.length];
            for (int i = 0; i < slots.length; i++) {
                slots[i] = new Slot(this, p.slots[i]);
            }
        }

        public void addObjective(IloLinearNumExpr expr) throws IloException {
            double coefficient = alpha;
            if (query.window.lastWindow)
                coefficient += beta;
            expr.addTerm(coefficient * p.internalCost, active);
            for (Slot slot : slots)
                slot.addObjective(expr, coefficient);
        }

        public double getCost() throws IloException {
            double cost = 0;
            cost += p.internalCost * getValue(active);
            for (Slot slot : slots) {
                cost += slot.getCost();
            }
            return cost;
        }

        public void addConstriant() throws IloException {
            for (Slot slot : slots)
                slot.addConstriant();
        }
    }

    class Query {
        Window window;
        SeqInumQuery q;
        InumPlan[] plans;

        public Query(Window window, SeqInumQuery q) throws IloException {
            this.window = window;
            this.q = q;
            plans = new InumPlan[q.plans.length];
            for (int i = 0; i < plans.length; i++) {
                plans[i] = new InumPlan(this, q.plans[i]);
            }
        }

        public void addObjective(IloLinearNumExpr expr) throws IloException {
            for (InumPlan plan : plans)
                plan.addObjective(expr);
        }

        public double getCost() throws IloException {
            double cost = 0;
            for (InumPlan plan : plans)
                cost += plan.getCost();
            return cost;
        }

        public void addConstriant() throws IloException {
            for (InumPlan plan : plans)
                plan.addConstriant();
            // One and only one plan can be used
            IloLinearNumExpr expr = cplex.linearNumExpr();
            for (InumPlan plan : plans)
                expr.addTerm(1, plan.active);
            cplex.addEq(expr, 1);
            if (showFormulas)
                Rt.p(expr.toString() + "==1");
        }
    }

    class Window {
        int id;
        double costConstraint;
        boolean lastWindow;
        Query[] queries;
        IloNumVar[] create, drop, present;
        Hashtable<SeqInumIndex, Integer> index2id = new Hashtable<SeqInumIndex, Integer>();
        Hashtable<SeqInumIndex, IloNumVar> index2present = new Hashtable<SeqInumIndex, IloNumVar>();
        boolean[] indexPresent;

        public Window(int id, boolean lastWindow, double costConstraint)
                throws IloException {
            this.id = id;
            this.lastWindow = lastWindow;
            this.costConstraint = costConstraint;
            this.queries = new Query[totalQueires];
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i] = new Query(this, costModel.queries.get(i));
            }
            this.create = createBinaryVars(totalIndices);
            this.drop = createBinaryVars(totalIndices);
            this.present = createBinaryVars(totalIndices);
            for (int i = 0; i < totalIndices; i++) {
                this.create[i].setName("CREATE_" + id + "_"
                        + costModel.indices.get(i).id);
                this.drop[i].setName("DROP_" + id + "_"
                        + costModel.indices.get(i).id);
                this.present[i].setName("PRESENT_" + id + "_"
                        + costModel.indices.get(i).id);
                index2id.put(costModel.indices.get(i), i);
                index2present.put(costModel.indices.get(i), present[i]);
            }
            addVars(create);
            addVars(drop);
            addVars(present);
        }

        public void addObjective(IloLinearNumExpr expr) throws IloException {
            for (Query query : queries)
                query.addObjective(expr);
            for (int i = 0; i < totalIndices; i++) {
                // expr.addTerm(costModel.indices.get(i).createCost, create[i]);
                // expr.addTerm(costModel.indices.get(i).dropCost, drop[i]);
                // The purpose of following objective is to
                // remove a index when it's not necessary
                // expr.addTerm(0.1, present[i]);
            }
        }

        public double getCost() throws IloException {
            double cost = 0;
            for (Query query : queries)
                cost += query.getCost();
            return cost;
        }

        public void addConstriant() throws IloException {
            for (Query query : queries)
                query.addConstriant();
            // add storage constraint
            IloLinearNumExpr expr;

            // expr = cplex.linearNumExpr();
            // for (int i = 0; i < totalIndices; i++) {
            // expr.addTerm(costModel.indices.get(i).storageCost,
            // this.present[i]);
            // }
            // cplex.addLe(expr, costModel.storageConstraint);
            // if (showFormulas)
            // Rt.p(expr.toString() + "<=" + costModel.storageConstraint);

            expr = cplex.linearNumExpr();
            for (int i = 0; i < totalIndices; i++) {
                expr.addTerm(costModel.indices.get(i).createCost,
                        this.create[i]);
            }
            cplex.addLe(expr, this.costConstraint);
            if (showFormulas)
                Rt.p(expr.toString() + "<=" + this.costConstraint);

            // index can't be created and droped at the same step
            for (int i = 0; i < totalIndices; i++) {
                expr = cplex.linearNumExpr();
                expr.addTerm(1, this.create[i]);
                expr.addTerm(1, this.drop[i]);
                cplex.addLe(expr, 1);
                if (showFormulas)
                    Rt.p(expr.toString() + "<=1");
            }
        }

        public void getValues() throws UnknownObjectException, IloException {
            indexPresent = new boolean[totalIndices];
            for (int j = 0; j < totalIndices; j++) {
                indexPresent[j] = getValue(present[j]) == 1;
            }
        }
    }

    public static boolean showFormulas = false;
    SeqInumCost costModel;
    IloNumVar[] iloVar = new IloNumVar[0];
    double[] windowConstraints;
    Window[] windows;
    double alpha;
    double beta;
    int totalQueires;
    int totalIndices;
    Logger log = Logger.getLogger(DAT.class.getName());

    public DAT(SeqInumCost cost, double[] windowConstraints, double alpha,
            double beta) throws IloException {
        this.costModel = cost;
        this.windowConstraints = windowConstraints;
        this.alpha = alpha;
        this.beta = beta;
        this.totalQueires = cost.queries.size();
        this.totalIndices = cost.indices.size();
    }

    IloLinearNumExpr objExpr;

    @Override
    protected final void buildBIP() {
        super.numConstraints = 0;
        try {
            cplex = new IloCplex();
            cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
            cplex.setOut(null);
            cplex.setWarning(null);
            windows = new Window[windowConstraints.length];
            for (int i = 0; i < windowConstraints.length; i++) {
                windows[i] = new Window(i, i == windowConstraints.length - 1,
                        windowConstraints[i]);
            }
            cplex.add(iloVar);

            IloLinearNumExpr expr = cplex.linearNumExpr();
            for (int i = 0; i < windows.length; i++) {
                this.windows[i].addObjective(expr);
            }
            objExpr = expr;
            IloObjective obj = cplex.minimize(expr);
            cplex.add(obj);
            if (showFormulas)
                Rt.p("Obj: " + expr.toString());

            for (int i = 0; i < windows.length; i++) {
                this.windows[i].addConstriant();
                for (int k = 0; k < totalIndices; k++) {
                    expr = cplex.linearNumExpr();
                    if (i > 0)
                        expr.addTerm(1, this.windows[i - 1].present[k]);
                    // for (int j = 0; j <= i; j++) {
                    expr.addTerm(1, this.windows[i].create[k]);
                    expr.addTerm(-1, this.windows[i].drop[k]);
                    // }
                    if (showFormulas)
                        Rt
                                .p(expr.toString() + "="
                                        + this.windows[i].present[k]);
                    cplex.addEq(expr, this.windows[i].present[k]);
                }
            }

            cplexVar = new Vector<IloNumVar>();
            for (IloNumVar var : iloVar)
                cplexVar.add(var);
        } catch (IloException e) {
            e.printStackTrace();
        }
    }

    public IndexTuningOutput baseline() {
        DATOutput output = new DATOutput(windowConstraints.length);

        super.numConstraints = 0;
        double totalCost = 0;
        try {
            windows = new Window[windowConstraints.length];
            double[] costs = new double[windowConstraints.length];
            boolean[] indexPresent = new boolean[totalIndices];
            Arrays.fill(indexPresent, false);
            for (int i = 0; i < windowConstraints.length; i++) {
                cplex = new IloCplex();
                cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
                cplex.setOut(null);
                cplex.setWarning(null);
                windows[i] = new Window(i, i == windowConstraints.length - 1,
                        windowConstraints[i]);
                IloLinearNumExpr expr = cplex.linearNumExpr();
                this.windows[i].addObjective(expr);
                IloObjective obj = cplex.minimize(expr);
                cplex.add(obj);
                if (showFormulas)
                    Rt.p("Obj: " + expr.toString());
                this.windows[i].addConstriant();
                for (int k = 0; k < totalIndices; k++) {
                    if (indexPresent[k]) {
                        if (showFormulas)
                            Rt.p(this.windows[i].present[k] + "=1");
                        cplex.addEq(this.windows[i].present[k], 1);
                        cplex.addEq(this.windows[i].create[k], 0);
                        cplex.addEq(this.windows[i].drop[k], 0);
                    } else {
                        expr = cplex.linearNumExpr();
                        expr.addTerm(1, this.windows[i].create[k]);
                        expr.addTerm(-1, this.windows[i].drop[k]);
                        // }
                        if (showFormulas)
                            Rt.p(expr.toString() + "="
                                    + this.windows[i].present[k]);
                        cplex.addEq(expr, this.windows[i].present[k]);
                    }
                }
                if (!cplex.solve())
                    throw new Error();
                costs[i] = cplex.getObjValue();
                totalCost += costs[i];
                windows[i].getValues();
                for (int j = 0; j < totalIndices; j++) {
                    indexPresent[j] = windows[i].indexPresent[j];
                }
                output.ws[i].indexUsed = Arrays.copyOf(windows[i].indexPresent,
                        totalIndices);
                output.ws[i].cost = windows[i].getCost();
                if (!windows[i].lastWindow
                        && Math.abs(output.ws[i].cost * alpha
                                - cplex.getObjValue()) > 1)
                    throw new Error();
            }

            output.totalCost = totalCost;

            cplexVar = new Vector<IloNumVar>();
            for (IloNumVar var : iloVar)
                cplexVar.add(var);
        } catch (IloException e) {
            e.printStackTrace();
        }
        return output;
    }

    private boolean[] cophy(double total) throws IloException {
        IloCplex old = cplex;
        cplex = new IloCplex();
        cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
        cplex.setOut(null);
        cplex.setWarning(null);
        Window window = new Window(0, true, total);
        IloLinearNumExpr expr = cplex.linearNumExpr();
        window.addObjective(expr);
        IloObjective obj = cplex.minimize(expr);
        cplex.add(obj);
        if (showFormulas)
            Rt.p("Obj: " + expr.toString());
        window.addConstriant();
        double max = 0;
        for (double d : windowConstraints) {
            if (d > max)
                max = d;
        }
        for (int k = 0; k < totalIndices; k++) {
            if (costModel.indices.get(k).createCost >= max)
                cplex.addEq(window.create[k], 0);
            expr = cplex.linearNumExpr();
            expr.addTerm(1, window.create[k]);
            expr.addTerm(-1, window.drop[k]);
            // }
            if (showFormulas)
                Rt.p(expr.toString() + "=" + window.present[k]);
            cplex.addEq(expr, window.present[k]);
        }
        if (!cplex.solve())
            throw new Error();
        window.getValues();
        boolean[] indexPresent = new boolean[totalIndices];
        for (int j = 0; j < totalIndices; j++) {
            indexPresent[j] = window.indexPresent[j];
        }
        cplex = old;
        return indexPresent;
    }

    private double costWithIndex(int i) throws IloException {
        boolean[] present = new boolean[totalIndices];
        Arrays.fill(present, false);
        present[i] = true;
        return costWithIndex(present);
    }

    private double costWithIndex(boolean[] present) throws IloException {
        IloCplex old = cplex;
        cplex = new IloCplex();
        cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
        cplex.setOut(null);
        cplex.setWarning(null);
        Window window = new Window(0, true, Double.MAX_VALUE);
        IloLinearNumExpr expr = cplex.linearNumExpr();
        window.addObjective(expr);
        IloObjective obj = cplex.minimize(expr);
        cplex.add(obj);
        if (showFormulas)
            Rt.p("Obj: " + expr.toString());
        window.addConstriant();
        for (int k = 0; k < totalIndices; k++) {
            cplex.addEq(window.present[k], present[k] ? 1 : 0);
            cplex.addEq(window.create[k], 0);
            cplex.addEq(window.drop[k], 0);
        }
        if (!cplex.solve())
            throw new Error();
        window.getValues();
        double cost = window.getCost();
        cplex = old;
        return cost;
    }

    /**
     * @param method
     *            optimal, greedy
     * @return
     */
    public IndexTuningOutput baseline2(String method) {
        DATOutput output = new DATOutput(windowConstraints.length);
        super.numConstraints = 0;
        double totalCost = 0;
        try {
            double total = 0;
            for (double d : windowConstraints)
                total += d;
            boolean[][] indexPresents = new boolean[windowConstraints.length][totalIndices];
            double costWithoutIndex = costWithIndex(new boolean[totalIndices]);
            while (true) {
                boolean[] indexPresent = cophy(total);
                // Rt.p(this.costWithIndex(indexPresent));
                Vector<SeqInumIndex> usedIndex = new Vector<SeqInumIndex>();
                for (int i = 0; i < indexPresent.length; i++) {
                    if (indexPresent[i]) {
                        if (costModel.indices.get(i).id != i)
                            throw new Error();
                        costModel.indices.get(i).indexBenefit = costWithoutIndex
                                - costWithIndex(i);
                        usedIndex.add(costModel.indices.get(i));
                    }
                }
                double[] bins = windowConstraints;
                double[] items = new double[usedIndex.size()];
                double[] profits = new double[items.length];
                double totalWeight = 0;
                for (int j = 0; j < items.length; j++) {
                    items[j] = usedIndex.get(j).createCost;
                    totalWeight += items[j];
                    profits[j] = usedIndex.get(j).indexBenefit;
                    Rt.np(usedIndex.get(j).id + "\t" + items[j] + "\t"
                            + profits[j]);
                }
                Rt.np(totalWeight + "/" + total);
                double[] binWeights = new double[windowConstraints.length];
                for (int j = 0; j < binWeights.length; j++)
                    binWeights[j] = binWeights.length - j;
                // Rt.p(method + " " + bins.length + " " + items.length);
                int[] belongs = null;
                if ("optimal".equals(method)) {
                    MKPOptimum m = new MKPOptimum(bins, binWeights, items,
                            profits);
                    if (m.cannotFitIn > 0) {
                        Rt.p("can't fit: " + m.cannotFitIn+" "+total);
                        total *= 0.9;
                        continue;
                    }
                    belongs = m.maxBelongs;
                } else if (method.startsWith("greedy")) {
                    MKPGreedy m = new MKPGreedy(bins, binWeights, items,
                            profits, method.equals("greedyRatio"));
                    if (m.cannotFitIn > 0) {
                        Rt.p("can't fit: " + m.cannotFitIn + " "
                                + m.cannotFitWeight+" "+total);
                        total *= 0.9;
                        continue;
                    }
                    belongs = m.belongs;
                } else {
                    throw new Error(method);
                }
                for (int i = 0; i < belongs.length; i++) {
                    if (belongs[i] >= 0) {
                        indexPresents[belongs[i]][usedIndex.get(i).id] = true;
                    }
                }
                break;
            }
            for (int wid = 0; wid < indexPresents.length; wid++) {
                System.out.print("window " + wid + "\t");
                for (int j = 0; j < totalIndices; j++) {
                    if (indexPresents[wid][j]) {
                        System.out.format("%d(%,.0f|%,.0f)", j,
                                costModel.indices.get(j).createCost,
                                costModel.indices.get(j).indexBenefit);
                    }
                }
                System.out.println();
            }
            for (int wid = 1; wid < indexPresents.length; wid++) {
                for (int j = 0; j < totalIndices; j++) {
                    if (indexPresents[wid - 1][j]) {
                        indexPresents[wid][j] = true;
                    }
                }
            }
            // while (true) {
            // boolean[] indexPresent = cophy(total);
            // for (int wid = 0; wid < indexPresents.length; wid++) {
            // if (wid > 0)
            // System.arraycopy(indexPresents[wid - 1], 0,
            // indexPresents[wid], 0,
            // indexPresents[wid].length);
            // double quota = windowConstraints[wid];
            // for (int j = 0; j < totalIndices; j++) {
            // if (indexPresent[j]) {
            // double c2 = costModel.indices.get(j).createCost;
            // if (c2 < quota) {
            // quota -= c2;
            // indexPresent[j] = false;
            // indexPresents[wid][j] = true;
            // }
            // }
            // }
            // }
            // int notFit = 0;
            // for (int j = 0; j < totalIndices; j++) {
            // if (indexPresent[j]) {
            // notFit++;
            // }
            // }
            // Rt.p("can't fit: " + notFit);
            // if (notFit == 0)
            // break;
            // total *= 0.9;
            // }
            // for (int wid = 0; wid < indexPresents.length; wid++) {
            // for (int j = 0; j < totalIndices; j++) {
            // System.out.print("\t" + indexPresents[wid][j]);
            // }
            // Rt.np();
            // }
            windows = new Window[windowConstraints.length];
            double[] costs = new double[windowConstraints.length];
            for (int i = 0; i < windowConstraints.length; i++) {
                cplex = new IloCplex();
                cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
                cplex.setOut(null);
                cplex.setWarning(null);
                windows[i] = new Window(i, i == windowConstraints.length - 1,
                        windowConstraints[i]);
                IloLinearNumExpr expr = cplex.linearNumExpr();
                this.windows[i].addObjective(expr);
                IloObjective obj = cplex.minimize(expr);
                cplex.add(obj);
                if (showFormulas)
                    Rt.p("Obj: " + expr.toString());
                this.windows[i].addConstriant();
                for (int k = 0; k < totalIndices; k++) {
                    cplex.addEq(this.windows[i].present[k],
                            indexPresents[i][k] ? 1 : 0);
                    cplex.addEq(this.windows[i].create[k], 0);
                    cplex.addEq(this.windows[i].drop[k], 0);
                }
                if (!cplex.solve())
                    throw new Error();
                costs[i] = cplex.getObjValue();
                totalCost += costs[i];
                windows[i].getValues();
                // for (int j = 0; j < totalIndices; j++) {
                // indexPresent[j] = windows[i].indexPresent[j];
                // }
                output.ws[i].indexUsed = Arrays.copyOf(windows[i].indexPresent,
                        totalIndices);
                output.ws[i].cost = windows[i].getCost();
                if (!windows[i].lastWindow
                        && Math.abs(output.ws[i].cost * alpha
                                - cplex.getObjValue()) > 1)
                    throw new Error();
            }

            output.totalCost = totalCost;

            cplexVar = new Vector<IloNumVar>();
            for (IloNumVar var : iloVar)
                cplexVar.add(var);
        } catch (IloException e) {
            e.printStackTrace();
        }
        return output;
    }

    @Override
    protected IndexTuningOutput getOutput() {
        DATOutput output = new DATOutput(windows.length);
        try {
            for (Window w : windows)
                w.getValues();
            double totalCost = 0;
            for (int i = 0; i < output.ws.length; i++) {
                output.ws[i].indexUsed = Arrays.copyOf(windows[i].indexPresent,
                        totalIndices);
                output.ws[i].cost = windows[i].getCost();
                totalCost += alpha * output.ws[i].cost;
                if (i == output.ws.length - 1)
                    totalCost += beta * output.ws[i].cost;
                // int total = 0;
                // for (boolean b : output.ws[i].indexUsed)
                // total++;
                // Rt.p(total);
                // Rt.p(this.costWithIndex(output.ws[i].indexUsed));
            }
            output.totalCost = totalCost;
            // Rt.p(cplex.getValue(cplex.getObjective().getExpr()));
            // Rt.p(cplex.getValue(objExpr));
            // if (Math.abs(totalCost - cplex.getObjValue()) > 1)
            // throw new Error(totalCost+" "+cplex.getObjValue());
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return output;
    }

    private void addVar(IloNumVar var) {
        int size = iloVar.length;
        iloVar = Arrays.copyOf(iloVar, size + 1);
        iloVar[size] = var;
    }

    private void addVars(IloNumVar[] vars) {
        int size = iloVar.length;
        iloVar = Arrays.copyOf(iloVar, size + vars.length);
        System.arraycopy(vars, 0, iloVar, size, vars.length);
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
}
