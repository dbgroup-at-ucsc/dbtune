package edu.ucsc.dbtune.seq.bip;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.concert.IloObjective;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Vector;
import java.util.logging.Logger;

import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.bip.def.SeqInumPlan;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlot;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlotIndexCost;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;

public class Wfit extends AbstractBIPSolver {
    class Slot {
        InumPlan plan;
        SeqInumSlot s;
        IloNumVar[] useIndex;
        SeqInumIndex[] indexes;

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

        public void addObjective(IloLinearNumExpr expr) throws IloException {
            for (int i = 0; i < s.costs.size(); i++) {
                expr.addTerm(s.costs.get(i).cost, useIndex[i]);
            }
            expr.addTerm(s.fullTableScanCost, useIndex[s.costs.size()]);
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
                expr.addTerm(-1, plan.query.index2present.get(indexes[i]));
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
            expr.addTerm(p.internalCost, active);
            for (Slot slot : slots)
                slot.addObjective(expr);
        }

        public void addConstriant() throws IloException {
            for (Slot slot : slots)
                slot.addConstriant();
        }
    }

    class Query {
        SeqInumQuery q;
        IloNumVar[] create, drop, present;
        InumPlan[] plans;
        Hashtable<SeqInumIndex, IloNumVar> index2present = new Hashtable<SeqInumIndex, IloNumVar>();

        public Query(SeqInumQuery q) throws IloException {
            this.q = q;
            this.create = createBinaryVars(totalIndices);
            this.drop = createBinaryVars(totalIndices);
            this.present = createBinaryVars(totalIndices);
            for (int i = 0; i < totalIndices; i++) {
                this.create[i].setName("CREATE_" + q.id + "_"
                        + cost.indices.get(i).id);
                this.drop[i].setName("DROP_" + q.id + "_"
                        + cost.indices.get(i).id);
                this.present[i].setName("PRESENT_" + q.id + "_"
                        + cost.indices.get(i).id);
                index2present.put(cost.indices.get(i), present[i]);
            }
            addVars(create);
            addVars(drop);
            addVars(present);
            plans = new InumPlan[q.plans.length];
            for (int i = 0; i < plans.length; i++) {
                plans[i] = new InumPlan(this, q.plans[i]);
            }
        }

        public void addObjective(IloLinearNumExpr expr) throws IloException {
            for (InumPlan plan : plans)
                plan.addObjective(expr);
            for (int i = 0; i < totalIndices; i++) {
                expr.addTerm(cost.indices.get(i).createCost, create[i]);
                expr.addTerm(cost.indices.get(i).dropCost, drop[i]);
                // The purpose of following objective is to
                // remove a index when it's not necessary
                expr.addTerm(0.1, present[i]);
            }
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

            // add storage constraint
            expr = cplex.linearNumExpr();
            for (int i = 0; i < totalIndices; i++) {
                expr.addTerm(cost.indices.get(i).storageCost, this.present[i]);
            }
            cplex.addLe(expr, cost.storageConstraint);
            if (showFormulas)
                Rt.p(expr.toString() + "<=" + cost.storageConstraint);

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
    }

    public static boolean showFormulas = false;
    SeqInumCost cost;
    IloNumVar[] iloVar = new IloNumVar[0];
    int totalQueires;
    int totalIndices;
    Vector<Query> queries = new Vector<Query>();
    Logger log = Logger.getLogger(SeqBIP.class.getName());

    public Wfit(SeqInumCost cost) throws IloException {
        this.cost = cost;
        this.totalQueires = cost.queries.size();
        this.totalIndices = cost.indices.size();
    }

    @Override
    protected final void buildBIP() {
        try {
            super.numConstraints = 0;
            exprObj = cplex.linearNumExpr();
            cplexVar = new Vector<IloNumVar>();
            for (IloNumVar var : iloVar)
                cplexVar.add(var);
        } catch (IloException e) {
            e.printStackTrace();
        }
    }

    IloLinearNumExpr exprObj;
    IloObjective lastObj = null;

    public void addQuery(SQLStatement statement) throws SQLException {
        try {
            RTimerN timer = new RTimerN();
            Query q = new Query(cost.addQuery(statement));
            double inumTime = timer.getSecondElapse();
            timer.reset();
            this.queries.add(q);
            totalQueires = this.queries.size();
            cplex.add(iloVar);
            if (lastObj != null)
                cplex.remove(lastObj);

            q.addObjective(exprObj);
            IloObjective obj = cplex.minimize(exprObj);
            lastObj = obj;
            cplex.add(obj);
            if (showFormulas)
                Rt.p("Obj: " + exprObj.toString());

            q.addConstriant();
            for (int k = 0; k < totalIndices; k++) {
                IloLinearNumExpr expr = cplex.linearNumExpr();
                if (this.queries.size() > 1)
                    expr
                            .addTerm(1, this.queries
                                    .get(this.queries.size() - 2).present[k]);
                // for (int j = 0; j <= i; j++) {
                expr.addTerm(1, q.create[k]);
                expr.addTerm(-1, q.drop[k]);
                // }
                if (showFormulas)
                    Rt.p(expr.toString() + "=" + q.present[k]);
                cplex.addEq(expr, q.present[k]);
            }
            cplex.solve();
            double bipTime = timer.getSecondElapse();
            Rt
                    .np(
                            "queries=%d inumTime=%.3f bipTime=%.3f totalTime=%.3f cost=%.2f",
                            this.queries.size(), inumTime, bipTime, inumTime
                                    + bipTime, cplex.getObjValue());

            cplexVar = new Vector<IloNumVar>();
            for (IloNumVar var : iloVar)
                cplexVar.add(var);
        } catch (IloException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected IndexTuningOutput getOutput() {
        SebBIPOutput output = new SebBIPOutput();
        try {
            output.indexUsed = new Vector[cost.queries.size()];
            for (int i = 0; i < output.indexUsed.length; i++)
                output.indexUsed[i] = new Vector<SeqInumIndex>();
            double[] xval = cplex.getValues(iloVar);
            for (int i = 0; i < xval.length; i++) {
                String name = iloVar[i].getName();
                if (name.startsWith("PRESENT")) {
                    String[] ss = name.split("_");
                    int queryId = Integer.parseInt(ss[1]);
                    int indexId = Integer.parseInt(ss[2]);
                    if (Math.abs(valVar[i] - 1) < 1E-5)
                        output.indexUsed[queryId]
                                .add(cost.indices.get(indexId));
                } else if (name.startsWith("PLAN")) {
                    String[] ss = name.split("_");
                    int queryId = Integer.parseInt(ss[1]);
                    int planId = Integer.parseInt(ss[2]);
                    if (Math.abs(valVar[i] - 1) < 1E-5)
                        cost.queries.get(queryId).selectedPlan = cost.queries
                                .get(queryId).plans[planId];
                } else if (name.startsWith("INDEX")) {
                    String[] ss = name.split("_");
                    int queryId = Integer.parseInt(ss[1]);
                    int planId = Integer.parseInt(ss[2]);
                    if (!"FTS".equals(ss[3])) {
                        int indexId = Integer.parseInt(ss[3]);
                        if (Math.abs(valVar[i] - 1) < 1E-5) {
                            for (SeqInumSlot slot : cost.queries.get(queryId).plans[planId].slots) {
                                for (SeqInumSlotIndexCost c : slot.costs) {
                                    if (c.index.id == indexId)
                                        slot.selectedIndex = c;
                                }
                            }
                        }
                    }
                } else if (name.startsWith("CREATE")) {
                    String[] ss = name.split("_");
                    int queryId = Integer.parseInt(ss[1]);
                    int indexId = Integer.parseInt(ss[2]);
                    if (Math.abs(valVar[i] - 1) < 1E-5) {
                        cost.queries.get(queryId).transitionCost += cost.indices
                                .get(indexId).createCost;
                    }
                }
                // Rt.p("%.0f %s", xval[i], name);
                if (Math.abs(valVar[i] - 1) < 1E-5) {
                } else if (Math.abs(valVar[i]) < 1E-5) {
                } else {
                    throw new Error("Not binary " + valVar[i]);
                }
            }
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
