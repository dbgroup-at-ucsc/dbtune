package edu.ucsc.dbtune.seq.bip;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.concert.IloObjective;
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

import edu.ucsc.dbtune.seq.bip.def.*;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.deployAware.CPlexWrapper;

/**
 * @author Rui Wang
 */
public class SeqBIP {
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
                useIndex[i] = cplex.createBinaryVar();
                String name = "INDEX_" + s.plan.query.id + "_" + s.plan.id;
                if (i < s.costs.size()) {
                    indexes[i] = s.costs.get(i).index;
                    name += "_" + indexes[i].id;
                } else
                    name += "_FTS";
                useIndex[i].setName(name);
            }
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
            this.active = cplex.createBinaryVar();
            active.setName("PLAN_" + p.query.id + "_" + p.id);
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
            this.create = cplex.createBinaryVars(totalIndices);
            this.drop = cplex.createBinaryVars(totalIndices);
            this.present = cplex.createBinaryVars(totalIndices);
            for (int i = 0; i < totalIndices; i++) {
                this.create[i].setName("CREATE_" + q.id + "_"
                        + cost.indices.get(i).id);
                this.drop[i].setName("DROP_" + q.id + "_"
                        + cost.indices.get(i).id);
                this.present[i].setName("PRESENT_" + q.id + "_"
                        + cost.indices.get(i).id);
                index2present.put(cost.indices.get(i), present[i]);
            }
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

    CPlexWrapper cplex = new CPlexWrapper();
    public static boolean showFormulas = false;
    SeqInumCost cost;
    IloNumVar[] iloVar = new IloNumVar[0];
    int totalQueires;
    int totalIndices;
    Query[] queries;
    Logger log = Logger.getLogger(SeqBIP.class.getName());

    public SeqBIP(SeqInumCost cost) throws IloException {
        this.cost = cost;
        this.totalQueires = cost.queries.size();
        this.totalIndices = cost.indices.size();
    }

    public SebBIPOutput solve() {
        try {
            this.queries = new Query[totalQueires];
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i] = new Query(cost.queries.get(i));
            }

            IloLinearNumExpr expr = cplex.linearNumExpr();
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i].addObjective(expr);
            }
            IloObjective obj = cplex.minimize(expr);
            cplex.add(obj);
            if (showFormulas)
                Rt.p("Obj: " + expr.toString());

            for (int i = 0; i < totalQueires; i++) {
                this.queries[i].addConstriant();
                for (int k = 0; k < totalIndices; k++) {
                    expr = cplex.linearNumExpr();
                    if (i > 0)
                        expr.addTerm(1, this.queries[i - 1].present[k]);
                    // for (int j = 0; j <= i; j++) {
                    expr.addTerm(1, this.queries[i].create[k]);
                    expr.addTerm(-1, this.queries[i].drop[k]);
                    // }
                    if (showFormulas)
                        Rt
                                .p(expr.toString() + "="
                                        + this.queries[i].present[k]);
                    cplex.addEq(expr, this.queries[i].present[k]);
                }
            }

            cplex.solve();
        } catch (IloException e) {
            e.printStackTrace();
        }

        SebBIPOutput output = new SebBIPOutput();
        try {
            output.indexUsed = new Vector[cost.queries.size()];
            for (int i = 0; i < output.indexUsed.length; i++)
                output.indexUsed[i] = new Vector<SeqInumIndex>();
            for (int i = 0; i < queries.length; i++) {
                Query q = queries[i];
                for (int j = 0; j < queries[i].present.length; j++) {
                    int a = cplex.getValue(queries[i].present[j]);
                    if (a == 1)
                        output.indexUsed[i].add(cost.indices.get(j));
                }
                // cost.queries.get(i).selectedPlan = cost.queries
                // .get(i).plans[planId];
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return output;
    }

    protected final void buildBIPOneByOne() {
        try {
            this.queries = new Query[totalQueires];
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i] = new Query(cost.queries.get(i));
            }

            IloLinearNumExpr exprObj = cplex.linearNumExpr();
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i].addObjective(exprObj);
                IloObjective obj = cplex.minimize(exprObj);
                cplex.add(obj);
                if (showFormulas)
                    Rt.p("Obj: " + exprObj.toString());

                this.queries[i].addConstriant();
                for (int k = 0; k < totalIndices; k++) {
                    IloLinearNumExpr expr = cplex.linearNumExpr();
                    if (i > 0)
                        expr.addTerm(1, this.queries[i - 1].present[k]);
                    // for (int j = 0; j <= i; j++) {
                    expr.addTerm(1, this.queries[i].create[k]);
                    expr.addTerm(-1, this.queries[i].drop[k]);
                    // }
                    if (showFormulas)
                        Rt
                                .p(expr.toString() + "="
                                        + this.queries[i].present[k]);
                    cplex.addEq(expr, this.queries[i].present[k]);
                }
                RTimerN timer = new RTimerN();
                cplex.solve();
                Rt.np("queries=%d time=%.3f cost=%.2f", i, timer
                        .getSecondElapse(), cplex.getObjValue());
                // if (i < totalQueires - 1)
                // cplex.remove(obj);
            }

        } catch (IloException e) {
            e.printStackTrace();
        }
    }

    public double getObjValue() throws IloException {
        return cplex.getObjValue();
    }
}
