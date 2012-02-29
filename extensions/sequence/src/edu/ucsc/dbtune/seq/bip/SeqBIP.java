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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.seq.bip.def.*;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;

/**
 * @author Rui Wang
 */
public class SeqBIP extends AbstractBIPSolver {
    class Slot {
        SeqInumSlot s;
        IloNumVar[] indexes;

        public Slot(SeqInumSlot s) throws IloException {
            this.s = s;
            indexes = new IloNumVar[s.costs.size() + 1];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = createBinaryVar();
                String name = "INDEX " + s.plan.query;
                if (i < s.costs.size())
                    name += "," + s.costs.get(i).index.name;
                else
                    name += ",fullTableScan";
                indexes[i].setName(name);
            }
            addVars(indexes);
        }

        public void addObjective(IloLinearNumExpr expr) throws IloException {
            for (int i = 0; i < s.costs.size(); i++) {
                expr.addTerm(s.costs.get(i).cost, indexes[i]);
            }
            expr.addTerm(s.fullTableScanCost, indexes[s.costs.size()]);
        }

        public void addConstriant() {

        }
    }

    class InumPlan {
        SeqInumPlan p;
        IloNumVar active;
        Slot[] slots;

        public InumPlan(SeqInumPlan p) throws IloException {
            this.p = p;
            this.active = createBinaryVar();
            active.setName("PLAN " + p.query.name + "," + p.id);
            addVar(active);
            slots = new Slot[p.slots.length];
            for (int i = 0; i < slots.length; i++) {
                // Rt.p(p.query.name+" "+p.id+" "+p.slots[i]);
                slots[i] = new Slot(p.slots[i]);
            }
        }

        public void addObjective(IloLinearNumExpr expr) throws IloException {
            expr.addTerm(p.baseCost, active);
            for (Slot slot : slots)
                slot.addObjective(expr);
        }

        public void addConstriant() {
            for (Slot slot : slots)
                slot.addConstriant();
        }
    }

    class Query {
        SeqInumQuery q;
        IloNumVar create, drop, present;
        InumPlan[] plans;

        public Query(SeqInumQuery q) throws IloException {
            this.q = q;
            this.create = createBinaryVar();
            this.drop = createBinaryVar();
            this.present = createBinaryVar();
            addVar(create);
            addVar(drop);
            addVar(present);
            plans = new InumPlan[q.plans.length];
            for (int i = 0; i < plans.length; i++) {
                plans[i] = new InumPlan(q.plans[i]);
            }
        }

        public void addObjective(IloLinearNumExpr expr) throws IloException {
            for (InumPlan plan : plans)
                plan.addObjective(expr);
        }

        public void addConstriant() {
            for (InumPlan plan : plans)
                plan.addConstriant();
        }
    }

    SeqInumCost cost;
    IloNumVar[] iloVar = new IloNumVar[0];
    int totalQueires;
    int totalIndices;
    Query[] queries;

    public SeqBIP(SeqInumCost cost) throws IloException {
        this.cost = cost;
        this.totalQueires = cost.sequence.length;
        this.totalIndices = cost.indicesV.size();
    }

    @Override
    protected final void buildBIP() {
        super.numConstraints = 0;
        try {
            this.queries = new Query[totalQueires];
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i] = new Query(cost.sequence[i]);
            }
            cplex.add(iloVar);

            cplexVar = new Vector<IloNumVar>();
            for (IloNumVar var : iloVar)
                cplexVar.add(var);

            IloLinearNumExpr expr = cplex.linearNumExpr();
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i].addObjective(expr);
            }

            IloObjective obj = cplex.minimize(expr);
            cplex.add(obj);
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i].addConstriant();
            }
            // cplex.addLe(cplex.sum(cplex.negative(iloVar[0]), iloVar[1],
            // iloVar[2]), 20);
            cplex.solve();
            Object objVal = cplex.getObjValue();
            double[] xval = cplex.getValues(iloVar);
            for (int i = 0; i < xval.length; i++) {
                Rt.p(xval[i]);
            }
        } catch (IloException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected IndexTuningOutput getOutput() {
        SebBIPOutput output = new SebBIPOutput();
        try {
            double[] xval = cplex.getValues(iloVar);
            for (int i = 0; i < xval.length; i++) {
                if (Math.abs(valVar[i] - 1) < 1E-20) {
                    System.out.println(i + " " + valVar[i]);
                } else if (Math.abs(valVar[i]) < 1E-20) {
                } else {
                    throw new Error("Not binary");
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
