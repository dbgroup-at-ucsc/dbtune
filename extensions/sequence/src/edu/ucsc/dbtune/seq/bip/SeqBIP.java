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
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;

/**
 * @author Rui Wang
 */
public class SeqBIP extends AbstractBIPSolver {
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
                String name = "INDEX_" + s.plan.query.id;
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
            cplex.addEq(expr, 1);
//            Rt.p(expr.toString()+"==1");
            for (int i = 0; i < indexes.length; i++) {
                expr = cplex.linearNumExpr();
                expr.addTerm(1, useIndex[i]);
                expr.addTerm(-1, plan.query.index2present.get(indexes[i]));
                cplex.addLe(expr, 0);
//                Rt.p(expr.toString()+"<=0");
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
            expr.addTerm(p.baseCost, active);
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
                        + cost.indicesV.get(i).id);
                this.drop[i].setName("DROP_" + q.id + "_"
                        + cost.indicesV.get(i).id);
                this.present[i].setName("PRESENT_" + q.id + "_"
                        + cost.indicesV.get(i).id);
                index2present.put(cost.indicesV.get(i), present[i]);
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
                expr.addTerm(cost.indicesV.get(i).createCost, create[i]);
                expr.addTerm(cost.indicesV.get(i).dropCost, drop[i]);
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
//            Rt.p(expr.toString()+"==1");
            // index can't be created and droped at the same step
            for (int i = 0; i < totalIndices; i++) {
                expr = cplex.linearNumExpr();
                expr.addTerm(1, this.create[i]);
                expr.addTerm(1, this.drop[i]);
                cplex.addLe(expr, 1);
//                Rt.p(expr.toString()+"<=1");
            }
        }
    }

    SeqInumCost cost;
    IloNumVar[] iloVar = new IloNumVar[0];
    int totalQueires;
    int totalIndices;
    Query[] queries;
    Logger log=Logger.getLogger(SeqBIP.class.getName());

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

            IloLinearNumExpr expr = cplex.linearNumExpr();
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i].addObjective(expr);
            }
            IloObjective obj = cplex.minimize(expr);
//            Rt.p("Obj: "+expr.toString());
            cplex.add(obj);
            
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i].addConstriant();
                for (int k = 0; k < totalIndices; k++) {
                    expr = cplex.linearNumExpr();
                    for (int j = 0; j <= i; j++) {
                        expr.addTerm(1, this.queries[j].create[k]);
                        expr.addTerm(-1, this.queries[j].drop[k]);
                    }
//                    Rt.p(expr.toString()+"="+this.queries[i].present[k]);
                    cplex.addEq(expr, this.queries[i].present[k]);
                }
            }
            
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
            output.indexUsed=new Vector[cost.sequence.length];
            for (int i=0;i<output.indexUsed.length;i++)
                output.indexUsed[i]=new Vector<SeqInumIndex>();
            double[] xval = cplex.getValues(iloVar);
            for (int i = 0; i < xval.length; i++) {
                String name = iloVar[i].getName();
                if (name.startsWith("PRESENT")) {
                    String[] ss = name.split("_");
                    int queryId = Integer.parseInt(ss[1]);
                    int indexId = Integer.parseInt(ss[2]);
                    if (Math.abs(valVar[i] - 1) < 1E-5)
                        output.indexUsed[queryId].add(cost.indicesV.get(indexId));
                }
//                Rt.p("%.0f %s", xval[i], name);
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
