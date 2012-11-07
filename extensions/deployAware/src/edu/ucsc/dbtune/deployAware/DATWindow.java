package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloObjective;
import ilog.cplex.IloCplex.UnknownObjectException;

import java.util.Arrays;
import java.util.Hashtable;

import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.util.Rt;

public class DATWindow {
    int id;
    double costConstraint;
    boolean lastWindow;
    DATQuery[] queries;
    IloNumVar[] create, drop, present;
    Hashtable<SeqInumIndex, Integer> index2id = new Hashtable<SeqInumIndex, Integer>();
    Hashtable<SeqInumIndex, IloNumVar> index2present = new Hashtable<SeqInumIndex, IloNumVar>();
    boolean[] indexPresent;
    SeqInumCost costModel;
    int totalQueires;
    int totalIndices;

    public DATWindow(SeqInumCost costModel, CPlexWrapper cplex, int id,
            boolean lastWindow, double costConstraint) throws IloException {
        this.costModel = costModel;
        this.totalQueires = costModel.queries.size();
        this.totalIndices = costModel.indices.size();
        this.id = id;
        this.lastWindow = lastWindow;
        this.costConstraint = costConstraint;
        this.queries = new DATQuery[totalQueires];
        for (int i = 0; i < totalQueires; i++) {
            this.queries[i] = new DATQuery(cplex, this, costModel.queries
                    .get(i));
        }
        this.create = cplex.createBinaryVars(totalIndices);
        this.drop = cplex.createBinaryVars(totalIndices);
        this.present = cplex.createBinaryVars(totalIndices);
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
        cplex.addVars(create);
        cplex.addVars(drop);
        cplex.addVars(present);
    }

    public void addObjective(IloLinearNumExpr expr, double coefficient)
            throws IloException {
        for (DATQuery query : queries)
            query.addObjective(expr, coefficient);
        for (int i = 0; i < totalIndices; i++) {
            // expr.addTerm(costModel.indices.get(i).createCost, create[i]);
            // expr.addTerm(costModel.indices.get(i).dropCost, drop[i]);
            // The purpose of following objective is to
            // remove a index when it's not necessary
            // expr.addTerm(0.1, present[i]);
        }
    }

    public double getCost(CPlexWrapper cplex) throws IloException {
        double cost = 0;
        for (DATQuery query : queries) {
            double qcost = query.getCost(cplex);
            // Rt.p(query.q.id + " " + qcost);
            cost += qcost;
        }
        return cost;
    }

    public int getCreated(CPlexWrapper cplex) throws IloException {
        int n = 0;
        for (int i = 0; i < totalIndices; i++)
            if (cplex.getValue(this.create[i]) == 1)
                n++;
        return n;
    }

    public int getDropped(CPlexWrapper cplex) throws IloException {
        int n = 0;
        for (int i = 0; i < totalIndices; i++)
            if (cplex.getValue(this.drop[i]) == 1)
                n++;
        return n;
    }
    
    public int getPresent(CPlexWrapper cplex) throws IloException {
        int n = 0;
        for (int i = 0; i < totalIndices; i++)
            if (cplex.getValue(this.present[i]) == 1)
                n++;
        return n;
    }

    public void addConstriant(CPlexWrapper cplex, double spaceConstraint,
            int maxIndexCreatedPerWindow) throws IloException {
        for (DATQuery query : queries)
            query.addConstriant(cplex);
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

        if (totalIndices > 0) {
            expr = cplex.linearNumExpr();
            for (int i = 0; i < totalIndices; i++) {
                expr.addTerm(costModel.indices.get(i).createCost,
                        this.create[i]);
            }
            cplex.addLe(expr, this.costConstraint);
            if (DAT.showFormulas)
                Rt.p(expr.toString() + "<=" + this.costConstraint);
            expr = cplex.linearNumExpr();
            for (int i = 0; i < totalIndices; i++) {
                expr.addTerm(costModel.indices.get(i).storageCost,
                        this.present[i]);
            }
            if (spaceConstraint > 1E-5) {
                cplex.addLe(expr, spaceConstraint);
                if (DAT.showFormulas)
                    Rt.p(expr.toString() + "<=" + spaceConstraint);
            }

            if (maxIndexCreatedPerWindow > 0) {
                expr = cplex.linearNumExpr();
                for (int i = 0; i < totalIndices; i++) {
                    expr.addTerm(1, this.create[i]);
                }
                cplex.addLe(expr, maxIndexCreatedPerWindow);
                if (DAT.showFormulas)
                    Rt.p(expr.toString() + "<=" + maxIndexCreatedPerWindow);
            }
        }

        // index can't be created and droped at the same step
        for (int i = 0; i < totalIndices; i++) {
            expr = cplex.linearNumExpr();
            expr.addTerm(1, this.create[i]);
            expr.addTerm(1, this.drop[i]);
            cplex.addLe(expr, 1);
            if (DAT.showFormulas)
                Rt.p(expr.toString() + "<=1");
        }
    }

    public void getValues(CPlexWrapper cplex) throws UnknownObjectException,
            IloException {
        indexPresent = new boolean[totalIndices];
        for (int j = 0; j < totalIndices; j++) {
            indexPresent[j] = cplex.getValue(present[j]) == 1;
        }
    }

    public static double costWithIndex(SeqInumCost costModel, int i)
            throws IloException {
        boolean[] present = new boolean[costModel.indexCount()];
        Arrays.fill(present, false);
        present[i] = true;
        return costWithIndex(costModel, present);
    }

    public static double costWithIndex(SeqInumCost costModel, boolean[] present)
            throws IloException {
        CPlexWrapper cplex = new CPlexWrapper();
        DATWindow window = new DATWindow(costModel, cplex, 0, true,
                Double.MAX_VALUE);
        IloLinearNumExpr expr = cplex.linearNumExpr();
        window.addObjective(expr, 1);
        IloObjective obj = cplex.minimize(expr);
        cplex.add(obj);
        if (DAT.showFormulas)
            Rt.p("Obj: " + expr.toString());
        window.addConstriant(cplex, 0, 0);
        for (int k = 0; k < costModel.indexCount(); k++) {
            cplex.addEq(window.present[k], present[k] ? 1 : 0);
            cplex.addEq(window.create[k], 0);
            cplex.addEq(window.drop[k], 0);
        }
        if (!cplex.solve())
            throw new Error();
        window.getValues(cplex);
        double cost = window.getCost(cplex);
        // Rt.p(Rt.booleanToString(window.indexPresent));
        return cost;
    }
}
