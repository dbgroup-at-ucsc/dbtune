package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloObjective;
import ilog.cplex.IloCplex.UnknownObjectException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;

public class DATWindow {
    int id;
    double costConstraint;
    public double weight = 1;
    boolean lastWindow;
    DATQuery[] queries;
    IloNumVar[] create, drop, present;
    Hashtable<SeqInumIndex, Integer> index2id = new Hashtable<SeqInumIndex, Integer>();
    Hashtable<SeqInumIndex, IloNumVar> index2present = new Hashtable<SeqInumIndex, IloNumVar>();
    boolean[] indexPresent;
    SeqInumCost costModel;
    int totalQueires;
    int totalIndices;

    public DATWindow(SeqInumCost costModel, CPlexWrapper cplex, int id, boolean lastWindow, double costConstraint)
            throws IloException {
        this.costModel = costModel;
        this.totalIndices = costModel.indices.size();
        this.id = id;
        this.lastWindow = lastWindow;
        this.costConstraint = costConstraint;
        if (costModel.queryMap != null) {
            int[] queryMap = costModel.queryMap[id];
            this.totalQueires = queryMap.length;
            this.queries = new DATQuery[totalQueires];
            int pos = 0;
            for (int queryId : queryMap)
                this.queries[pos++] = new DATQuery(cplex, this, costModel.queries.get(queryId));
        } else {
            this.totalQueires = costModel.queries.size();
            this.queries = new DATQuery[totalQueires];
            for (int i = 0; i < totalQueires; i++) {
                this.queries[i] = new DATQuery(cplex, this, costModel.queries.get(i));
            }
        }
        this.create = cplex.createBinaryVars(totalIndices);
        this.drop = cplex.createBinaryVars(totalIndices);
        this.present = cplex.createBinaryVars(totalIndices);
        for (int i = 0; i < totalIndices; i++) {
            this.create[i].setName("CREATE_" + id + "_" + costModel.indices.get(i).id);
            this.drop[i].setName("DROP_" + id + "_" + costModel.indices.get(i).id);
            this.present[i].setName("PRESENT_" + id + "_" + costModel.indices.get(i).id);
            index2id.put(costModel.indices.get(i), i);
            index2present.put(costModel.indices.get(i), present[i]);
        }
        cplex.addVars(create);
        cplex.addVars(drop);
        cplex.addVars(present);
    }

    public void addObjective(IloLinearNumExpr expr, double coefficient) throws IloException {
        coefficient *= weight;
        for (DATQuery query : queries)
            query.addObjective(expr, coefficient);
        if (costModel.addTransitionCostToObjective) {
            for (int i = 0; i < totalIndices; i++) {
                expr.addTerm(costModel.indices.get(i).createCost, create[i]);
                expr.addTerm(costModel.indices.get(i).dropCost, drop[i]);
                // The purpose of following objective is to
                // remove a index when it's not necessary
                // expr.addTerm(0.1, present[i]);
            }
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

    public double getCost(CPlexWrapper cplex, DatabaseSystem db, DB2Optimizer optimizer) throws Exception {
        double cost = 0;
        HashSet<Index> allIndexes = new HashSet<Index>();
        for (int j = 0; j < totalIndices; j++)
            if (cplex.getValue(this.present[j]) == 1)
                allIndexes.add(costModel.indices.get(j).loadIndex(db));
        for (DATQuery query : queries) {
            ExplainedSQLStatement explain = optimizer.explain(query.q.sql, allIndexes);
            double qcost = explain.getTotalCost();
            cost += qcost;
        }
        return cost;
    }
    
    // -------------------------------------------------------------
    // TODO: Trung's modification
    // -------------------------------------------------------------
    public void showUsedIndex(CPlexWrapper cplex, DatabaseSystem db, InumOptimizer optimizer) throws Exception {
        
        HashSet<Index> allIndexes = new HashSet<Index>();
        for (int j = 0; j < totalIndices; j++)
            if (cplex.getValue(this.present[j]) == 1)
                allIndexes.add(costModel.indices.get(j).loadIndex(db));
        int count = 0;
        for (DATQuery query : queries) {
            ExplainedSQLStatement explain = optimizer.explain(query.q.sql, allIndexes);
            
            System.out.println(explain.getTotalCost());
            if (count == 12) {
                Rt.p(" query " + query.q.sql.toString());
                Set<Index> test = new HashSet<Index>();
                
                for (Index i : explain.getUsedConfiguration()) {
                    Rt.p(i.toString());
                    if (i.getTable().getName().contains("CATALOG_SALES"))
                        test.add(i);
                }
                Rt.p(" cost = " + explain.getTotalCost());
                Rt.p("-----------------");
                
                Rt.p(" test conf " + test);
                ExplainedSQLStatement explain1 = optimizer.explain(query.q.sql, test);
                for (Index i : explain1.getUsedConfiguration()) {
                    Rt.p(i.toString());
                }
                
            }
            count++;
            
        }
        
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

    public void addConstriant(CPlexWrapper cplex, double spaceConstraint, int maxIndexCreatedPerWindow)
            throws IloException {
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
            if (0 <= this.costConstraint && this.costConstraint < Double.MAX_VALUE) {
                expr = cplex.linearNumExpr();
                for (int i = 0; i < totalIndices; i++) {
                    expr.addTerm(costModel.indices.get(i).createCost, this.create[i]);
                }
                cplex.addLe(expr, this.costConstraint);
                if (DAT.showFormulas)
                    Rt.p(expr.toString() + "<=" + this.costConstraint);
            }
            expr = cplex.linearNumExpr();
            for (int i = 0; i < totalIndices; i++) {
                expr.addTerm(costModel.indices.get(i).storageCost, this.present[i]);
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
            // cplex.addEq(this.present[i], 1);
        }

    }

    public void getValues(CPlexWrapper cplex) throws UnknownObjectException, IloException {
        indexPresent = new boolean[totalIndices];
        for (int j = 0; j < totalIndices; j++) {
            indexPresent[j] = cplex.getValue(present[j]) == 1;
        }
    }

    public static double costWithIndex(SeqInumCost costModel, int i) throws IloException {
        boolean[] present = new boolean[costModel.indexCount()];
        Arrays.fill(present, false);
        present[i] = true;
        return costWithIndex(costModel, present);
    }

    public static double costWithIndex(SeqInumCost costModel, boolean[] present) throws IloException {
        CPlexWrapper cplex = new CPlexWrapper();
        DATWindow window = new DATWindow(costModel, cplex, 0, true, Double.MAX_VALUE);
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
