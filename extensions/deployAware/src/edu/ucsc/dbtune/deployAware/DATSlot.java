package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlot;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlotIndexCost;
import edu.ucsc.dbtune.util.Rt;

public class DATSlot {
    DATInumPlan plan;
    SeqInumSlot s;
    SeqInumIndex[] indexes;
    IloNumVar[] useIndex;

    public DATSlot(CPlexWrapper cplex, DATInumPlan plan, SeqInumSlot s)
            throws IloException {
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
        cplex.addVars(useIndex);
    }

    public void addObjective(IloLinearNumExpr expr, double coefficient)
            throws IloException {
        for (int i = 0; i < s.costs.size(); i++) {
            SeqInumSlotIndexCost c = s.costs.get(i);
            expr
                    .addTerm((c.cost + c.updateCost) * coefficient,
                            useIndex[i]);
        }
        expr.addTerm(s.fullTableScanCost * coefficient, useIndex[s.costs
                .size()]);
    }

    public double getCost(CPlexWrapper cplex) throws IloException {
        double cost = 0;
        for (int i = 0; i < s.costs.size(); i++) {
            SeqInumSlotIndexCost c = s.costs.get(i);
            // Rt.p((c.cost + c.updateCost) + " " + getValue(useIndex[i]));
            cost += (c.cost + c.updateCost) * cplex.getValue(useIndex[i]);
        }
        cost += s.fullTableScanCost
                * cplex.getValue(useIndex[s.costs.size()]);
        return cost;
    }

    public void addConstriant(CPlexWrapper cplex) throws IloException {
        IloLinearNumExpr expr = cplex.linearNumExpr();
        for (IloNumVar var : useIndex)
            expr.addTerm(1, var);
        cplex.addEq(expr, plan.active);
        if (DAT.showFormulas)
            Rt.p(expr.toString() + "==" + plan.active);
        for (int i = 0; i < indexes.length; i++) {
            expr = cplex.linearNumExpr();
            expr.addTerm(1, useIndex[i]);
            expr.addTerm(-1, plan.query.window.index2present
                    .get(indexes[i]));
            cplex.addLe(expr, 0);
            if (DAT.showFormulas)
                Rt.p(expr.toString() + "<=0");
        }
    }
}
