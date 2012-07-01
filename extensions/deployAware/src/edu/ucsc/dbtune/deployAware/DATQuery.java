package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.utils.Rt;

public class DATQuery {
    DATWindow window;
    SeqInumQuery q;
    DATInumPlan[] plans;

    public DATQuery(CPlexWrapper cplex, DATWindow window, SeqInumQuery q)
            throws IloException {
        this.window = window;
        this.q = q;
        plans = new DATInumPlan[q.plans.length];
        for (int i = 0; i < plans.length; i++) {
            plans[i] = new DATInumPlan(cplex, this, q.plans[i]);
        }
    }

    public void addObjective(IloLinearNumExpr expr, double coefficient)
            throws IloException {
        for (DATInumPlan plan : plans)
            plan.addObjective(coefficient, expr);
    }

    public double getCost(CPlexWrapper cplex) throws IloException {
        double cost = 0;
        cost += q.baseTableUpdateCost;
        for (DATInumPlan plan : plans) {
            double pcost = plan.getCost(cplex);
            // Rt.p(this.q.id + " " + plan.p.id + " " + pcost);
            cost += pcost;
        }
        return cost;
    }

    public void addConstriant(CPlexWrapper cplex) throws IloException {
        for (DATInumPlan plan : plans)
            plan.addConstriant(cplex);
        if (plans.length > 0) {
            // One and only one plan can be used
            IloLinearNumExpr expr = cplex.linearNumExpr();
            for (DATInumPlan plan : plans)
                expr.addTerm(1, plan.active);
            cplex.addEq(expr, 1);
            if (DAT.showFormulas)
                Rt.p(expr.toString() + "==1");
        }
    }
}
