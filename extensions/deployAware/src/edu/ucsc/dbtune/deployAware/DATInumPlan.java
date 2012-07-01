package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import edu.ucsc.dbtune.seq.bip.def.SeqInumPlan;

public class DATInumPlan {
    DATQuery query;
    SeqInumPlan p;
    IloNumVar active;
    DATSlot[] slots;

    public DATInumPlan(CPlexWrapper cplex, DATQuery query, SeqInumPlan p)
            throws IloException {
        this.query = query;
        this.p = p;
        this.active = cplex.createBinaryVar();
        active.setName("PLAN_" + p.query.id + "_" + p.id);
        cplex.addVar(active);
        slots = new DATSlot[p.slots.length];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = new DATSlot(cplex, this, p.slots[i]);
        }
    }

    public void addObjective(double coefficient, IloLinearNumExpr expr)
            throws IloException {
        expr.addTerm(coefficient * p.internalCost, active);
        for (DATSlot slot : slots)
            slot.addObjective(expr, coefficient);
    }

    public double getCost(CPlexWrapper cplex) throws IloException {
        double cost = 0;
        cost += p.internalCost * cplex.getValue(active);
        for (DATSlot slot : slots) {
            double scost = slot.getCost(cplex);
            // Rt.p(query.q.id + " " + scost);
            cost += scost;
        }
        return cost;
    }

    public void addConstriant(CPlexWrapper cplex) throws IloException {
        for (DATSlot slot : slots)
            slot.addConstriant(cplex);
    }
}
