package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.concert.IloObjective;
import ilog.cplex.IloCplex;
import ilog.cplex.IloCplex.UnknownObjectException;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;
import java.util.logging.Logger;

import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.bip.def.SeqInumPlan;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlot;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlotIndexCost;
import edu.ucsc.dbtune.seq.utils.PerfTest;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

/**
 * @author Rui Wang
 */
public class DAT {
    public static boolean showFormulas = false;
    public DATWindow[] windows;
    public Rx debug;

    public DATOutput runDAT(DATParameter param) throws IloException {
        // Rt.p("alpha=" + param.alpha);
        // Rt.p("beta=" + param.beta);
        // Rt.p("m=" + param.windowConstraints.length);
        // Rt.p("space=" + param.spaceConstraint);
        // Rt.p("window=" + param.windowConstraints[0]);
        // Rt.p("l=" + param.maxIndexCreatedPerWindow);
        CPlexWrapper cplex = new CPlexWrapper();
        windows = new DATWindow[param.windowConstraints.length];
        for (int i = 0; i < param.windowConstraints.length; i++) {
            windows[i] = new DATWindow(param.costModel, cplex, i,
                    i == param.windowConstraints.length - 1,
                    param.windowConstraints[i]);
        }
        IloLinearNumExpr expr = cplex.linearNumExpr();
        for (int i = 0; i < windows.length; i++) {
            this.windows[i].addObjective(expr,
                    i == windows.length - 1 ? param.beta : param.alpha);
        }
        IloObjective obj = cplex.minimize(expr);
        cplex.add(obj);
        if (showFormulas)
            Rt.p("Obj: " + expr.toString());

        if (param.intermediateConstraint > 0.1) {
            expr = cplex.linearNumExpr();
            for (int i = 0; i < windows.length - 1; i++) {
                this.windows[i].addObjective(expr, 1);
            }
            cplex.addLe(expr, param.intermediateConstraint);
            if (showFormulas)
                Rt.p(expr.toString() + "<=" + param.intermediateConstraint);
        }
        for (int i = 0; i < windows.length; i++) {
            this.windows[i].addConstriant(cplex, param.spaceConstraint,
                    param.maxIndexCreatedPerWindow);
            for (int k = 0; k < param.totalIndices; k++) {
                expr = cplex.linearNumExpr();
                if (i > 0)
                    expr.addTerm(1, this.windows[i - 1].present[k]);
                // for (int j = 0; j <= i; j++) {
                expr.addTerm(1, this.windows[i].create[k]);
                expr.addTerm(-1, this.windows[i].drop[k]);
                // }
                if (showFormulas)
                    Rt.p(expr.toString() + "=" + this.windows[i].present[k]);
                cplex.addEq(expr, this.windows[i].present[k]);
            }
        }

        PerfTest.startTimer();
        if (!cplex.solve())
            throw new Error("Can't solve bip");
        PerfTest.addTimer("dat");

        DATOutput output = new DATOutput(windows.length);
        try {
            for (DATWindow w : windows)
                w.getValues(cplex);
            double totalCost = 0;
            for (int i = 0; i < output.ws.length; i++) {
                output.ws[i].indexUsed = Arrays.copyOf(windows[i].indexPresent,
                        param.totalIndices);
                output.ws[i].cost = windows[i].getCost(cplex);
                output.ws[i].create = windows[i].getCreated(cplex);
                output.ws[i].drop = windows[i].getDropped(cplex);
                // Rt.p(Rt.booleanToString(output.ws[i].indexUsed));
                PerfTest.startTimer();
                double c2 = DATWindow.costWithIndex(param.costModel,
                        output.ws[i].indexUsed);
                PerfTest.addTimer("dat verify");
                Rt.p("DAT Window %d: %,.0f\t%,.0f\tusedIndexes=%d", i, c2,
                        output.ws[i].cost, windows[i].getPresent(cplex));
                if (Math.abs(output.ws[i].cost - c2) / c2 > 0.1) {
                    Rt.error("ERROR: verify failed " + output.ws[i].cost + " "
                            + c2);
                    output.ws[i].cost = c2;
                }
                if (i == output.ws.length - 1)
                    totalCost += param.beta * output.ws[i].cost;
                else
                    totalCost += param.alpha * output.ws[i].cost;
                // int total = 0;
                // for (boolean b : output.ws[i].indexUsed)
                // total++;
                // Rt.p(total);
                // Rt.p(this.costWithIndex(output.ws[i].indexUsed));
            }
            output.totalCost = totalCost;
            if (debug != null) {
                Rx root = debug.createChild("dat");
                root.setAttribute("cost", output.totalCost);
                for (int windowId = 0; windowId < windows.length; windowId++) {
                    Rx window = root.createChild("window");
                    saveDebugInfo(window, param, cplex, windows[windowId]);
                }
            }
            // Rt.p(cplex.getValue(cplex.getObjective().getExpr()));
            // Rt.p(cplex.getValue(objExpr));
            // if (Math.abs(totalCost - cplex.getObjValue()) > 1)
            // throw new Error(totalCost+" "+cplex.getObjValue());
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return output;
    }

    public static void saveDebugInfo(Rx window, DATParameter param,
            CPlexWrapper cplex, DATWindow w) throws IloException {
        window.setAttribute("cost", w.getCost(cplex));
        window.setAttribute("created", w.getCreated(cplex));
        window.setAttribute("dropped", w.getDropped(cplex));
        boolean[] indexUsed = Arrays.copyOf(w.indexPresent, param.totalIndices);
        Rx usedIndex = window.createChild("present");
        for (int i = 0; i < indexUsed.length; i++) {
            if (indexUsed[i]) {
                usedIndex.createChild("index", i);
            }
        }
        Rx createdIndex = window.createChild("created");
        for (int i = 0; i < param.totalIndices; i++)
            if (cplex.getValue(w.create[i]) == 1)
                createdIndex.createChild("index", i);
        Rx droppedIndex = window.createChild("dropped");
        for (int i = 0; i < param.totalIndices; i++)
            if (cplex.getValue(w.drop[i]) == 1)
                droppedIndex.createChild("index", i);
        for (DATQuery query : w.queries) {
            Rx qx = window.createChild("query");
            qx.setAttribute("id", query.q.id);
            for (DATInumPlan plan : query.plans) {
                Rx px = qx.createChild("plan");
                px.setAttribute("id", plan.p.id);
                boolean active = cplex.getValue(plan.active) == 1;
                px.setAttribute("active", active);
                for (DATSlot slot : plan.slots) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < slot.useIndex.length; i++) {
                        if (cplex.getValue(slot.useIndex[i]) == 1) {
                            if (sb.length() > 0)
                                sb.append(",");
                            if (i < slot.indexes.length)
                                sb.append(slot.indexes[i].id);
                            else
                                sb.append("FTS");
                        }
                    }
                    if (sb.length() > 0)
                        px.createChild("slot", sb.toString()).setAttribute(
                                "id", slot.s.id);
                }
            }
        }
    }
}
