package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloObjective;

import java.util.Arrays;
import java.util.Vector;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

public class DATBaselines {
    public static boolean[] cophy(DATParameter param, double total,
            double maxIndexCost, int maxIndices) throws IloException {
        CPlexWrapper cplex = new CPlexWrapper();
        DATWindow window = new DATWindow(param.costModel, cplex, 0, true, total);
        IloLinearNumExpr expr = cplex.linearNumExpr();
        window.addObjective(expr, 1);
        IloObjective obj = cplex.minimize(expr);
        cplex.add(obj);
        if (DAT.showFormulas)
            Rt.p("Obj: " + expr.toString());
        window.addConstriant(cplex, param.spaceConstraint, maxIndices);
        for (int k = 0; k < param.costModel.indexCount(); k++) {
            if (param.costModel.indices.get(k).createCost >= maxIndexCost)
                cplex.addEq(window.create[k], 0);
            expr = cplex.linearNumExpr();
            expr.addTerm(1, window.create[k]);
            expr.addTerm(-1, window.drop[k]);
            if (DAT.showFormulas)
                Rt.p(expr.toString() + "=" + window.present[k]);
            cplex.addEq(expr, window.present[k]);
        }
        if (!cplex.solve())
            throw new Error();
        window.getValues(cplex);
        boolean[] indexPresent = new boolean[param.costModel.indexCount()];
        for (int j = 0; j < param.costModel.indexCount(); j++) {
            indexPresent[j] = window.indexPresent[j];
        }
        return indexPresent;
    }

    /**
     * @param method
     *            optimal, greedy
     * @return
     */
    public static double baseline2WindowConstraint;

    public static void getIndexBenefit(SeqInumCost costModel)
            throws IloException {
        boolean[] bs = new boolean[costModel.indexCount()];
        // Rt.p("get index benefit 1");
        double costWithoutIndex = DATWindow.costWithIndex(costModel, bs);
        for (SeqInumIndex index : costModel.indices) {
            // Rt.p(index.id);
            index.indexBenefit = costWithoutIndex
                    - DATWindow.costWithIndex(costModel, index.id);
        }
        // Rt.p("get index benefit 2");
        Arrays.fill(bs, true);
        double costWithAllIndex = DATWindow.costWithIndex(costModel, bs);
        for (SeqInumIndex index : costModel.indices) {
            // Rt.p(index.id);
            Arrays.fill(bs, true);
            bs[index.id] = false;
            index.indexBenefit2 = DATWindow.costWithIndex(costModel, bs)
                    - costWithAllIndex;
        }
    }

    public static double[] getQueryCost(SeqInumCost costModel, boolean[] present)
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
        window.addConstriant(cplex, Double.MAX_VALUE, Integer.MAX_VALUE);
        for (int k = 0; k < costModel.indexCount(); k++) {
            cplex.addEq(window.present[k], present[k] ? 1 : 0);
            cplex.addEq(window.create[k], 0);
            cplex.addEq(window.drop[k], 0);
        }
        if (!cplex.solve())
            throw new Error();
        window.getValues(cplex);
        double[] ds = new double[costModel.queries.size()];
        for (int j = 0; j < ds.length; j++) {
            ds[j] = window.queries[j].getCost(cplex);
        }
        return ds;
    }

    public static DATOutput baseline2(DATParameter param, String method,
            Rx debug) {
        if (method.equals("bip"))
            method = "mkp";
        int totalIndices = param.costModel.indexCount();
        DATOutput output = new DATOutput(param.windowConstraints.length);
        double totalCost = 0;
        try {
            double total = 0;
            double maxIndexCost = 0;
            for (double d : param.windowConstraints) {
                total += d;
                if (d > maxIndexCost)
                    maxIndexCost = d;
            }
            double total0 = total;
            boolean[][] indexPresents = new boolean[param.windowConstraints.length][totalIndices];
            // double costWithoutIndex = costWithIndex(new
            // boolean[totalIndices]);
            while (true) {
                boolean[] indexPresent = cophy(param, total, maxIndexCost,
                        param.maxIndexCreatedPerWindow
                                * param.windowConstraints.length);
                // Rt.p(this.costWithIndex(indexPresent));
                Vector<SeqInumIndex> usedIndex = new Vector<SeqInumIndex>();
                for (int i = 0; i < indexPresent.length; i++) {
                    if (indexPresent[i]) {
                        if (param.costModel.indices.get(i).id != i)
                            throw new Error();
                        // costModel.indices.get(i).indexBenefit =
                        // costWithoutIndex
                        // - costWithIndex(i);
                        usedIndex.add(param.costModel.indices.get(i));
                    }
                }
                double[] bins = param.windowConstraints;
                double[] items = new double[usedIndex.size()];
                double[] profits = new double[items.length];
                double totalWeight = 0;
                for (int j = 0; j < items.length; j++) {
                    items[j] = usedIndex.get(j).createCost;
                    totalWeight += items[j];
                    profits[j] = usedIndex.get(j).indexBenefit;
                    // Rt.np(usedIndex.get(j).id + "\t" + items[j] + "\t"
                    // + profits[j]);
                }
                // Rt.np(totalWeight + "/" + total);
                double[] binWeights = new double[param.windowConstraints.length];
                for (int j = 0; j < binWeights.length; j++)
                    binWeights[j] = binWeights.length - j;
                // Rt.p(method + " " + bins.length + " " + items.length);
                int[] belongs = null;
                if ("optimal".equals(method)) {
                    MKPOptimum m = new MKPOptimum(bins, binWeights, items,
                            profits);
                    if (m.cannotFitIn > 0) {
                        Rt.p("can't fit: " + m.cannotFitIn + " " + total);
                        total *= 0.9;
                        continue;
                    }
                    belongs = m.maxBelongs;
                } else if (method.startsWith("greedy")) {
                    MKPGreedy m = new MKPGreedy(bins, binWeights, items,
                            profits, method.equals("greedyRatio"),
                            param.maxIndexCreatedPerWindow);
                    if (m.cannotFitIn > 0) {
                        // Rt.p("can't fit: " + m.cannotFitIn + " "
                        // + m.cannotFitWeight + " " + total);
                        total *= 0.9;
                        continue;
                    }
                    belongs = m.belongs;
                } else if (method.startsWith("bip") || method.startsWith("mkp")) {
                    double[] binWeights2 = new double[param.windowConstraints.length];
                    for (int j = 0; j < binWeights2.length; j++)
                        binWeights2[j] = (binWeights2.length - j);
                    // * alpha
                    // + beta;
                    MKPBip m = new MKPBip(bins, binWeights2, items, profits,
                            param.maxIndexCreatedPerWindow);
                    if (m.cannotFitIn > 0) {
                        // Rt.p("can't fit: " + m.cannotFitIn + " "
                        // + m.cannotFitWeight + " " + total);
                        total *= 0.9;
                        continue;
                    }
                    belongs = m.belongs;
                } else {
                    throw new Error(method);
                }
                for (int i = 0; i < belongs.length; i++) {
                    if (belongs[i] >= 0) {
                        indexPresents[belongs[i]][usedIndex.get(i).id] = true;
                    }
                }
                break;
            }
            // for (int wid = 0; wid < indexPresents.length; wid++) {
            // System.out.print("window " + wid + "\t");
            // for (int j = 0; j < totalIndices; j++) {
            // if (indexPresents[wid][j]) {
            // System.out.format("%d(%,.0f|%,.0f)", j,
            // costModel.indices.get(j).createCost,
            // costModel.indices.get(j).indexBenefit);
            // }
            // }
            // System.out.println();
            // }
            baseline2WindowConstraint = total / total0 * 100;
            for (int wid = 0; wid < indexPresents.length; wid++) {
                output.ws[wid].create = 0;
                for (int j = 0; j < totalIndices; j++) {
                    if (indexPresents[wid][j])
                        output.ws[wid].create++;
                }
                if (wid < 1)
                    continue;
                for (int j = 0; j < totalIndices; j++) {
                    if (indexPresents[wid - 1][j]) {
                        indexPresents[wid][j] = true;
                    }
                }
            }
            // while (true) {
            // boolean[] indexPresent = cophy(total);
            // for (int wid = 0; wid < indexPresents.length; wid++) {
            // if (wid > 0)
            // System.arraycopy(indexPresents[wid - 1], 0,
            // indexPresents[wid], 0,
            // indexPresents[wid].length);
            // double quota = windowConstraints[wid];
            // for (int j = 0; j < totalIndices; j++) {
            // if (indexPresent[j]) {
            // double c2 = costModel.indices.get(j).createCost;
            // if (c2 < quota) {
            // quota -= c2;
            // indexPresent[j] = false;
            // indexPresents[wid][j] = true;
            // }
            // }
            // }
            // }
            // int notFit = 0;
            // for (int j = 0; j < totalIndices; j++) {
            // if (indexPresent[j]) {
            // notFit++;
            // }
            // }
            // Rt.p("can't fit: " + notFit);
            // if (notFit == 0)
            // break;
            // total *= 0.9;
            // }
            // for (int wid = 0; wid < indexPresents.length; wid++) {
            // for (int j = 0; j < totalIndices; j++) {
            // System.out.print("\t" + indexPresents[wid][j]);
            // }
            // Rt.np();
            // }
            DATWindow[] windows = new DATWindow[param.windowConstraints.length];
            double[] costs = new double[param.windowConstraints.length];
            Rx root = null;
            if (debug != null)
                root = debug.createChild(method);
            for (int i = 0; i < param.windowConstraints.length; i++) {
                CPlexWrapper cplex = new CPlexWrapper();
                windows[i] = new DATWindow(param.costModel, cplex, i,
                        i == param.windowConstraints.length - 1,
                        param.windowConstraints[i]);
                IloLinearNumExpr expr = cplex.linearNumExpr();
                windows[i].addObjective(expr, 1);
                IloObjective obj = cplex.minimize(expr);
                cplex.add(obj);
                if (DAT.showFormulas)
                    Rt.p("Obj: " + expr.toString());
                windows[i].addConstriant(cplex, param.spaceConstraint,
                        param.maxIndexCreatedPerWindow);
                for (int k = 0; k < totalIndices; k++) {
                    cplex.addEq(windows[i].present[k], indexPresents[i][k] ? 1
                            : 0);
                    cplex.addEq(windows[i].create[k], 0);
                    cplex.addEq(windows[i].drop[k], 0);
                }
                if (!cplex.solve())
                    throw new Error();
                windows[i].getValues(cplex);
                costs[i] = windows[i].getCost(cplex);// cplex.getObjValue();
                if (i < param.windowConstraints.length - 1)
                    totalCost += costs[i] * param.alpha;
                else
                    totalCost += costs[i] * param.beta;
                // for (int j = 0; j < totalIndices; j++) {
                // indexPresent[j] = windows[i].indexPresent[j];
                // }
                output.ws[i].indexUsed = Arrays.copyOf(windows[i].indexPresent,
                        totalIndices);
                output.ws[i].cost = windows[i].getCost(cplex);
                double c2 = DATWindow.costWithIndex(param.costModel,
                        output.ws[i].indexUsed);
                if (Math.abs(output.ws[i].cost - c2) > 1)
                    throw new Error();
                // if (!windows[i].lastWindow
                // && Math.abs(output.ws[i].cost * alpha
                // - cplex.getObjValue()) > 1)
                // throw new Error();
                if (root != null) {
                    Rx window = root.createChild("window");
                    DAT.saveDebugInfo(window, param, cplex, windows[i]);
                }

            }

            output.totalCost = totalCost;
            if (root != null) {
                root.setAttribute("cost", output.totalCost);
            }
        } catch (IloException e) {
            e.printStackTrace();
        }
        return output;
    }
}
