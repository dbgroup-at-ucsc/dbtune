package edu.ucsc.dbtune.seq;

import java.sql.SQLException;

import edu.ucsc.dbtune.seq.def.*;
import edu.ucsc.dbtune.util.Rt;

public class SeqOptimal {
    public SeqStep[] steps;

    public static SeqStep[] getOptimalSteps(SeqIndex[] source,
            SeqIndex[] destination, SeqQuerySet[] sequence,
            SeqConfiguration[] allConfigurations) {
        int n = sequence.length;
        SeqStep[] steps = new SeqStep[n + 2];
        steps[0] = new SeqStep(null, source);
        steps[steps.length - 1] = new SeqStep(null, destination);
        for (int i = 0; i < n; i++) {
            steps[1 + i] = new SeqStep(sequence[i], allConfigurations);
        }
        return steps;
    }

    public SeqOptimal(SeqCost cost, SeqIndex[] source, SeqIndex[] destination,
            SeqQuerySet[] sequence, SeqStep[] steps) throws SQLException {
        int n = sequence.length;
        this.steps = steps;
        for (int iStep = 0; iStep < n + 1; iStep++) {
            SeqStep prev = steps[iStep];
            SeqStep cur = steps[iStep + 1];
            SeqQuerySet query = iStep < n ? sequence[iStep] : null;
            for (int i = 0; i < cur.configurations.length; i++) {
                SeqStepConf curConf = cur.configurations[i];
                double minCost = Double.MAX_VALUE;
                SeqStepConf minConf = null;
                double transitionCost = 0;
                double queryCost = 0;
                for (int j = 0; j < prev.configurations.length; j++) {
                    SeqStepConf prevConf = prev.configurations[j];
                    double c = prevConf.costUtilThisStep;
                    double tc = cost.getCost(prevConf.configuration,
                            curConf.configuration);
                    if (cost.maxTransitionCost > 0
                            && tc > cost.maxTransitionCost)
                        continue;
                    double stepCost = tc;
                    double qc = 0;
                    if (query != null)
                        qc = cost.getCost(query, curConf.configuration);
                    stepCost += qc;
                    if (cost.stepBoost == null)
                        c += stepCost;
                    else
                        c += stepCost * cost.stepBoost[iStep];
                    if (c < minCost) {
                        minCost = c;
                        minConf = prevConf;
                        transitionCost = tc;
                        queryCost = qc;
                    }
                }
                if (minConf == null) {
                    // throw new Error("minConf is null");
                    curConf.costUtilThisStep = Double.MAX_VALUE;
                    curConf.bestPreviousConfiguration = null;
                    curConf.transitionCost = Double.MAX_VALUE;
                    curConf.queryCost = Double.MAX_VALUE;
                } else {
                    curConf.costUtilThisStep = minCost;
                    curConf.bestPreviousConfiguration = minConf;
                    curConf.transitionCost = transitionCost;
                    curConf.queryCost = queryCost;
                }
            }
        }
    }

    public SeqStepConf[] getBestSteps() {
        SeqStepConf[] best = new SeqStepConf[steps.length];
        best[steps.length - 1] = steps[steps.length - 1].configurations[0];
        best[steps.length - 1].isBestPath = true;
        for (int i = steps.length - 2; i >= 0; i--) {
            best[i] = best[i + 1].bestPreviousConfiguration;
            best[i].isBestPath = true;
        }
        return best;
    }

    public static String formatSteps(SeqStep[] steps) {
        String[][] tb = new String[steps.length][];
        int y = 0;
        int height = 0;
        for (SeqStep step : steps) {
            String[] ss = new String[step.configurations.length + 1];
            tb[y++] = ss;
            int x = 0;
            ss[x++] = step.queries != null ? step.queries.toString() : "&nbsp;";
            for (SeqStepConf conf : step.configurations) {
                StringBuilder sb = new StringBuilder();
                if (conf.isBestPath)
                    sb.append("BEST");
                if (conf.bestPreviousConfiguration != null) {
                    sb.append("<font size=\"-1\">{"
                            + conf.bestPreviousConfiguration.configuration
                            + "} ");
                    sb.append(String.format("%.0f",
                            conf.bestPreviousConfiguration.costUtilThisStep));
                    sb.append("<br />");
                    sb.append("-> "
                            + String.format("%.0f", conf.transitionCost)
                            + "<br /></font>");
                }
                sb.append("<b>{" + conf.configuration + "}</b> ");
                if (step.queries != null) {
                    sb.append(String.format("%.0f", conf.queryCost));
                }
                sb.append("<br />");
                sb.append(String.format("<b>%.0f</b>", conf.costUtilThisStep));
                ss[x++] = sb.toString();
            }
            if (x > height)
                height = x;
        }
        StringBuilder sb = new StringBuilder();
        sb
                .append("<table class=\"rtable\" cellpadding=\"2\" cellspacing=\"0\">");
        for (int i = 0; i < height; i++) {
            sb.append("<tr>");
            for (int j = 0; j < tb.length; j++) {
                if (i < tb[j].length) {
                    if (tb[j][i] == null) {
                        sb.append("<td>&nbsp;");
                    } else {
                        String s = tb[j][i];
                        if (s.startsWith("BEST")) {
                            s = s.substring(4);
                            sb.append("<td style=\"background:#cccccc;\">" + s);
                        } else {
                            sb.append("<td>" + s);
                        }
                    }
                } else {
                    sb.append("<td>&nbsp;");
                }
                sb.append("</td>");
            }
            sb.append("</tr>");
        }
        sb.append("</table>");
        return sb.toString();
    }

    public static String formatBestPath(SeqStepConf[] confs) {
        StringBuilder sb = new StringBuilder();
        for (SeqStepConf c : confs) {
            if (sb.length() > 0)
                sb.append("-> ");
            sb.append("<b>{" + c.configuration + "}</b> ");
            if (c.step.queries != null)
                sb.append("<b>" + c.step.queries + "</b>");
            sb.append("(" + String.format("%.0f", c.costUtilThisStep) + ")");
        }
        return sb.toString();
    }

    public static String formatBestPathPlain(SeqStepConf[] confs) {
        StringBuilder sb = new StringBuilder();
        for (SeqStepConf c : confs) {
            if (sb.length() > 0)
                sb.append("-> ");
            sb.append("{" + c.configuration + "} ");
            if (c.step.queries != null)
                sb.append("" + c.step.queries.name + "");
            sb.append("("
                    + String.format("q=%,.0f t=%,.0f sum=%,.0f", c.queryCost,
                            c.transitionCost, c.costUtilThisStep) + ")\r\n");
        }
        return sb.toString();
    }
}
