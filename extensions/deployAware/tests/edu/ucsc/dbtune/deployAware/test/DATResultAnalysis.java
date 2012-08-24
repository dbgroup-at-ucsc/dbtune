package edu.ucsc.dbtune.deployAware.test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import org.xml.sax.SAXException;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.bip.def.SeqInumPlan;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlot;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlotIndexCost;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;
import edu.ucsc.dbtune.util.Environment;

public class DATResultAnalysis {
    public static void analyze(File file) throws Exception {
        Rx rx = Rx.findRoot(Rt.readFile(file));
        Hashtable<Integer, SeqInumIndex> indexHash = new Hashtable<Integer, SeqInumIndex>();
        Hashtable<String, SeqInumIndex> indexHash2 = new Hashtable<String, SeqInumIndex>();
        Hashtable<Integer, SeqInumQuery> queryHash = new Hashtable<Integer, SeqInumQuery>();
        for (Rx r : rx.findChild("index").findChilds("index")) {
            SeqInumIndex index = new SeqInumIndex(r, null);
            if (indexHash2.put(index.name, index) != null)
                throw new Error("duplicate index");
            indexHash.put(index.id, index);
        }
        for (Rx r : rx.findChild("query").findChilds("query")) {
            SeqInumQuery q = new SeqInumQuery(indexHash2, r);
            queryHash.put(q.id, q);
        }
        Rx[] windows1 = rx.findChild("dat").findChilds("window");
        Rx[] windows2 = rx.findChild("greedyRatio").findChilds("window");
        double datCost = rx.findChild("dat").getDoubleAttribute("cost");
        double greedyCost = rx.findChild("greedyRatio").getDoubleAttribute(
                "cost");
        StringBuilder overall = new StringBuilder();
        for (int i = 0; i < windows1.length; i++) {
            StringBuilder datIndex = new StringBuilder();
            StringBuilder greedyIndex = new StringBuilder();
            Rt.np("/***********************/");
            Rt.np("WINDOW " + i);
            System.out.print("DAT present ");
            for (Rx a : windows1[i].findChild("present").findChilds("index")) {
                System.out.print(a.getText() + ",");
                datIndex.append(a.getText() + ",");
            }
            System.out.println();
            System.out.print("DAT created ");
            for (Rx a : windows1[i].findChild("created").findChilds("index")) {
                System.out.print(a.getText() + ",");
            }
            System.out.println();
            System.out.print("DAT dropped ");
            for (Rx a : windows1[i].findChild("dropped").findChilds("index")) {
                System.out.print(a.getText() + ",");
            }
            System.out.println();
            System.out.print("Greedy present ");
            for (Rx a : windows2[i].findChild("present").findChilds("index")) {
                System.out.print(a.getText() + ",");
                greedyIndex.append(a.getText() + ",");
            }
            System.out.println();
            System.out.print("Greedy created ");
            for (Rx a : windows2[i].findChild("created").findChilds("index")) {
                System.out.print(a.getText() + ",");
            }
            System.out.println();
            System.out.print("Greedy dropped ");
            for (Rx a : windows2[i].findChild("dropped").findChilds("index")) {
                System.out.print(a.getText() + ",");
            }
            System.out.println();
            Rx[] queries1 = windows1[i].findChilds("query");
            Rx[] queries2 = windows2[i].findChilds("query");
            for (int j = 0; j < queries1.length; j++) {
                SeqInumQuery q = queryHash.get(queries1[j]
                        .getIntAttribute("id"));
                Rx[] ps = new Rx[2];
                for (Rx p : queries1[j].findChilds("plan")) {
                    if (p.getBooleanAttribute("active")) {
                        ps[0] = p;
                        break;
                    }
                }
                for (Rx p : queries2[j].findChilds("plan")) {
                    if (p.getBooleanAttribute("active")) {
                        ps[1] = p;
                        break;
                    }
                }
                SeqInumPlan[] plans = { q.plans[ps[0].getIntAttribute("id")],
                        q.plans[ps[1].getIntAttribute("id")] };
                double[] costs = new double[2];
                String[] s5 = new String[2];
                for (int k = 0; k < 2; k++) {
                    StringBuilder slots = new StringBuilder();
                    double cost = plans[k].internalCost;
                    for (Rx s : ps[k].findChilds("slot")) {
                        int id = s.getIntAttribute("id");
                        SeqInumIndex index = null;
                        if ("FTS".equals(s.getText())) {
                        } else {
                            int indexId = Integer.parseInt(s.getText());
                            index = indexHash.get(indexId);
                        }
                        SeqInumSlot slot = plans[k].slots[id];
                        double slotCost = slot.getCost(index);
                        slots.append(String.format("\tslot%d cost=%,.0f %s\n",
                                slot.id, slotCost, index == null ? "FTS"
                                        : index.toString()));
                        cost += slotCost;
                        slots.append(String.format("\t\t%,.0f\tFTS \n",
                                slot.fullTableScanCost));
                        for (SeqInumSlotIndexCost c : slot.costs) {
                            slots.append(String.format("\t\t%,.0f\t%s\n",
                                    c.cost, c.index.toString()));
                        }
                    }
                    costs[k] = cost;
                    s5[k] = slots.toString();
                }
                Rt.np("---- query " + q.id);
                for (SeqInumPlan plan : q.plans) {
                    Rt.np("   PLAN " + plan.id + "/" + q.plans.length);
                    Rt.np(plan.plan);
                    for (SeqInumSlot slot : plan.slots) {
                        Rt.np("\tSLOT " + slot.id);
                        Rt.np("\t\t%,.0f\tFTS", slot.fullTableScanCost);
                        for (SeqInumSlotIndexCost c : slot.costs) {
                            Rt.np("\t\t%,.0f\t%s", c.cost, c.index.toString());
                        }
                    }
                }
                Rt
                        .np(String
                                .format(
                                        "DAT/GREEDY=%.2f\tDAT=%,.0f\tGREEDY=%,.0f\twin%d query%d",
                                        costs[0] / costs[1], costs[0],
                                        costs[1], i, j));
                overall.append(String.format(
                        "%.2f\t%,.0f\t%,.0f\twin%d query%d\n", costs[0]
                                / costs[1], costs[0], costs[1], i, j));
                for (int k = 0; k < 2; k++) {
                    Rt.np("%s win%d q%d plan%d internalCost=%,.0f index %s",
                            k == 0 ? "DAT" : "GREEDY", i, j, plans[k].id,
                            plans[k].internalCost, k == 0 ? datIndex
                                    : greedyIndex);
                    Rt.np(plans[k].plan);
                    Rt.np(s5[k]);
                }
            }
        }
        Rt.np("DAT/GREEDY\tDAT\tGREEDY\tWINDOW QUERY");
        Rt.np(overall);
        Rt.np("%,.0f\t%,.0f", datCost, greedyCost);
    }

    public static void comparePlans(File file) throws Exception {
        Environment en = Environment.getInstance();
        String dbName = "tpch10g";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        DatabaseSystem db = newDatabaseSystem(en);
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        Rx rx = Rx.findRoot(Rt.readFile(file));
        Hashtable<Integer, SeqInumIndex> indexHash = new Hashtable<Integer, SeqInumIndex>();
        Hashtable<String, SeqInumIndex> indexHash2 = new Hashtable<String, SeqInumIndex>();
        Hashtable<Integer, SeqInumQuery> queryHash = new Hashtable<Integer, SeqInumQuery>();
        Set<Index> indexes = new HashSet<Index>();
        for (Rx r : rx.findChild("index").findChilds("index")) {
            SeqInumIndex index = new SeqInumIndex(r, db);
            if (indexHash2.put(index.name, index) != null)
                throw new Error("duplicate index");
            indexHash.put(index.id, index);
            indexes.add(index.index);
        }
        for (Rx r : rx.findChild("query").findChilds("query")) {
            SeqInumQuery q = new SeqInumQuery(indexHash2, r);
            queryHash.put(q.id, q);
            if (q.id != 11)
                continue;
            Db2PlanExtractor extractor = new Db2PlanExtractor(q.sql,
                    db2optimizer, indexes);
            Rt.np("---- query " + q.id);
            for (SeqInumPlan plan : q.plans) {
                Rt.np("   PLAN " + plan.id + "/" + q.plans.length);
                Rt.np(plan.plan);
                for (SeqInumSlot slot : plan.slots) {
                    Rt.np("\tSLOT " + slot.id);
                    Rt.np("\t\t%,.0f\tFTS", slot.fullTableScanCost);
                    for (SeqInumSlotIndexCost c : slot.costs) {
                        Rt.np("\t\t%,.0f\t%s", c.cost, c.index.toString());
                    }
                }
            }
            int i = 0;
            for (SQLStatementPlan plan : extractor.plans) {
                Rt.np("DB2 " + i + "/" + extractor.plans.size());
                Rt.np(plan);
                i++;
            }
        }
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        File inputDir = new File("/home/wangrui/dbtune/debug");
        File outputDir = new File(inputDir, "txt");

        // comparePlans(new File(inputDir, "tpch10gXtpch-inumX1mada_1.xml"));
        outputDir.mkdir();
        for (File file : inputDir.listFiles()) {
            file=new File(inputDir,
            "testXonline-benchmark-100X1mada_1.xml");
            String name = file.getName();
            if (!name.endsWith(".xml"))
                continue;
            name = name.substring(0, name.lastIndexOf('.'));
            PrintStream ps = new PrintStream(new File(outputDir, name + ".txt"));
            System.setOut(ps);
            analyze(file);
            ps.close();
            break;
        }
    }
}
