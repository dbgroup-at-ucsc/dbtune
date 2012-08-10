package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Hashtable;
import java.util.Vector;

import org.xml.sax.SAXException;

import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.bip.def.SeqInumPlan;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlot;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlotIndexCost;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;

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
        Rx[] windows1 = rx.findChild("mkp").findChilds("window");
        Rx[] windows2 = rx.findChild("greedyRatio").findChilds("window");
        StringBuilder overall = new StringBuilder();
        for (int i = 0; i < windows1.length; i++) {
            StringBuilder datIndex = new StringBuilder();
            StringBuilder greedyIndex = new StringBuilder();

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
                Rt.np("---- query " + q.id);
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
                String s = String.format("%.2f\t%,.0f\t%,.0f\twin%d query%d\n",
                        costs[0] / costs[1], costs[0], costs[1], i, j);
                Rt
                        .np(String
                                .format(
                                        "DAT/GREEDY=%.2f\tDAT=%,.0f\tGREEDY=%,.0f\twin%d query%d",
                                        costs[0] / costs[1], costs[0],
                                        costs[1], i, j));
                overall.append(s);
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
    }

    public static void main(String[] args) throws Exception {
        for (File file : new File("/home/wangrui/dbtune/debug").listFiles()) {
            String name = file.getName();
            if (!name.endsWith(".xml"))
                continue;
            name = name.substring(0, name.lastIndexOf('.'));
            PrintStream ps = new PrintStream(new File(
                    "/home/wangrui/dbtune/debug/txt/" + name + ".txt"));
            System.setOut(ps);
            analyze(file);
            ps.close();
        }
    }
}
