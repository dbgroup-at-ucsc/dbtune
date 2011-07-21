package edu.ucsc.dbtune.inum.scratch;

import edu.ucsc.dbtune.inum.commons.Utils;
import edu.ucsc.dbtune.inum.model.MSPlan;
import edu.ucsc.dbtune.inum.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.model.QueryDesc;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;

/**
 * Created by IntelliJ IDEA.
 * User: dash
 * Date: May 11, 2009
 * Time: 1:06:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class AnalyzePlans {
    public static void main(String[] args) throws FileNotFoundException {
        if(args.length != 0)
            savePlans();
        else
            loadPlans();
    }

    private static void loadPlans() throws FileNotFoundException {
        List maps = (List) Utils.loadObject("workload\\ms\\ce.test.sql.gz");
        int k = 1;
        for(Object o: maps) {
            QueryDesc desc = (QueryDesc) o;
            if(k == 13) { k++; continue; }
            
            System.out.println("queryString.que = " + desc.queryString);
            PrintWriter writerXml = new PrintWriter(""+k+".xmls.out");
            PrintWriter writerCosts = new PrintWriter(""+k+".costs.out");

            MultiMap mmap = new MultiValueMap();
            Map costMap = new HashMap();
            PhysicalConfiguration config = desc.interesting_orders;
            List tables=new ArrayList(desc.getUsedTableNames());
            Collections.sort(tables);            
            String columns[][] = new String[tables.size()][];
            int i = 0;

            for (Iterator iterator = tables.iterator(); iterator.hasNext();) {
                String table = (String) iterator.next();
                LinkedHashSet set = config.getFirstIndexForTable(table).getColumns();
                columns[i++] = (String[]) set.toArray(new String[0]);
            }

            for(HashMap map : new HashMap[] {desc.plans, desc.nljMap}) {
                for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext();) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    MSPlan plan = (MSPlan) entry.getValue();

                    String xml = plan.getPlanXML();
                    mmap.put(xml, entry.getKey().toString());

                    double accessCost = 0;
                    for (Iterator iterator1 = tables.iterator(); iterator1.hasNext();) {
                        String table = (String) iterator1.next();
                        accessCost += plan.getAccessCost(table);
                    }

                    if((accessCost + plan.getInternalCost() - plan.getTotalCost())/plan.getTotalCost() > 0.01) {
                        System.out.println("accessCost = " + accessCost);
                        System.out.println("plan.getInternalCost() = " + plan.getInternalCost());
                    }

                    costMap.put(xml, plan.getInternalCost());
                }
            }

            i = 0;
            for (Iterator iterator = mmap.keySet().iterator(); iterator.hasNext();) {
                String xml = (String) iterator.next();
                i++;
                List orders = (List) mmap.get(xml);
                for (Iterator iterator1 = orders.iterator(); iterator1.hasNext();) {
                    String list = (String) iterator1.next();
                    int[] ids = parseIndexes(list);
                    StringBuffer intOrders = new StringBuffer();
                    writerCosts.print(i + "," + xml.contains("Nested")+",");
                    for (int j = 0; j < ids.length; j++) {
                        int id = ids[j];
                        intOrders.append((i!=0)?", ":"");
                        if(id != 0) {
                            intOrders.append(columns[j][id-1]);
                        } else {
                            intOrders.append(tables.get(j));
                        }
                    }
                    writerCosts.print(intOrders);
                    writerCosts.print(", " + costMap.get(xml));
                    writerCosts.println();
                    writerXml.println("[" + intOrders + "]->" + xml);
                }
            }
            k++;
            writerCosts.close();
            writerXml.close();
        }
    }

    private static int[] parseIndexes(String list) {
        String str = list.substring(1).substring(0,list.length()-2);
        String strs[] = str.split(", ");
        int ints[] = new int[strs.length];
        for (int i = 0; i < strs.length; i++) {
            String s = strs[i];
            ints[i] = Integer.parseInt(s);
        }
        return ints;
    }

    private static void savePlans() {
        List maps = (List) Utils.loadObject("workload\\ms\\ce.test.sql.gz");
        int i = 1;
        for (Iterator iterator = maps.iterator(); iterator.hasNext();) {
            QueryDesc desc = (QueryDesc) iterator.next();
            Utils.saveObject(""+i+".gz", desc);
            i++;
        }
    }
}