package edu.ucsc.dbtune.inum.mathprog;

import Zql.ParseException;
import com.google.common.base.Joiner;
import edu.ucsc.dbtune.inum.Config;
import edu.ucsc.dbtune.inum.EnumerationGenerator;
import edu.ucsc.dbtune.inum.IndexAccessGenerator;
import edu.ucsc.dbtune.inum.autopilot.autopilot;
import edu.ucsc.dbtune.inum.commons.Initializers;
import edu.ucsc.dbtune.inum.model.Index;
import edu.ucsc.dbtune.inum.model.Plan;
import edu.ucsc.dbtune.inum.model.QueryDesc;
import edu.ucsc.dbtune.inum.model.WorkloadProcessor;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Feb 24, 2008
 * Time: 8:29:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProgGenerator {
    private boolean limitCE = false;
    private Set reallyInterestingTables = new HashSet(Arrays.asList("lineitem", "orders", "part", "customer"));
    //    private Set reallyInterestingTables = new HashSet();
    private WorkloadProcessor proc;
    private String workload;
    private Map idxMap;
    private CPlexBuffer buf;
    private int consCount = 0;
    private int idxId = 0;
    MultiMap map = new MultiValueMap();
    MultiMap clusteredMap = new MultiValueMap();
    private autopilot ap;
    private Set clustered;
    private float size;
    private int maxIndexSize;

    public ProgGenerator(String workload, float size, int maxIndexSize) {
      this(
          Initializers.initializeAutopilot(),
          initializeWorkloadProcessor(workload),
          workload,
          size,
          maxIndexSize
      );
    }

    public ProgGenerator(autopilot autopilot, String workload, float size, int maxIndexSize) {
      this(autopilot, initializeWorkloadProcessor(workload), workload, size, maxIndexSize);
    }

    public ProgGenerator(autopilot autopilot,
        WorkloadProcessor processor,
        String workload,
        float size,
        int maxIndexSize
    ) {
      this.ap           = autopilot;
      this.workload     = workload;
      this.size         = size;
      this.maxIndexSize = maxIndexSize;
      this.proc         = Checks.checkNotNull(processor);
    }

    private static WorkloadProcessor initializeWorkloadProcessor(String workload) {
      final WorkloadProcessor proc;
      try {
        proc = new WorkloadProcessor(Config.getWorkloadFile(workload));
        proc.generateCandidateIndexes();
        return proc;
      } catch (ParseException ignored) {
        return null;
      }
    }



    public void build(LogListener listener) throws IOException, ParseException {
    	  listener.onLogEvent(LogListener.COPHY, "Building optimization program...");

        //proc.generateCandidateMatViews();
        listener.onLogEvent(LogListener.COPHY, "" + proc.query_descriptors.size() + " queries parsed ...");
        listener.onLogEvent(LogListener.COPHY, "" + proc.candidates.size() + " candidate indexes generated");

        buf = new CPlexBuffer(workload);

        idxMap = new HashMap();
        IndexAccessGenerator.loadIndexAccessCosts(workload, proc.query_descriptors);
        ap.setIndexSizeMap(IndexAccessGenerator.loadIndexSizes(workload));
        EnumerationGenerator.loadConfigEnumerations(workload, proc.query_descriptors);
        int totalPlanSize = 0;
        for(int i=0; i < proc.query_descriptors.size(); i++) {
        	QueryDesc desc = proc.query_descriptors.get(i);
            totalPlanSize += desc.plans.size();
            totalPlanSize += desc.nljMap.size();
        }

        listener.onLogEvent(LogListener.COPHY, "" + totalPlanSize + " candidate indexes generated");

        int oId = 0;
        List linList = new ArrayList();
        Variables vars = new Variables();

        // remove the idnexes which are too big.
        for (Iterator<Index> iterator = proc.candidates.iterator(); iterator.hasNext();) {
            Index index = iterator.next();
            if(index.getColumns().size() > maxIndexSize) {
                iterator.remove();
            }
        }

        // proc.candidates.clear();

        clustered = new HashSet();

        if (Config.getDatabaseName().equals("tpch"))
            clustered = new HashSet(proc.getCandidateClusteredIndexes("lineitem"));

/*
        try {
            IndexAccessGenerator iag = new IndexAccessGenerator(ap, new ArrayList(clustered), proc.query_descriptors);
            iag.generateIndexAccessCosts();
            IndexAccessGenerator.saveIndexCosts(workload, proc.query_descriptors);
        } catch (ParseException e) {
            e.printStackTrace();//To change body of catch statement use File | Settings | File Templates.
        }
*/

        proc.candidates.addAll(clustered);

        // in the cadidates add the empty index.
        for (Iterator woIter = proc.universe.getIndexedTableNames().iterator(); woIter.hasNext();) {
            String table = (String) woIter.next();

            // go to this.
            Index emptyIndex = new Index(table, new LinkedHashSet());
            for (Iterator iterator = proc.query_descriptors.iterator(); iterator.hasNext();) {
                QueryDesc queryDesc = (QueryDesc) iterator.next();

                if (!queryDesc.used.getIndexedTableNames().contains(table)) continue;

                Plan emptyPlan = (Plan) queryDesc.plans.get(Collections.nCopies(queryDesc.interesting_orders.getIndexedTableNames().size(), 0).toString());
                for (Iterator iterator1 = queryDesc.plans.keySet().iterator(); iterator1.hasNext();) {
                    String s = (String) iterator1.next();
                    Plan plan = (Plan) queryDesc.plans.get(s);

                    float cost = emptyPlan.getAccessCost(table);

                    queryDesc.access_costs.put(emptyIndex.getKey(), cost);
                }
            }
            
            proc.candidates.add(emptyIndex);
        }

        vars.indexes = proc.candidates;
        int idx = 0;
        for (Iterator iterator = proc.candidates.iterator(); iterator.hasNext();) {
            Index index = (Index) iterator.next();
            idxMap.put(index.getKey(), idx);
            idx++;
        }

        float totalEmptyCost = 0;
        for (int i = 0; i < proc.query_descriptors.size(); i++) {
            QueryDesc desc = (QueryDesc) proc.query_descriptors.get(i);
            List oIdsForQuery = new ArrayList();
            List queryCosts = new ArrayList();

            List tables = new ArrayList(desc.interesting_orders.getIndexedTableNames());
            Collections.sort(tables);

            for (Iterator iterator = desc.plans.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Plan> entry = (Map.Entry<String, Plan>) iterator.next();
                String iorder = entry.getKey();
                List state = getOrdersFromKey(iorder);

                if(processPlan(oId++, desc, state, entry.getValue(), false)) {
                    linList.add("c"+(oId-1));
                    oIdsForQuery.add("o" + (oId-1));
                }
            }

            for (Iterator iterator = desc.nljMap.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Plan> entry = (Map.Entry<String, Plan>) iterator.next();
                String iorder = entry.getKey();
                List state = getOrdersFromKey(iorder);

                if(processPlan(oId++, desc, state, entry.getValue(), true)) {
                    linList.add("c"+(oId-1));
                    oIdsForQuery.add("o" + (oId-1));
                }
            }

            if(oIdsForQuery.size() != 0) {
                buf.getCons().append("plans").append(String.valueOf(i)).append(": ").append(
                    Joiner.on(" + ").join(
                    oIdsForQuery)).append(" = 1").println();
            }
        }

        if (!clustered.isEmpty()) {
            List clustedUsed = new ArrayList();
            for (Iterator iterator = clustered.iterator(); iterator.hasNext();) {

                Index index = (Index) iterator.next();

                Object id = idxMap.get(index.getKey());
                if (map.containsKey(id)) {
                    clustedUsed.add("y" + id);
                }

            }
            buf.getCons().println("clustered" + (consCount++) + ": " + " + " + Joiner.on(" + ").join( clustedUsed) + " = 1 ");
        }


        /*
        for (Iterator iter = map.keySet().iterator(); iter.hasNext();) {
            Integer key = (Integer) iter.next();
            Collection values = (Collection) map.get(key);
            for (Iterator iterator = values.iterator(); iterator.hasNext();) {
                String s = (String) iterator.next();
                //buf.getBin().println(s);
                buf.getCons().println("y" + key + " - " + s + " >= 0");
            }
        }
        */

        List sizeList = new ArrayList();
        for (int i = 0; i < proc.candidates.size(); i++) {
            Index index = (Index) proc.candidates.get(i);
            if (map.containsKey(i)) {
                String idxVar = "y" + i;
                if (!clustered.contains(index) && !index.getColumns().isEmpty()) {
                    sizeList.add("" + ap.getIndexSize(index) + idxVar);
                }
                
                buf.getBin().println("y" + i + " \\MODEL: " + index.toString());
                for (Iterator iterator = ((Collection) map.get(i)).iterator(); iterator.hasNext();) {
                    String var = (String) iterator.next();
                    buf.getCons().append(var).append(": ").append(idxVar).append(" - ").append(var).append(" >= 0").println();
                    buf.getBin().println(var);
                }
            }
        }

        buf.getCons().println("size: " + Joiner.on(" + ").join(sizeList) + " - s = 0");
        buf.getCons().println("cost: " + Joiner.on(" + ").join(linList) + " - c = 0");
        buf.getCons().println("sanityC: c > 0");
        buf.getCons().println("sizeC: s <= " + size);


        buf.getObj().println("c");

        buf.close();
        System.out.println("totalEmptyCost = " + totalEmptyCost);
        ap.dispose();

        listener.onLogEvent(LogListener.COPHY, "Built the optimization program");
    }

    private List getOrdersFromKey(String iorder) {
        iorder = iorder.substring(1, iorder.length() - 1);
        String[] iorders = iorder.split(", ");
        ArrayList retList = new ArrayList();
        for (int i = 0; i < iorders.length; i++) {
            String s = iorders[i];
            retList.add(Integer.parseInt(s));
        }

        return retList;
    }

    private boolean processPlan(int oId, QueryDesc desc, List state, Plan plan, boolean nlj) {
        // add plan to the list.
        List[] indexesPerTable = new List[state.size()];
        Set interestingColumns = new HashSet();
        ArrayList tableNames = new ArrayList(desc.interesting_orders.getIndexedTableNames());
        Collections.sort(tableNames);
        for (int i = 0; i < state.size(); i++) {
            int s = (Integer) state.get(i);
            if (s != 0) {
                interestingColumns.add(tableNames.get(s - 1));
            }
        }

        for (int j = 0; j < state.size(); j++) {
            Integer colIdx = (Integer) state.get(j);
            String tableName = (String) tableNames.get(j);
            if (colIdx > 0) {
                String colName = desc.interesting_orders.getFirstIndexForTable(tableName).getColumnByIndex(colIdx - 1);
                indexesPerTable[j] = filterIndexesForInterestingOrder(tableName, colName);
            } else {
                // TODO: pass the query queryString in here.
                indexesPerTable[j] = getIndexesWithNoInterestingOrder(tableName, interestingColumns);
            }
        }

        List consList = new ArrayList();
        float internalCost = plan.getInternalCost();
        String oIdName = "o" + (oId);

        for (int j = 0; j < indexesPerTable.length; j++) {
            List index = indexesPerTable[j];
            List iOrderList = new ArrayList();
            List idxList = new ArrayList();

            for (int k = 0; k < index.size(); k++) {
                Index index1 = (Index) index.get(k);

                Float cost = null;
                if (nlj) {
                    cost = (Float) desc.access_costs.get(index1.getKey() + "_NLJ_");
                }

                if (cost == null)
                    cost = (Float) desc.access_costs.get(index1.getKey());

                //if (cost != null && cost > 0.0 && (index1.getColumns().isEmpty() || (queryString.emptyCost - (cost + internalCost) > (0.2 * queryString.emptyCost)))) {
                if (cost != null && cost > 0.0) {
                    // add this as a variable.
                    int idxVarId = (Integer) idxMap.get(index1.getKey());

                    String varId = "y" + idxVarId + "_" + oId;
                    consList.add("" + cost + ' ' + varId);
                    idxList.add(varId);

                    iOrderList.add(varId);

                    map.put(idxVarId, varId);
                }
            }

            if (!iOrderList.isEmpty())
                buf.getCons().println("cons" + (consCount++) + ": " + Joiner.on(" + ").join(
                    iOrderList) + " - " + oIdName + " =  0 ");
        }

        if (!consList.isEmpty())
            buf.getCons().println("cons" + (consCount++) + ": - c" + oId + " + " + Joiner.on(" + ").join(
                consList) + " + " + internalCost + " " + oIdName + " = 0");
        
        return !consList.isEmpty();
    }

    private List getIndexesWithNoInterestingOrder(String tableName, Set columns) {
        List indexes = new ArrayList();

        LinkedHashSet allColumns = proc.universe.getFirstIndexForTable(tableName).getColumns();

        for (int i = 0; i < proc.candidates.size(); i++) {
            Index index = proc.candidates.get(i);

            String column = index.getFirstColumn();
            if (tableName.equals(index.getTableName())) {
                if ((column == null || !columns.contains(column) && allColumns.contains(column))) {
                    indexes.add(index);
                }
            }
        }

        return indexes;
    }

    private List filterIndexesForInterestingOrder(String tableName, String column) {
        List indexes = new ArrayList();

        for (int i = 0; i < proc.candidates.size(); i++) {
            Index index = (Index) proc.candidates.get(i);

            if (column.equals(index.getFirstColumn())) {
                indexes.add(index);
            }
        }

        return indexes;
    }

    public static void main(String[] args) throws IOException, ParseException {
        ProgGenerator gen = new ProgGenerator(args[0], Float.parseFloat(args[1]), 15);
        gen.build(new DefaultLogger());
    }
}