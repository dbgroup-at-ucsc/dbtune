package edu.ucsc.dbtune.tools.cmudb.greedy;

import Zql.ParseException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import edu.ucsc.dbtune.tools.cmudb.inum.CostEstimator;
import edu.ucsc.dbtune.tools.cmudb.model.Index;
import edu.ucsc.dbtune.tools.cmudb.model.PhysicalConfiguration;
import edu.ucsc.dbtune.tools.cmudb.model.Plan;
import edu.ucsc.dbtune.tools.cmudb.model.QueryDesc;
import edu.ucsc.dbtune.tools.cmudb.model.WorkloadProcessor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Mar 12, 2008
 * Time: 11:35:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class LookupStructure {
    private float limit = (1.07f * 1024 * 1024 / 4);
    private WorkloadProcessor proc;
    private List<FlattenedINumCacheEntry> flattenedINum;
    private static final float HORRIBLY_BIG_VALUE = 100000f;
    private Map intToPlansMap;
    private boolean acceptedIndexes[];
    private float[] space;
    private Multimap<String, Integer> columnToPlanMap = HashMultimap.create();
    private long startingTime;
    private List<Map<String, Set<Integer>>> perQueryPerColumnIndexes;
    private List<Map<String, Float>> perQueryPerColumnMinCost;
    private static final float ALPHA = 0.0f;
    private CostEstimator ce;

    public LookupStructure(String fileName) throws ParseException, IOException {
        ce = new CostEstimator(fileName);

        proc = ce.proc;

        System.out.println(" Loaded all data ");

        acceptedIndexes = new boolean[proc.candidates.size()];
        space = new float[proc.candidates.size()];

        for (int i = 0; i < space.length; i++) {
            space[i] = ce.AP.getIndexSize((Index) proc.candidates.get(i));
        }
    }

    private List flattenInum() {
        List flatList = new ArrayList();
        int queryId = 0, planId = 0;
        for (Iterator iterator = proc.query_descriptors.iterator(); iterator.hasNext();) {
            QueryDesc desc = (QueryDesc) iterator.next();

            System.out.println("proc.query_descriptors.size() = " + proc.query_descriptors.size());
            PhysicalConfiguration intOrderConfig = desc.interesting_orders;
            Set tableNames = intOrderConfig.getIndexedTableNames();

            HashMap planMap = desc.plans;
            Plan emptyPlan = (Plan) planMap.get(Collections.nCopies(tableNames.size(), 0).toString());
            desc.emptyCost = emptyPlan.getTotalCost();

            flattenPlans(flatList, queryId, intOrderConfig, planMap, emptyPlan, false);
            flattenPlans(flatList, queryId, intOrderConfig, desc.nljMap, emptyPlan, true);
            queryId++;
        }

        return flatList;
    }

    private void flattenPlans(List flatList, int queryId, PhysicalConfiguration intOrderConfig, HashMap planMap, Plan emptyPlan, boolean nlj) {
        for (Iterator planIter = planMap.keySet().iterator(); planIter.hasNext();) {
            String orders = (String) planIter.next();
            if (!orders.endsWith("]")) {
                // just look for indexes now.
                continue;
            }

            Plan plan = (Plan) planMap.get(orders);
            Map interMap = getInterestingOrderColumns(orders, intOrderConfig);
            QueryDesc desc = proc.query_descriptors.get(queryId);

            Map minCosts = new HashMap();
            for (Iterator iterator1 = interMap.keySet().iterator(); iterator1.hasNext();) {
                String table = (String) iterator1.next();
                String col = (String) interMap.get(table);
                MinAccessCostInfo info = new MinAccessCostInfo();

                if ("EMPTY".equals(col)) {
                    info.currentMinAccessCost = emptyPlan.getAccessCost(table);
                    if (ALPHA > 0) {
                        info.globalMinAccessCost = perQueryPerColumnMinCost.get(queryId).get(col);
                        info.globallyUsefulIndexes = perQueryPerColumnIndexes.get(queryId).get(col);
                    }

                    // find the "uninteresting" columns for this table.
                    Set all = desc.used.getFirstIndexForTable(table).getColumns();
                    Collection diff = CollectionUtils.subtract(all, intOrderConfig.getFirstIndexForTable(table).getColumns());
                    for (Iterator iterator = diff.iterator(); iterator.hasNext();) {
                        String column = (String) iterator.next();

                        columnToPlanMap.put(column, flatList.size());
                    }
                } else {
                    columnToPlanMap.put(col, flatList.size());
                    info.currentMinAccessCost = HORRIBLY_BIG_VALUE;
                    if (ALPHA > 0) {
                        info.globalMinAccessCost = perQueryPerColumnMinCost.get(queryId).get(col);
                        info.globallyUsefulIndexes = perQueryPerColumnIndexes.get(queryId).get(col);
                    }
                }

                minCosts.put(table, info);
            }

            flatList.add(new FlattenedINumCacheEntry(queryId, flatList.size(), interMap, plan, nlj, minCosts));
        }
    }

    private void buildPerColumnIndices() {
        perQueryPerColumnIndexes = new ArrayList();
        perQueryPerColumnMinCost = new ArrayList();

        for (Iterator iterator = proc.query_descriptors.iterator(); iterator.hasNext();) {
            QueryDesc desc = (QueryDesc) iterator.next();
            PhysicalConfiguration config = desc.interesting_orders;
            Iterator<Index> iterator1 = config.indexes();
            Map<String, Float> minPerColumn = new HashMap();
            Map<String, Set<Integer>> indexesPerColumn = new HashMap();

            Set<String> interestingColumns = new HashSet();
            while (iterator1.hasNext()) {
                Index index = iterator1.next();
                interestingColumns.addAll(index.getColumns());
            }

            for (int i = 0; i < proc.candidates.size(); i++) {
                Index index = (Index) proc.candidates.get(i);

                if(!desc.used.getIndexedTableNames().contains(index.getTableName())) {
                    // not really!
                    continue;
                }

                String col = index.getFirstColumn();
                if (!interestingColumns.contains(col)) {
                    col = "EMPTY";
                }

                Float accessCost = (Float) desc.access_costs.get(index.getKey());
                if (accessCost == null || accessCost <= 0.0) {
                    continue;
                }

                Float currentMin = minPerColumn.get(col);
                if (currentMin == null) {
                    minPerColumn.put(col, accessCost);
                } else {
                    minPerColumn.put(col, Math.min(currentMin, accessCost));
                }

                Set set = indexesPerColumn.get(col);
                if(set == null) {
                    indexesPerColumn.put(col, set = new HashSet());
                }

                set.add(i);
            }

            perQueryPerColumnIndexes.add(indexesPerColumn);
            perQueryPerColumnMinCost.add(minPerColumn);
        }
    }

    private Map getInterestingOrderColumns(String orders, PhysicalConfiguration orderConfig) {
        String[] orderIds = orders.split("[\\[, \\]]");
        Map retMap = new HashMap(orderIds.length);
        Iterator tableIterator = orderConfig.getIndexedTableNames().iterator();
        for (int i = 0; i < orderIds.length; i++) {
            String idStr = orderIds[i];
            if (idStr.length() == 0) continue;
            String tableName = (String) tableIterator.next();
            int id = Integer.parseInt(idStr);
            if (id == 0) {
                retMap.put(tableName, "EMPTY");
            } else {
                String col = (String) CollectionUtils.get(orderConfig.getFirstIndexForTable(tableName).getColumns(), id - 1);
                retMap.put(tableName, col);
            }
        }

        return retMap;
    }

    public void findGreedy() throws IOException {
        this.startingTime = System.currentTimeMillis();
        if(ALPHA > 0)
            this.buildPerColumnIndices();
        this.flattenedINum = flattenInum();

        float spaceConsumed = 0;

        float[] currentOptimum = new float[proc.query_descriptors.size()];
        for (int i = 0; i < currentOptimum.length; i++) {
            currentOptimum[i] = ((QueryDesc) proc.query_descriptors.get(i)).emptyCost;
        }
        int idx = 0;

        System.out.println("Greedy: currentOptimum = " + Arrays.toString(currentOptimum));
        System.out.println("Greedy: Total Cost = " + sum(currentOptimum));


        if (true && edu.ucsc.dbtune.tools.cmudb.Config.getDatabaseName().equals("tpch")) {
            // find the clustered indexes first.
            List clusteredIndexes = proc.getCandidateClusteredIndexes("lineitem");

            // change the space requirement for this set of candidates.
            float spaceBak[] = this.space;
            this.space = new float[clusteredIndexes.size()];

            for (int i = 0; i < space.length; i++) {
                space[i] = 1.0f;
            }

            System.out.println("Checking clustered indexes");
            idx = findWinningIndex(clusteredIndexes, currentOptimum, 1E10f);
            if (idx != -1)
                acceptWinningIndex((Index) clusteredIndexes.get(idx), currentOptimum);

            this.space = spaceBak;
            System.out.println("Clustered: " + (System.currentTimeMillis() - startingTime) + " : " + idx);
        }

        int iter = 0;
        System.out.println("Greedy: " + (System.currentTimeMillis() - startingTime) + " : " + 0);
        System.out.println("Space left: " + (limit - spaceConsumed));
        System.out.println("Greedy: currentOptimum = " + Arrays.toString(currentOptimum));
        System.out.println("Greedy: Total Cost = " + sum(currentOptimum));

        while (spaceConsumed < limit) {
            System.out.println("Running Greedy " + (++iter));
            idx = findWinningIndex(proc.candidates, currentOptimum, limit - spaceConsumed);
            if (idx == -1) break;
            Index index = (Index) proc.candidates.get(idx);
            acceptWinningIndex(index, currentOptimum);
            spaceConsumed += space[idx];
            acceptedIndexes[idx] = true;
            if(ALPHA > 0)
                elliminateLargeIndexes(proc.candidates, limit - spaceConsumed);

            System.out.println("Greedy: " + (System.currentTimeMillis() - startingTime) + " : " + index);
            System.out.println("Space left: " + (limit - spaceConsumed));
            System.out.println("Greedy: currentOptimum = " + Arrays.toString(currentOptimum));
            System.out.println("Greedy: Total Cost = " + sum(currentOptimum));
        }
    }

    private void elliminateLargeIndexes(List candidates, float spaceLeft) {
        for (int i = 0; i < candidates.size(); i++) {
            Index index = (Index) candidates.get(i);
            if (space[i] > spaceLeft) {
                // remove this index, also update the
                // global minimum possible in the plans.

                Collection interestedPlans = (Collection) columnToPlanMap.get(index.getFirstColumn());

                for (Iterator iter = interestedPlans.iterator(); iter.hasNext();) {
                    Integer id = (Integer) iter.next();
                    FlattenedINumCacheEntry entry = flattenedINum.get(id);

                    entry.removeFromGlobalMinAccessCosts(index.getTableName(), i);
                }
            }
        }
    }

    private int findWinningIndex(List candidates, float[] currentOptimum, float spaceLeft) {
        float currentTotalCost = sum(currentOptimum);
        Index currentOptimumIndex = null;
        int currentOptimumIndexId = -1;

        List candidateWinners = new ArrayList();
        List canddiateWinnerCosts = new ArrayList();

        float newOptimum[] = new float[currentOptimum.length];
        for (int i = 0; i < candidates.size(); i++) {
            Index index = (Index) candidates.get(i);
            if (acceptedIndexes[i] == true) continue;
            if (space[i] > spaceLeft) continue;

            System.arraycopy(currentOptimum, 0, newOptimum, 0, newOptimum.length);

            Collection interestedPlans = (Collection) columnToPlanMap.get(index.getFirstColumn());

            for (Iterator iter = interestedPlans.iterator(); iter.hasNext();) {
                Integer id = (Integer) iter.next();
                FlattenedINumCacheEntry entry = flattenedINum.get(id);

                QueryDesc desc = (QueryDesc) proc.query_descriptors.get(entry.queryId);
                Float accessCost = (Float) desc.access_costs.get(index.getKey());
                if (accessCost == null || accessCost <= 0.0) {
                    continue;
                }

                Float min = (Float) entry.getCurrentMinAccessCostForTable(index.getTableName());
                if (min > accessCost) {
                    // I got something I can change.
                    float cost = entry.getCost() - min + accessCost;
                    if (cost < newOptimum[entry.queryId]) {
                        // wo wo wee wa
                        newOptimum[entry.queryId] = cost;
                    }
                }
            }

            // check if we got a good candidate now.
            float newTotalCost = sum(newOptimum);
            if (newTotalCost < currentTotalCost) {
                candidateWinners.add(i);
                canddiateWinnerCosts.add(newTotalCost);
            }
        }

        float currentOptMetric = Float.MIN_VALUE;
        for (int i = 0; i < candidateWinners.size(); i++) {
            int idx = (Integer) candidateWinners.get(i);
            //float currentBenefit = (currentTotalCost - (Float) canddiateWinnerCosts.get(i))/space[i];
            float candidateCost = (Float) canddiateWinnerCosts.get(i);
            float currentBenefit = currentTotalCost - candidateCost;
            float potentialBenefit = ALPHA > 0 ?
                    ( candidateCost - findPotentialOfIndex((Index) candidates.get(idx), currentOptimum)) : 0;
            potentialBenefit *= (space[idx]/spaceLeft);

            assert potentialBenefit >= 0;
            assert currentBenefit >= 0;

            float newMetric = ((1 - ALPHA) * currentBenefit) + (ALPHA * potentialBenefit);

            if (newMetric > currentOptMetric) {
                currentOptimumIndexId = idx;
                currentOptMetric = newMetric;
            }
        }

        return currentOptimumIndexId;
    }

    private float findPotentialOfIndex(Index index, float[] currentOptimum) {
        float potentialMin[] = new float[currentOptimum.length];
        System.arraycopy(currentOptimum, 0, potentialMin, 0, currentOptimum.length);
        Collection interestedPlans = (Collection) columnToPlanMap.get(index.getFirstColumn());

        for (Iterator iter = interestedPlans.iterator(); iter.hasNext();) {
            Integer id = (Integer) iter.next();
            FlattenedINumCacheEntry entry = flattenedINum.get(id);

            QueryDesc desc = (QueryDesc) proc.query_descriptors.get(entry.queryId);
            Float accessCost = (Float) desc.access_costs.get(index.getKey());
            if (accessCost == null || accessCost <= 0.0) {
                continue;
            }

            Float globalMin = (Float) entry.getGlobalMinAccessFor(index.getTableName());
            if (globalMin < accessCost) {
                float globalMinCost = entry.getGlobalMinCost() - globalMin + accessCost;
                potentialMin[entry.queryId] = Math.min(globalMinCost, potentialMin[entry.queryId]);
            }
        }

        return sum(currentOptimum) - sum(potentialMin);
    }

    private void acceptWinningIndex(Index index, float[] currentOptimum) {
        // now we change
        Collection interestedPlans = columnToPlanMap.get(index.getFirstColumn());

        for (Iterator iter = interestedPlans.iterator(); iter.hasNext();) {
            Integer id = (Integer) iter.next();
            FlattenedINumCacheEntry entry = flattenedINum.get(id);

            QueryDesc desc = (QueryDesc) proc.query_descriptors.get(entry.queryId);
            Float accessCost = (Float) desc.access_costs.get(index.getKey());
            if (accessCost == null || accessCost <= 0.0) {
                continue;
            }

            Float min = (Float) entry.getCurrentMinAccessCostForTable(index.getTableName());
            if (min > accessCost) {
                // I got something I can change.
                float cost = entry.getCost() - min + accessCost;
                entry.updateCurrentMinAccessCostForTable(index.getTableName(), accessCost);
                // entry.minAccessCosts.put(index.getTableName(), accessCost);

                if (cost < currentOptimum[entry.queryId]) {
                    // wo wo wee wa
                    currentOptimum[entry.queryId] = cost;
                }
            }
        }
    }

    private float sum(float[] currentOptimum) {
        int sum = 0;
        for (int i = 0; i < currentOptimum.length; i++) {
            float v = currentOptimum[i];
            sum += v;
        }

        return sum;
    }

    class FlattenedINumCacheEntry {
        int queryId;
        int planId;
        boolean isNLP;
        Plan plan;
        Map orders;
        Map<String, MinAccessCostInfo> minAccessCosts;

        public FlattenedINumCacheEntry(int queryId, int planId, Map interestingOrders, Plan plan, boolean nlj, Map minCosts) {
            orders = interestingOrders;
            minAccessCosts = minCosts;
            this.queryId = queryId;
            this.planId = planId;
            this.plan = plan;
            this.isNLP = nlj;
        }

        public float getCost() {
            float cost = plan.getInternalCost();
            for (Iterator iterator = minAccessCosts.values().iterator(); iterator.hasNext();) {
                MinAccessCostInfo maci = (MinAccessCostInfo) iterator.next();
                cost += maci.currentMinAccessCost;
            }

            return cost;
        }

        public Float getGlobalMinCost() {
            float cost = plan.getInternalCost();
            for (Iterator iterator = minAccessCosts.values().iterator(); iterator.hasNext();) {
                MinAccessCostInfo maci = (MinAccessCostInfo) iterator.next();
                cost += maci.globalMinAccessCost;
            }

            return cost;
        }

        public void removeFromGlobalMinAccessCosts(String tableName, int indexId) {
            QueryDesc desc = (QueryDesc) proc.query_descriptors.get(queryId);
            Float accessCost = (Float) desc.access_costs.get(((Index) proc.candidates.get(indexId)).getKey());

            if (accessCost == null || accessCost <= 0) return;

            MinAccessCostInfo maci = minAccessCosts.get(tableName);
            maci.globallyUsefulIndexes.remove(indexId);

            if (accessCost <= maci.globalMinAccessCost) {
                float currentMin = HORRIBLY_BIG_VALUE;
                for (Iterator<Integer> iterator = maci.globallyUsefulIndexes.iterator(); iterator.hasNext();) {
                    Integer id = iterator.next();

                    Float newMin = (Float) desc.access_costs.get(((Index) proc.candidates.get(indexId)).getKey());
                    if (currentMin > newMin) {
                        currentMin = newMin;
                    }
                }

                maci.globalMinAccessCost = currentMin;
            }
        }

        public float getGlobalMinAccessFor(String tableName) {
            return ((MinAccessCostInfo) minAccessCosts.get(tableName)).globalMinAccessCost;
        }

        public float getCurrentMinAccessCostForTable(String tableName) {
            return ((MinAccessCostInfo) minAccessCosts.get(tableName)).currentMinAccessCost;
        }

        public void updateCurrentMinAccessCostForTable(String tableName, Float cost) {
            ((MinAccessCostInfo) minAccessCosts.get(tableName)).currentMinAccessCost = cost;
        }
    }

    class MinAccessCostInfo {
        float currentMinAccessCost;
        float globalMinAccessCost;
        Set<Integer> globallyUsefulIndexes;
    }

    public static void main(String[] args) throws IOException, ParseException {
        LookupStructure fg = new LookupStructure(args[0]);
        fg.findGreedy();
    }
}