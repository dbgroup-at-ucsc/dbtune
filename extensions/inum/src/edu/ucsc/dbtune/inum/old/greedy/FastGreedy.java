package edu.ucsc.dbtune.inum.old.greedy;

import Zql.ParseException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import edu.ucsc.dbtune.inum.old.Config;
import edu.ucsc.dbtune.inum.old.CostEstimator;
import edu.ucsc.dbtune.inum.old.autopilot.PostgresPlan;
import edu.ucsc.dbtune.inum.old.autopilot.autopilot;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.Plan;
import edu.ucsc.dbtune.inum.old.model.QueryDesc;
import edu.ucsc.dbtune.inum.old.model.WorkloadProcessor;
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
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Mar 12, 2008
 * Time: 11:35:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class FastGreedy {
    private float limit = (1.07f * 1024 * 1024 / 4);
    private WorkloadProcessor proc;
    private List<FlattenedINumCacheEntry> flattenedINum;
    private static final Double HORRIBLY_BIG_VALUE = 1000000000.0;

    private Map intToPlansMap;
    private boolean acceptedIndexes[];
    private float[] space;
    private Multimap<String, Integer> columnToPlanMap = HashMultimap.create();
    private long startingTime;
    private List<Map<String, Set<Integer>>> perQueryPerColumnIndexes;
    private List<Map<String, Float>> perQueryPerColumnMinCost;
    private static final float ALPHA = 0.0f; 
    private CostEstimator ce;
    private Map<String,Index>[] usedIndexes;

    public FastGreedy(String fileName, float limit) throws ParseException, IOException {
        ce = new CostEstimator(fileName);
        this.limit = limit;

        proc = ce.proc; 

        System.out.println(" Loaded all data ");

        acceptedIndexes = new boolean[proc.candidates.size()]; 
        usedIndexes = new Map[proc.query_descriptors.size()];
        for (int i = 0; i < usedIndexes.length; i++) {
            usedIndexes[i] = new HashMap();
        }
        space = new float[proc.candidates.size()];

        //for each index from the candidates list
        for (int i = 0; i < space.length; i++) {
            space[i] = ce.AP.getIndexSize((Index) proc.candidates.get(i));//get the index size. 
            //i added the index size
            proc.candidates.get(i).setTheSize((int)space[i]);
        }
    }
    // index advisor
  // it talks to inum tk determine configurations
    public GreedyResult findGreedy() throws IOException {
        this.startingTime = System.currentTimeMillis();
        if(ALPHA > 0)
            this.buildPerColumnIndices();
        this.flattenedINum = flattenInum(); 

        float spaceConsumed = 0;

        float[] currentOptimum = new float[proc.query_descriptors.size()];
        for (int i = 0; i < currentOptimum.length; i++) {
            currentOptimum[i] = ((QueryDesc) proc.query_descriptors.get(i)).emptyCost;//optimum cost for query
        }
        int idx = 0;

        System.out.println("Greedy: currentOptimum = " + Arrays.toString(currentOptimum));
        System.out.println("Greedy: Total Cost = " + sum(currentOptimum));

        //TODO: ce ma fac cu cluster index? Cred ca nu il bag
        if (false && Config.getDatabaseName().equals("tpch")) {
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

        GreedyResult res = new GreedyResult();
        res.queryDescs = proc.query_descriptors;
        res.usedIndexes =  Arrays.asList(this.usedIndexes);
        res.queryCosts = currentOptimum;
        //updateGreedyResultCost(res);

        return res;
    }
    
    private List flattenInum() {
        List flatList = new ArrayList();
        int queryId = 0, planId = 0;
        //C: for each QueryDescr element in the query_descriptors
        for (Iterator iterator = proc.query_descriptors.iterator(); iterator.hasNext();) {
            QueryDesc desc = (QueryDesc) iterator.next();

            System.out.println("proc.query_descriptors.size() = " + proc.query_descriptors.size());
            //C: get the interesting orders
            PhysicalConfiguration intOrderConfig = desc.interesting_orders;
            Set tableNames = intOrderConfig.getIndexedTableNames(); //the number of tables wich will have indexes

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
                    info.currentMinAccessCost = new Double(emptyPlan.getAccessCost(table));
                    if (ALPHA > 0) {
                        info.globalMinAccessCost = perQueryPerColumnMinCost.get(queryId).get(col).doubleValue();
                        info.globallyUsefulIndexes = perQueryPerColumnIndexes.get(queryId).get(col);
                    }

                    // find the "uninteresting" columns for this table.
                    Set all = desc.used.getFirstIndexForTable(table).getColumns();
                    Collection diff = CollectionUtils
                        .subtract(all, intOrderConfig.getFirstIndexForTable(table).getColumns());
                    for (Iterator iterator = diff.iterator(); iterator.hasNext();) {
                        String column = (String) iterator.next();

                        columnToPlanMap.put(column, flatList.size());
                    }
                } else {
                    columnToPlanMap.put(col, flatList.size());
                    info.currentMinAccessCost = HORRIBLY_BIG_VALUE;
                    if (ALPHA > 0) {
                        info.globalMinAccessCost = perQueryPerColumnMinCost.get(queryId).get(col).doubleValue();
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
        //ArrayList tableNames = new ArrayList(orderConfig.interesting_orders.getIndexedTableNames());
        ArrayList tableNames = new ArrayList(orderConfig.getIndexedTableNames());
        Collections.sort(tableNames);
        Iterator tableIterator = tableNames.iterator();

        String[] orderIds = orders.split("[\\[, \\]]");
        Map retMap = new HashMap(orderIds.length);
        //Iterator tableIterator = orderConfig.getIndexedTableNames().iterator();
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
            if (acceptedIndexes[i] == true) continue;//C: if it was already accepted then do nothing with it
            if (space[i] > spaceLeft) continue;//C: if its space is higher than the remaining space then do nothing with that index

            System.arraycopy(currentOptimum, 0, newOptimum, 0, newOptimum.length);

            Collection interestedPlans = (Collection) columnToPlanMap.get(index.getFirstColumn());

            for (Iterator iter = interestedPlans.iterator(); iter.hasNext();) {
                Integer id = (Integer) iter.next();
                FlattenedINumCacheEntry entry = flattenedINum.get(id);

                QueryDesc desc = (QueryDesc) proc.query_descriptors.get(entry.queryId);
                Float accessCost = 0.0f;
                if(entry.isNLP)
                	accessCost = (Float) desc.access_costs.get(index.getKey()+"_NLJ_");
                else
                	accessCost = (Float) desc.access_costs.get(index.getKey());
                
                if (accessCost == null || accessCost <= 0.0) {
                    continue;
                }

                Double min = entry.getCurrentMinAccessCostForTable(index.getTableName());
                if (min > accessCost) {
                    // I got something I can change.
                    Double cost = entry.getCost() - min + accessCost;
                    if (cost < newOptimum[entry.queryId] ) {
                        newOptimum[entry.queryId] = cost.floatValue() + accessCost ;
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

            Double globalMin = entry.getGlobalMinAccessFor(index.getTableName());
            if (globalMin < accessCost) {
                Double globalMinCost = entry.getGlobalMinCost() - globalMin + accessCost;
                potentialMin[entry.queryId] = (float) Math.min(globalMinCost, potentialMin[entry.queryId]);
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
            Float accessCost = 0.0f;
            if(entry.isNLP)
            	accessCost = (Float) desc.access_costs.get(index.getKey()+"_NLJ_");
            else
            	accessCost = (Float) desc.access_costs.get(index.getKey());
            
            if (accessCost == null || accessCost <= 0.0) {
                continue;
            }

            Double min = entry.getCurrentMinAccessCostForTable(index.getTableName());
            if (min > accessCost) {
                // I got something I can change.
                Double cost = entry.getCost() - min + accessCost;
                entry.updateCurrentMinAccessCostForTable(index.getTableName(), accessCost.doubleValue());
                // entry.minAccessCosts.put(index.getTableName(), accessCost);

                if (cost < currentOptimum[entry.queryId]) {
                    currentOptimum[entry.queryId] = cost.floatValue();
                    Map map = usedIndexes[entry.queryId];
                    map.put(index.getTableName(), index);
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

    private void updateGreedyResultCost(GreedyResult greedyResult)
    {
   	 	int size = greedyResult.queryCosts.length;
   	 	greedyResult.queryCosts = new float[size];
   	 
   	    /*Create Configuration*/
   	    PhysicalConfiguration config = new PhysicalConfiguration();
   	    /*Get list of suggested Indexes*/
   	    List<Map<String, Index>> suggestedIndexesList =  greedyResult.usedIndexes;
   	    for (int i = 0; i < suggestedIndexesList.size(); i++)
   	    {
   	    	Map<String, Index> temp = (Map<String, Index>) suggestedIndexesList.get(i);
   	    	Iterator<Entry<String, Index>> it = temp.entrySet().iterator();
   	    	while (it.hasNext()) 
   	    		config.addIndex((Index) ((Entry<String, Index>)it.next()).getValue());
   	    }
   	    /*Connect to database*/
   	    autopilot ap = new autopilot();
   	    ap.init_database();
   	    ap.implement_configuration(config);
   	    
   	    List<QueryDesc> queriesList = greedyResult.queryDescs;
   	    /*get Execution Plan per query for suggested indexes (with & without NLJ)*/
   	    for (int i = 0; i < queriesList.size(); i++)
   	    {
   	    	float min = 0.0f;
   	    	/*Get Query*/
   	    	QueryDesc queryDesc = (QueryDesc) queriesList.get(i);
   	    	/*Prepare without NLJ*/
   	        String query = ap.prepareQueryForConfiguration(queryDesc, config, true, false);
   	        PostgresPlan plan = (PostgresPlan) ap.optimizer_cost(query);
   	        min = plan.getTotalCost();
   	        ap.removeQueryPreparation(queryDesc);
   	    	
   	        /*Prepare with NLJ*/
   	        query = ap.prepareQueryForConfiguration(queryDesc, config, true, true);
   	        plan = (PostgresPlan) ap.optimizer_cost(query);
   	        if(min > plan.getTotalCost()){
   	        	min = plan.getTotalCost();
   	        }
   	        
   	        ap.removeQueryPreparation(queryDesc);
   	        /*Update query cost based on What-if components*/
   	     greedyResult.queryCosts[i] = min;
   	    }
   	    /*Close database connection*/
   	    ap.dispose();
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

        public Double getCost() {
            Double cost = new Float(plan.getInternalCost()).doubleValue();
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
            Double accessCost = desc.access_costs.get(((Index) proc.candidates.get(indexId)).getKey()).doubleValue();

            if (accessCost == null || accessCost <= 0) return;

            MinAccessCostInfo maci = minAccessCosts.get(tableName);
            maci.globallyUsefulIndexes.remove(indexId);

            if (accessCost <= maci.globalMinAccessCost) {
                Double currentMin = HORRIBLY_BIG_VALUE;
                for (Iterator<Integer> iterator = maci.globallyUsefulIndexes.iterator(); iterator.hasNext();) {
                    Integer id = iterator.next();

                    Double newMin = desc.access_costs.get(((Index) proc.candidates.get(indexId)).getKey()).doubleValue();
                    if (currentMin > newMin) {
                        currentMin = newMin;
                    }
                }
                maci.globalMinAccessCost = currentMin;
            }
        }

        public Double getGlobalMinAccessFor(String tableName) {
            return new Double(((MinAccessCostInfo) minAccessCosts.get(tableName)).globalMinAccessCost);
        }

        public Double getCurrentMinAccessCostForTable(String tableName) {
            return new Double(((MinAccessCostInfo) minAccessCosts.get(tableName)).currentMinAccessCost);
        }

        public void updateCurrentMinAccessCostForTable(String tableName, Double cost) {
            ((MinAccessCostInfo) minAccessCosts.get(tableName)).currentMinAccessCost = cost;
        }
    }

    class MinAccessCostInfo {
        Double currentMinAccessCost;
        Double globalMinAccessCost;
        Set<Integer> globallyUsefulIndexes;
    }

    public static void main(String[] args) throws IOException, ParseException {
        FastGreedy fg = new FastGreedy(args[0], 1.07f * 1024 * 1024 / 4);
        fg.findGreedy();
    }
}
