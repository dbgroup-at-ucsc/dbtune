package edu.ucsc.dbtune.inum.old.linprog;

//Extracts a set of configurations from a query object.

import com.ibm.almaden.Summer.util.BernouliSampler;
import com.ibm.almaden.Summer.util.Sampler;
import edu.ucsc.dbtune.inum.old.Config;
import edu.ucsc.dbtune.inum.old.CostEstimator;
import edu.ucsc.dbtune.inum.old.Enumerator;
import edu.ucsc.dbtune.util.Pair;
import edu.ucsc.dbtune.inum.old.model.CandidateGenerator;
import edu.ucsc.dbtune.inum.old.model.Configuration;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.Plan;
import edu.ucsc.dbtune.inum.old.model.QueryDesc;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.commons.collections.CollectionUtils;

//import com.ibm.almaden.Summer.util.Sampler;
//import com.ibm.almaden.Summer.util.ReservoirSampler;


public class ExtractConfigs {
    private static final String indexeFile = "pre-good.index";
    private List indexes = new ArrayList();
    private Set fields = new HashSet();

    public static final boolean FIELD_BASED_PRUNING = false;
    public static final boolean INDEX_BASED_PRUNNING = false;
    private static final boolean SIZE_BASED_PRUNNING = false;
    private static final int MAX_INDEX_SIZE = 6;
    private static final int MAX_EXTENSION = 3;

    public static final int MIN_SAMPLE_THRESHOLD = 10;

    public ArrayList<Index> filteredConfigs = new ArrayList();
    private static final float NUM = 10;
    public static final int MAX_INDICIES_PER_TABLE = 100;
    private static final float CONFIG_LOSS_OF_QUALITY = 5;
    private static final Logger _log = Logger.getLogger("ExtractConfigs");
    Random rand = new Random();

    //filter a set of single-index configs based on theirt cost.
    //return the maximum cost of those that survived
    public float filterCost(QueryDesc QD, ArrayList<Index> configList, float max, CostEstimator ce) {
        Float maxNonFiltered = Float.MIN_VALUE;
        for (ListIterator ci = configList.listIterator(); ci.hasNext();) {
            Index idx = (Index) ci.next();

            float aFloat = ce.getAccessCostForQuery(idx, QD);
            //System.out.print ("filter: " + key + " " + aFloat + " " + max);

            if (aFloat <= 0.0) {
                //System.out.println("filter: error: cannot compute index access cost for " + idx);
                ci.remove();
            }
            else {
                if (aFloat > max) {
                    //System.out.println(" eliminate");
                    ci.remove();//do the actual filtering
                } else {
/*                    if (max - aFloat < (0.0 * max)) {//0.4 * max) {//remove values that are close to the total query cost
                        //System.out.println(" eliminate");
                        ci.remove();
                    } else */
                    if (aFloat > maxNonFiltered)
                        maxNonFiltered = aFloat;
                }
            }
        }//next configuration

        if(configList.size() > 25) {
            int oldsize = configList.size();
            double fraction = 25.0/configList.size();
            for (Iterator<Index> indexIterator = configList.iterator(); indexIterator.hasNext();) {
                Index index = indexIterator.next();
                if(rand.nextDouble() > fraction) {
                    indexIterator.remove();
                }
            }
            _log.info("filtering indexs from " + configList.size() + ", to " + configList.size());
        }

        return maxNonFiltered;
    }

    private boolean hasPrefix(LinkedHashSet columns, int maxExt) {
        for (int i = 0; i < indexes.size(); i++) {
            LinkedHashSet index = (LinkedHashSet) indexes.get(i);
            if (index.size() > columns.size()) continue;

            Iterator iter = columns.iterator();
            Iterator iterator = index.iterator();
            if (iter.next().equals(iterator.next())) {
                if (columns.containsAll(index)) {
                    if (columns.size() - index.size() <= MAX_EXTENSION) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    //filter all the indexes for a query before doing the combinations.
    public void filter(QueryDesc QD, List[] matchingConfigs, ArrayList inputOrders, CostEstimator ce) {
        for (int i = 0; i < matchingConfigs.length; i++) {

            //if (((Integer) inputOrders.get(i)).intValue()>0)
            //continue;

            ArrayList configsPerOrder = (ArrayList) matchingConfigs[i];
            //System.out.println("filter: original:(" + i + ") " + configsPerOrder.size());
            float max = filterCost(QD, configsPerOrder, QD.emptyCost, ce);
            //System.out.println("filter: after 1st pass:(" + i + ") " + configsPerOrder.size());

            //further filtering
            if (configsPerOrder.size() > 0) {
                if (max < 10.0)//this collection of indexes does not help much.
                //for now just keep one so that the code does not break.
                //not going to lose more than 10 points of benefit
                //the reason is that scanning cost is probably around 10 units as well
                {
                    Index idx = (Index) configsPerOrder.listIterator().next();
                    configsPerOrder.clear();
                    if (idx != null)
                        configsPerOrder.add(idx);
                }
            }
        }
    }

    private void loadIndexes() {
        if (new File(indexeFile).exists()) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(indexeFile));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    String[] columns = line.split(", ");
                    indexes.add(new LinkedHashSet(Arrays.asList(columns)));
                    fields.add(columns[0]);
                }
            } catch (IOException e) {
            }
        }
    }

    //singleConfigList is a list of single-index configurations.
    //configList is a list of configurations.
    public ArrayList product(ArrayList configList, ArrayList singleConfigList) {

        System.out.println("product: " + configList + " " + singleConfigList.size());

        ArrayList result = new ArrayList();
        //for every singleconfig..
        for (ListIterator li = singleConfigList.listIterator(); li.hasNext();) {

            Configuration C = (Configuration) li.next();
            //get table and columnset for the single-index config selected from the singleConfigList
            String tableName = (String) C.map.keySet().iterator().next();
            LinkedHashSet columnSet = (LinkedHashSet) C.getSingleTableSet(tableName);

            //add it to all the targets in there ...

            if (configList.size() == 0) {//if the list is empty

                Configuration target = C.copy();
                result.add(target);
                continue;
            }
            for (ListIterator ci = configList.listIterator(); ci.hasNext();) {

                //Configuration target = ((Configuration) ci.next()).copy();
                Configuration target = (Configuration) ci.next();
                target.addColumnSet(tableName, columnSet);
                result.add(target);
            }
        }
        return result;
    }


    //This is a large configuration, containing one entry per table
    //with all our candidate indexes in this pool
    public PhysicalConfiguration candidatePool;

    public ExtractConfigs(ArrayList configs) {
        loadIndexes();
        int i = 0, pruned = 0;
        //Start from a list of single-index configurations.
        candidatePool = new PhysicalConfiguration();
        for (ListIterator li = configs.listIterator(); li.hasNext();) {
            Index C = (Index) li.next();
            LinkedHashSet columns = C.getColumns();

            if (FIELD_BASED_PRUNING && !fields.contains(columns.iterator().next())) {
                pruned++;
                continue;
            }

            if (INDEX_BASED_PRUNNING && !hasPrefix(columns, MAX_EXTENSION)) {
                pruned++;
                continue;
            }

            if (SIZE_BASED_PRUNNING && columns.size() > MAX_INDEX_SIZE) {
                pruned++;
                continue;
            }

            i++;
            candidatePool.addIndex(C);
            filteredConfigs.add(C);
        }

        System.out.println("The candidate pool size: " + i + ", pruned = " + pruned);
    }

    public List<Index> findMatch(String tableName, String inputOrder, PhysicalConfiguration configPool) {
        //search configuration configpool for indexes starting with inputOrder.
        //Method: Obtain all the indexes for tableName and filter all the indexes matching the
        //inputOrder ...
        ArrayList result = new ArrayList();
        Set<Index> set = configPool.getIndexesForTable(tableName);

        if (set == null || set.isEmpty())
            return new ArrayList();

        //System.out.println("findMatch: columnsets: " + columnSets);
        //iterate over the columnsets...
        for (Index idx : set) {
            //some pattern matching here ...
            if (idx.getFirstColumn().equals(inputOrder)) {
                //System.out.println("findMatch: match " + inputOrder + " " + columns);
                result.add(idx);
            }
        }
        return result;
    }

    public List<Index> findNonInteresting(QueryDesc QD, String tableName) {
        //return only the indexes that are noninteresting
        ArrayList result = new ArrayList();
        //System.out.println("findMatch: tableName: " + tableName);
        Set<Index> indexesOnTable = candidatePool.getIndexesForTable(tableName);
        if (indexesOnTable == null || indexesOnTable.isEmpty()) {
            //dont consider any configs from that table ...
            return result;
        }

        LinkedHashSet interestingOrderSet = QD.interesting_orders.getFirstIndexForTable(tableName).getColumns();
        for (Index idx : indexesOnTable) {
            //for every interesting order in the query ...
            if (!interestingOrderSet.contains(idx.getFirstColumn())) {
                result.add(idx);
            }
        }
        return result;
    }

    public List extract(QueryDesc QD, ArrayList inputOrders, CostEstimator CE) {
        ArrayList<String> tableNames = new ArrayList(QD.interesting_orders.getIndexedTableNames());
        List<Index>[] matchingConfigs = new List[tableNames.size()];
        int qID = CE.proc.query_descriptors.indexOf(QD) + 1;

        if (inputOrders == null) {
            for (int i = 0; i < tableNames.size(); i++) {
                Set<Index> set = candidatePool.getIndexesForTable(tableNames.get(i));
                matchingConfigs[i] = new ArrayList(set);
            }
        } else {
            //Array of all the interesting orders.
            //each entry contains all the interesting orders per table.
            //for each table.
            int i = -1;
            for (Iterator iter = QD.interesting_orders.indexes(); iter.hasNext();) {
                Index idx = (Index) iter.next();
                i++;

                String interestingTableName = idx.getTableName();
                if (ConfigCosts.limitCE && !CostEstimator.reallyInterestingTables.contains(interestingTableName)) {
                    matchingConfigs[i] = new ArrayList();
                    continue;
                }
                System.out.println("interesting table name: " + interestingTableName);

                //retrieve the interesting order
                int inputOrderIndex = ((Integer) inputOrders.get(i)).intValue() - 1;

                //if this is a non-interesting order just get all the indexes in there ...
                if (inputOrderIndex < 0) {
                    matchingConfigs[i] = new ArrayList(findNonInteresting(QD, interestingTableName));
                    //System.out.println("extract: match: " + matchingConfigs[i]);
                    /*
                matchingConfigs[i] = new ArrayList();
                matchingConfigs[i].add(new Configuration());
                */
                    continue;
                }

                String inputInterestingOrder = (String) CollectionUtils.get(idx.getColumns(), inputOrderIndex);
                //System.out.println("interesting " + " " + inputOrderIndex + " " + inputInterestingOrder + " " + interestingTablesArray[i]);
                //find all the matching configs and store them in there.
                matchingConfigs[i] = findMatch(interestingTableName, inputInterestingOrder, candidatePool);
                //System.out.println("extract: match: " + matchingConfigs[i]);
            }
        }

        //smarts to remove unnecessary configurations..
        filter(QD, matchingConfigs, inputOrders, CE);

        //sampleMatchingConfigs(matchingConfigs, QD, CE);

        // float minCost = getMinCost(matchingConfigs, CE, QD, nljpcs, pcs);
        float maxCost = QD.emptyCost;

        return processMatchingConfigsExhaustive(matchingConfigs, inputOrders, CE, QD, maxCost);
    }

    private List processMatchingConfigsExhaustive(List[] matchingConfigs, List inputOrders, CostEstimator CE, QueryDesc QD, float maxCost) {
        int qID = CE.proc.query_descriptors.indexOf(QD);
        System.out.print("extract: matchingConfigs ");
        ArrayList<Integer> matchingVector = new ArrayList();
        int totalSize = 1;
        for (int i = 0; i < matchingConfigs.length; i++) {
            matchingVector.add(new Integer(matchingConfigs[i].size()));
            totalSize *= (matchingVector.get(matchingVector.size()-1)+1);
        }
        System.out.println();
        System.out.println("extract: matchingVector: " + matchingVector);

        /*
                PlanCostSummary pcs = new PlanCostSummary(QD, inputOrders, false);
                PlanCostSummary nljpcs = new PlanCostSummary(QD, inputOrders, true);

        */
        Enumerator E = new Enumerator(matchingVector);
        int extractedConfigCount = 0;
        ArrayList state = E.next();
        int c = 0;
        Sampler sampler = new BernouliSampler(100000d/(totalSize));
        List result = new ArrayList();

        Map minSizedConfigs = new HashMap();

        nextstate:
        while (state != null) {
            ConfigPair pair = new ConfigPair(null,QD.emptyCost*2);
            c++;
            if(Config.sampleConfigurations()) {
                ConfigPair pair1 = (ConfigPair) sampler.addItem(pair);
                if(pair != pair1) {
                    state = E.next();
                    continue;
                }
            }


            //System.out.println("extract: retrieve elements " + state + " from " + matchingVector);
            PhysicalConfiguration C = new PhysicalConfiguration();
            pair.config = C;
            for (int i = 0; i < state.size(); i++) {
                ArrayList configsToChooseFrom = (ArrayList) matchingConfigs[i];
                int index = ((Integer) state.get(i)).intValue();
                if (inputOrders != null) {
                    if (index == 0) {
                        if (!inputOrders.get(i).equals(0)) {
                            state = E.next();
                            continue nextstate;
                        }
                    } else {
                        C.addIndex((Index) configsToChooseFrom.get(index - 1));
                    }
                } else {
                    if (index != 0) {
                        C.addIndex((Index) configsToChooseFrom.get(index - 1));
                    }
                }
            }

            //pick a config from the next table in the state variable

            //            float costUB = CE.EstimatedCost(QD,C, true);
            // float costUB = CE.EstimatedCostUB(QD,C);
            //System.out.println("before LB");
            //float costLB = CE.EstimatedCostLB(QD,C);
            //System.out.println("after LB");

            float cost = 0;
            try {
                //cost = Math.min(pcs.getCost(CE, C, QD), nljpcs.getCost(CE, C, QD));
                cost = CE.getCost(QD, C);
                /*
                                if(Math.abs(cost1 - cost) > 0.05 * cost1 ) {
                                    CE.EstimatedCost(QD, C, CostEstimator.limitCE);
                                    pcs.getCost(CE, C, QD);
                                    nljpcs.getCost(CE, C, QD);
                                    System.out.println("C = " + C + ", cost1 = " + cost1 + ", cost  = " +cost);
                                }
                */
/*                Plan optimizePlan = CE.getOptimizerPlan(QD, C);

                if(Math.abs(optimizePlan.getTotalCost()-cost)/optimizePlan.getTotalCost() > 0.01) {
                    _log.warning("processMatchingConfigsExhaustive: " + C);
                    _log.warning("processMatchingConfigsExhaustive: " +
                            "optimzerCost = " + optimizePlan.getTotalCost() +", INUM Cost = " + cost +
                            ", plan = " + optimizePlan);
                }
                _log.info("COSTS: " + qID + "," + optimizePlan.getTotalCost() + "," + cost);
*/                
            } catch (Exception e) {
                e.printStackTrace();
                cost = 100;
            }

            //System.out.println("costs:"+qID+"\t"+inputOrders+"\t"+CE.AP.getConfigSize(C)+"\t"+cost);
            pair.cost = cost;
            // System.out.println("configCosts bounds: " + qID + " value " + cost + " UB " + costUB + " empty " + QD.emptyCost);
            //
            //              result.add(C);

            /*
            float ocost = CE.getOptimizerCosts(Arrays.asList(QD), C)[0];
            System.out.println("cost = " + cost + ", ocost = " + ocost);
            float rat = ocost / cost;
            if(cost > 5 && Math.abs(1-rat) > 0.5) {
                System.out.println("bad query = " + qID + ", inputOrders = " + inputOrders + ", config = " + C);
            }
            */
            //filter!!
            //System.out.println("SAMPLE: "+qID+"|"+inputOrders+"|"+cost);
            float diff = QD.emptyCost - cost;
            if (diff > (0.0 * QD.emptyCost)) {

                result.add(new ConfigPair(C, cost));

                extractedConfigCount++;
            } else {

            }

            state = E.next();//go to the next iteresting order combination
        }
        System.out.println("extract: extracted configs " + extractedConfigCount + "/" + c + " = " + totalSize);
        //return sampler.extract();
        //return ConfigCosts.SAMPLE_CONFIGURATIONS ? new ArrayList(minSizedConfigs.values()) : result;
        List list = sampler.extract();
        for (Iterator iterator = list.iterator(); iterator.hasNext();) {
            ConfigPair pair = (ConfigPair) iterator.next();
            if(pair.config ==null)
                iterator.remove();
        }
        //return Config.sampleConfigurations() ? list : result;
        return result;
    }

    private void sampleMatchingConfigs(List<Index>[] matchinConfigs, QueryDesc QD, CostEstimator CE) {
        List tableNames = new ArrayList(QD.interesting_orders.getIndexedTableNames());
        int total = 1;
        for (int i = 0; i < matchinConfigs.length; i++) {
            List matchinConfig = matchinConfigs[i];
            total *= (matchinConfig.size() + 1);
        }

        if (total < MAX_INDICIES_PER_TABLE * matchinConfigs.length) {
            return;
        }

        // ok. now filter out the matching configs according to the access costs.
        for (int i = 0; i < matchinConfigs.length; i++) {
            List<Index> configList = matchinConfigs[i];
            // get all the index costs from the Ce.

            if (configList.size() < MAX_INDICIES_PER_TABLE) {
                continue;
            }

            Pair<Index,Float> costList[] = new Pair[configList.size()];
            for (int j = 0; j < configList.size(); j++) {
                Index config = (Index) configList.get(j);
                Pair<Index,Float> cp = Pair.of(config, CE.getAccessCostForQuery(config, QD));
                costList[j] = cp;
            }

            Arrays.sort(costList, new Comparator<Pair<Index,Float>>() {
                public int compare(Pair<Index, Float> o1, Pair<Index, Float> o2) {
                    return Float.compare(o1.getRight(), o2.getRight());
                }
            });

            ArrayList filteredConfigs = new ArrayList();

            for (int j = 0; j < costList.length;) {
                Pair<Index,Float> configPair = costList[j];
                filteredConfigs.add(configPair.getLeft());
                j += (costList.length / MAX_INDICIES_PER_TABLE);
            }

            System.out.println(tableNames.get(i) + "'s configs filtered from " + configList.size() + " to " + filteredConfigs.size());

            matchinConfigs[i] = filteredConfigs;
        }
    }

    public ArrayList exhaustiveExtract(QueryDesc QD, CostEstimator CE, LinearModel LM, CPlexBuffer cbuf) {
        int qId = CE.proc.query_descriptors.indexOf(QD);

        //form and extract all possible configurations for query QD
        ArrayList result = new ArrayList();

        //Build a vector, with one entry per referenced table.
        //The entry contains the number of different interesting orders on that table.
        //LinkedHashSet[] tableSets = QD.interesting_orders.getTableSets();
        List tableNames = new ArrayList(QD.interesting_orders.getIndexedTableNames());
        ArrayList input = new ArrayList();
        for (int i = 0; i < tableNames.size(); i++) {
            if (ConfigCosts.limitCE && !ConfigCosts.reallyInterestingTables.contains(tableNames.get(i))) {
                input.add(0);
            } else
                input.add(new Integer(QD.interesting_orders.getFirstIndexForTable((String) tableNames.get(i)).getColumns().size()));
        }

        Enumerator E = new Enumerator(input);
        ArrayList state = E.next();

        Plan plan = (Plan) QD.plans.get(state.toString());
        QD.emptyCost = plan.getTotalCost();
        //for each possible interesting order combination
        while (state != null) {
            System.out.println("" + qId + " -- ***** exhaustiveExtract: " + state);
            //extract all the indexes in the original candidate pool that match this interesting order combination.

            List resultList = extract(QD, state, CE);

            result.addAll(resultList);

            if (result.size() > 50000) {
                // write out the values to disk.
                writeResultToOutput(qId, QD, LM, cbuf, result);
                result.clear();
            }

            System.out.println("Extract: total configs for this int. order: " + resultList.size());

            //result.add (extract(QD, state));

            state = E.next();
            //break; //only first set
        }

        if (result.size() > 0)
            writeResultToOutput(qId, QD, LM, cbuf, result);
        result.clear();
        /*
                result = extract(QD, null, CE);
                writeResultToOutput(qId, QD, LM, cbuf, result);
        */

        return result;
    }

    private void writeResultToOutput(int qId, QueryDesc QD, LinearModel lm, CPlexBuffer cbuf, ArrayList result) {
        System.out.println("Writing " + result.size() + ", rows to the model");
        lm.addQueryConfigs(qId, QD, result, Float.MIN_VALUE, cbuf);
    }


    //Testing the extraction functionality...
    public static void main(String[] args) {
        try {
            //Parse a query workload.
            String filename = args[0];

            //Load sample configs from file into the module.
            ExtractConfigs EC = new ExtractConfigs(new ArrayList(Configuration.loadFromFile(args[1])));
            File file = new File(filename);
            CandidateGenerator CG = new CandidateGenerator(file);
            CG.getInterestingOrders();


            for (ListIterator qi = CG.query_descriptors.listIterator(); qi.hasNext();)
                //EC.exhaustiveExtract((QueryDesc) qi.next());


                System.out.println("V");
        }
        catch (Exception E) {
            System.out.println("main: error: " + E.getMessage());
            E.printStackTrace();
        }
    }
}
