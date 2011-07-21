package edu.ucsc.dbtune.inum;

import Zql.ParseException;
import edu.ucsc.dbtune.inum.autopilot.autopilot;
import edu.ucsc.dbtune.inum.commons.Initializers;
import edu.ucsc.dbtune.inum.model.Index;
import edu.ucsc.dbtune.inum.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.model.Plan;
import edu.ucsc.dbtune.inum.model.QueryDesc;
import edu.ucsc.dbtune.inum.model.WorkloadProcessor;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Feb 23, 2008
 * Time: 11:20:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class CostEstimator {
    public WorkloadProcessor proc;
    public static Set reallyInterestingTables = new HashSet();
    public autopilot AP;
    private Random rand = new Random(1234);
    private static final Logger _log = Logger.getLogger(CostEstimator.class.getName());

    /**
     * construct a cost estimator object with already configured autopilot and workload processor
     * objects for the given workload file.
     * @param autopilot
     *    an already configured with database initiated autopilot object.
     * @param processor
     *    an already configured workload process for workloadfile.
     * @param workloadFile
     *    a persisted workload file.
     * @throws IOException
     *    unable to find workload file.
     */
    public CostEstimator(autopilot autopilot, WorkloadProcessor processor, String workloadFile)
        throws IOException {
      this.AP   = autopilot;

      if (new File(InumUtils.getIndexAccessCostFile(workloadFile)).exists()) {
        PostgresIndexAccessGenerator.loadIndexAccessCosts(workloadFile, processor.query_descriptors);
      }

      IndexAccessGenerator.loadIndexAccessCosts(workloadFile, processor.query_descriptors);
      AP.setIndexSizeMap(IndexAccessGenerator.loadIndexSizes(workloadFile));

      if (new File(InumUtils.getEnumerationFileName(workloadFile)).exists()) {
        EnumerationGenerator.loadConfigEnumerations(workloadFile, processor.query_descriptors);
      } else {
        PostgresEnumerationGenerator generator = new PostgresEnumerationGenerator();
        generator.AllEnumerateConfigs(processor.query_descriptors, AP);
        EnumerationGenerator.saveConfigEnumerations(workloadFile, processor.query_descriptors);
      }

      // test the validity of plan computation.
      for (QueryDesc queryDesc : processor.query_descriptors) {
        for (Object o : queryDesc.plans.values()) {
          Plan plan = (Plan) o;
          float cost = plan.getInternalCost();
          for (String table : queryDesc.getUsedTableNames()) {
            cost += plan.getAccessCost(table);
          }

          if (Math.abs(cost - plan.getTotalCost()) / cost > 0.01) {
            System.out.println("cost = " + cost + ", expected: " + plan.getTotalCost());
          }
        }
      }

      this.proc = processor;

    }

    /**
     * construct a default cost estimator for a given workload file.
     * @param workloadFile
     *    a persisted workload file.
     * @throws IOException
     *    if unable to find workload file.
     * @throws ParseException
     *    if workload processor could not parse workload file.
     */
    public CostEstimator(String workloadFile) throws IOException, ParseException {
      this(Initializers.initializeAutopilot(), Initializers.initializeWorkloadProcessor(
          workloadFile), workloadFile);
    }

  public float getPlanCost(String query, PhysicalConfiguration config, HashSet usedIndexes) {
        for (int i = 0; i < proc.query_descriptors.size(); i++) {
            QueryDesc desc = proc.query_descriptors.get(i);
            if(query.equals(desc.queryString)) {
                return getPlanCost(desc, config, usedIndexes);
            }
        }

        return Float.NaN;
    }

    public float getPlanCost(QueryDesc desc, PhysicalConfiguration config, HashSet usedIndexes) {
        System.out.println("desc.queryString = " + desc.queryString);
        HashSet localUsedIndexes = new HashSet();
        float minCost = getINUMCost(desc, new PhysicalConfiguration(), localUsedIndexes);

        Iterator iter = config.atomicConfigurationIterator();
        while (iter.hasNext()) {
            PhysicalConfiguration configuration = (PhysicalConfiguration) iter.next();
            HashSet usedIndexesInLastCall = new HashSet();
            float cost = getINUMCost(desc, configuration, usedIndexesInLastCall);
            if(cost < minCost) {
                minCost = cost;
                localUsedIndexes.clear();
                localUsedIndexes.addAll(usedIndexesInLastCall);
            }
        }

        usedIndexes.clear();
        usedIndexes.addAll(localUsedIndexes);
        System.out.println("plan.getTotalCost() = " + minCost + ", Used: " + usedIndexes+" config = " + config);
        return minCost;
    }

    public float getCost(QueryDesc desc, PhysicalConfiguration config) {
        return getINUMCost(desc, config, new HashSet());
    }

    public float getINUMCost(QueryDesc desc, PhysicalConfiguration config, HashSet usedIndexes) {
        // first get the interesting order combination for the config.
        PhysicalConfiguration intOrder = desc.interesting_orders;
        int qId = proc.query_descriptors.indexOf(desc);

        List tableNames = new ArrayList(intOrder.getIndexedTableNames());
        Collections.sort(tableNames);
        List<Integer> orders = new ArrayList(intOrder.getIndexedTableNames().size());
        List<Integer> emptyOrders = new ArrayList(intOrder.getIndexedTableNames().size());

        //find the position of the current config in the interesting orders.
        for (int i = 0; i < tableNames.size(); i++) {
            String tableName = (String) tableNames.get(i);
            emptyOrders.add(0);
            
            LinkedHashSet set = intOrder.getFirstIndexForTable(tableName).getColumns();
            Index idx = config.getFirstIndexForTable(tableName);
            if(idx == null) {
                orders.add(0);
                continue;
            }

            String firstColumn = idx.getFirstColumn();
            int pos = 0;
            for (Object column : set) {
                String s = (String) column;
                if (firstColumn.equals(s)) {
                    break;
                }
                pos++;
            }

            orders.add(pos == set.size() ? 0 :  pos + 1);
        }

        Plan emptyPlan = (Plan) desc.plans.get(emptyOrders.toString());
        Plan mhjPlan = (Plan) desc.plans.get(orders.toString());

        //float mhj = this.originalCostForPlan(queryString, config, mhjPlan);
        HashSet mhjUsedIndexes = new HashSet();
        float mhj = getCostForPlan(desc, config, mhjPlan, mhjUsedIndexes);
        //System.out.println("getCostForPlan(queryString, config, mhjPlan) = " + mhj);
        float nlj = Float.MAX_VALUE;

        HashSet nljUsedIndexes = new HashSet();
        Plan nljPlan = (Plan) desc.nljMap.get(orders.toString());
        if(nljPlan != null) {
            nlj = getCostForPlan(desc, config, nljPlan, nljUsedIndexes);
        }
        float cost = Math.min(desc.emptyCost, Math.min(mhj, nlj));

        usedIndexes.clear();
        if(desc.emptyCost <= cost) {
            return desc.emptyCost;
        } else if(mhj <= nlj) {
            usedIndexes.addAll(mhjUsedIndexes);
            return mhj;
        } else {
            usedIndexes.addAll(nljUsedIndexes);
            return nlj;
        }
/*
        if(rand.nextDouble() < 1) {
            Plan optimizePlan = getOptimizerPlan(desc, config);
            if(Math.abs(optimizePlan.getTotalCost()-cost)/optimizePlan.getTotalCost() > 0.05) {
                _log.warning("processMatchingConfigsExhaustive: " + config);
                _log.warning("processMatchingConfigsExhaustive: mhj " + mhj + ", nlj " + nlj);
                _log.warning("processMatchingConfigsExhaustive: " +
                        "optimzerCost = " + optimizePlan.getTotalCost() +", INUM Cost = " + cost +
                        ", mhjPlan = " + optimizePlan);
                _log.warning("processMatchingConfigsExhaustive: nljPlan " + nljPlan + ", mhjPlan " + mhjPlan);
            }
            _log.info("COSTS: " + qId + ", " + optimizePlan.getTotalCost() + "," + cost + " -- for -- " + config);
        }
*/
    }

    public Plan getOptimizerPlan(QueryDesc desc, PhysicalConfiguration config) {
        AP.implement_configuration(config);
        String query = AP.prepareQueryForConfiguration(desc, config, false, false);
        Plan plan = AP.optimizer_cost(query);
        AP.drop_configuration(config);
        if(plan.getTotalCost() > desc.emptyCost) {
            ArrayList orders = new ArrayList();
            for (String s : desc.used.getIndexedTableNames()) {
                orders.add(0);
            }
            return (Plan) desc.plans.get(orders.toString());
        } else {
            return plan;
        }
    }

    private float getCostForPlan(QueryDesc desc, PhysicalConfiguration config, Plan plan, Set usedIndexes) {
        float starting = plan.getInternalCost();
        for (Object o : desc.getUsedTableNames()) {
            String tableName = (String) o;
            float cost = plan.getAccessCost(tableName);
            Index idx = config.getFirstIndexForTable(tableName);
            if (idx != null) {
                String key;
                if (plan.isNLJTable(tableName)) {
                    key = idx.getKey() + "_NLJ_";
                } else {
                    key = idx.getKey();
                }
                Float indexCost = desc.access_costs.get(key);
                if (indexCost != null && indexCost > 0.0) {
                    cost = indexCost;
                    /*
                    if(plan.isNLJTable(tableName)) {
                        double multiple = plan.getNLJMultiple(tableName);
                        if(multiple > 0) cost *= multiple;
                    }
                    */
                    usedIndexes.add(idx.getKey());
                } else {
                    // there is a bug, the index is supposed to be helpful
                    // if not, it is useless completely, and the plan cannot be used.
                    return desc.emptyCost;
                }
            }
            starting += cost;
        }

        return starting;
    }

    /*
    private float originalCostForPlan(QueryDesc QD, PhysicalConfiguration config, Plan plan) {
        // indexSizes = new HashMap();
        Set interestingTableNames = QD.interesting_orders.getIndexedTableNames();
        float indexAccessCost = 0;

        Set configTableNames = config.getIndexedTableNames();
        List<Integer> orders = new ArrayList(interestingTableNames.size());
        // find the position of the current config in the interesting orders.
        for (Iterator iterator = QD.interesting_orders.indexes(); iterator.hasNext();) {
            Index order = (Index) iterator.next();
            if(!configTableNames.contains(order.getTableName())) {
                orders.add(0);
                continue;
            }

            String firstColumn = config.getFirstIndexForTable(order.getTableName()).getFirstColumn();
            LinkedHashSet columns = order.getColumns();
            int pos = 0;
            for (Iterator iterator1 = columns.iterator(); iterator1.hasNext();) {
                String s = (String) iterator1.next();
                if(firstColumn.equals(s)) {
                    break;
                }
                pos++;
            }

            orders.add(pos == columns.size() ? 0 :  pos + 1);
        }

        for (Iterator ci = config.indexes(); ci.hasNext();) {
            Index idx = (Index) ci.next();
            LinkedHashSet configColSet = idx.getColumns();
            Float aFloat = (Float) QD.access_costs.get(idx.getKey());
            indexAccessCost += aFloat.floatValue();
        }


        Plan plans = (Plan) QD.plans.get(orders.toString());
        float totalSubtreeCost = 0;
        totalSubtreeCost = ((MSPlanDesc) plans.list.get(0)).totalSubtreeCost;

        for (ListIterator pi = plans.list.listIterator(); pi.hasNext();) {
            MSPlanDesc PD = (MSPlanDesc) pi.next();
            //System.out.println("ec: " + PD.physicalOp + " " + PD.argument + " " + (PD.estimateIO + PD.estimateCPU) + " " + PD.totalSubtreeCost);
            if ((PD.getOpType() & (MSPlanDesc.INDEX_SCAN | MSPlanDesc.INDEX_SEEK | MSPlanDesc.TABLE_SCAN | MSPlanDesc.BKMK_LOOKUP)) != 0) {

                //remove only those leaves that correspond to an index in the configuration ..,
                if (configTableNames.contains(PD.getTable())) {
                    totalSubtreeCost -= PD.totalSubtreeCost;
                    //System.out.println("\tec: total becomes " + totalSubtreeCost);
                }
            }
        }

        //System.out.println("EstimatedCost: empty " + QD.emptyCost + " config: " + config + " totalSubtreeCost: " + totalSubtreeCost + " indexAccess " + indexAccessCost);
        totalSubtreeCost += indexAccessCost;

        float result_cost = totalSubtreeCost;
        if (QD.emptyCost < totalSubtreeCost)
            result_cost = QD.emptyCost;


        return result_cost;
    }
    */

    public float getAccessCostForQuery(Index idx, QueryDesc desc) {
        Float flt = desc.access_costs.get(idx.getKey());
        Float flt1 = desc.access_costs.get(idx.getKey()+"_NLJ_");
        flt = flt != null ? flt : 0;
        flt1 = flt1 != null ? flt1 : 0;
        return Math.max(flt1,flt);
    }
}
