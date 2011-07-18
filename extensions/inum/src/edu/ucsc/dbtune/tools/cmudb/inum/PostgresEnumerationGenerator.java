/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.ucsc.dbtune.tools.cmudb.inum;

import Zql.ParseException;
import edu.ucsc.dbtune.tools.cmudb.Config;
import edu.ucsc.dbtune.tools.cmudb.autopilot.autopilot;
import edu.ucsc.dbtune.tools.cmudb.model.Index;
import edu.ucsc.dbtune.tools.cmudb.model.PhysicalConfiguration;
import edu.ucsc.dbtune.tools.cmudb.model.Plan;
import edu.ucsc.dbtune.tools.cmudb.model.QueryDesc;
import edu.ucsc.dbtune.tools.cmudb.model.WorkloadProcessor;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

/**
 * @author cristina
 *         class used to generate the access costs for all kind of combination of interesting orders
 */
public class PostgresEnumerationGenerator {
    private boolean limitCE = false;
    public static final Set reallyInterestingTables = new HashSet(Arrays.asList("lineitem", "orders", "part", "partsupp"));
    private static final boolean PRINT_CE_TIMES = false;
    private static final Logger _log = Logger.getLogger("DB2EnumerationGenerator");


    //get all the enum generator

    public void AllEnumerateConfigs(List queryDescs, final autopilot AP) {
        int i = 0;
        //for each query call EnumerateConfigs
        for (ListIterator qi = queryDescs.listIterator(); qi.hasNext();) {
            final QueryDesc QD = (QueryDesc) qi.next();

            EnumerateConfigs(QD, AP, null);
        }
    }

    public void EnumerateConfigs(QueryDesc QD, autopilot AP, BlockingQueue dropQueue) {
        long startTime = 0;
        //create a mathing vector ...
        ArrayList input = new ArrayList();
        ArrayList tableNames = new ArrayList(QD.interesting_orders.getIndexedTableNames());
        Collections.sort(tableNames);
        QD.bestInternalPlanCost = Float.MAX_VALUE;

        for (Iterator iterator = tableNames.iterator(); iterator.hasNext();) {
            String tableName = (String) iterator.next();
            if (limitCE && !reallyInterestingTables.contains(tableName)) {
                //iterator.remove();
                input.add(0);
            } else {
                input.add(QD.interesting_orders.getFirstIndexForTable(tableName).getColumns().size());
            }
        }//end for tableNames.iterator()

        if (input.size() == 0) {
            PhysicalConfiguration emptyConfig = new PhysicalConfiguration();
            if (PRINT_CE_TIMES) startTime = System.currentTimeMillis();
            String Q = AP.prepareQueryForEnumeration(QD, emptyConfig, false, true);
            Plan plan = AP.optimizer_cost(Q);
            AP.removeQueryPreparation(QD);
            //if (PRINT_CE_TIMES) printCEDebug(qId, (System.currentTimeMillis() - startTime), emptyConfig);
            QD.plans.put(new String("[]"), plan);
            QD.emptyCost = plan.getTotalCost();
            System.out.println("EnumerateConfigs: emptyCost = " + QD.emptyCost);
            return;
        }
        Enumerator E = new Enumerator(input);
        List state = E.next();
        while (state != null) {
            if (QD.plans.get(state.toString()) != null) {
                state = E.next();
                continue;
            }
            //create a new configuration and get the plan when nested loops is disabled
            PhysicalConfiguration newConfig = createConfigurationForState(QD, tableNames, state);

            //implement the configuration
            AP.implement_configuration(newConfig);
            System.out.println("implemented " + newConfig);
            //prepare query for enumeration
            String Q = AP.prepareQueryForEnumeration(QD, newConfig, false, false);
            // now put all the plans into the optimizer.
            // Save the MHJ plan ...
            Plan plan = AP.optimizer_cost(Q);
            plan.fixAccessCosts(newConfig, QD);//if some tables are not found
            //delete from cache the infor added at prepareQueryForEnumeration
            AP.removeQueryPreparation(QD);
            //save the plan
            QD.plans.put(state.toString(), plan);

            // enable nested loops, and get the plan for the same configuration
            //(Dash from ms)FIX: Save the NLJ plan only if all the (interesting) indexes in the configuration are accessed through NL (Index Seek).
            //(Dasd from ms)If not all of them are accessed, there will be another interesting order for which this will be the case.
            Q = AP.prepareQueryForEnumeration(QD, newConfig, false, true);
            Plan nlPlan = AP.optimizer_cost(Q);
            nlPlan.fixAccessCosts(newConfig, QD);
            AP.removeQueryPreparation(QD);

            int indexSum = 0;
            for (Iterator iterator = state.iterator(); iterator.hasNext();) {
                Integer integer = (Integer) iterator.next();
                indexSum += integer;
            }
            //if there is no index then set the emptyCost as the minimum between the nlj and non -nlj plan cost
            if (indexSum == 0) {
                if (plan.getTotalCost() < nlPlan.getAccessCost(Q))
                    QD.emptyCost = plan.getTotalCost();
                else QD.emptyCost = nlPlan.getTotalCost();
            }
            boolean nlj = false;

            nlj = nlPlan.isNLJPlan();
            if (nlj) {
                QD.nljMap.put(state.toString(), nlPlan);
            }
            //remove from cache info about the configuration
            AP.drop_configuration(newConfig);
            state = E.next();
        }//end while

    }//end f enumerateConfigs()

    private PhysicalConfiguration createConfigurationForState(QueryDesc QD, ArrayList tableNames, List state) {
        long startTime;
        System.out.println("EnumerateConfigs state:" + state);
        PhysicalConfiguration newConfig = new PhysicalConfiguration();
        if (PRINT_CE_TIMES) 
        	startTime = System.currentTimeMillis();
        //for each index from the state, add it
        for (int si = 0; si < state.size(); si++) {
            int index = ((Integer) state.get(si)).intValue();
            if (index > 0) {
                //get the table name
                String tableName = (String) tableNames.get(si);
                LinkedHashSet newColumnSet = new LinkedHashSet();

                String column = QD.interesting_orders.getFirstIndexForTable(tableName).getColumnByIndex(index - 1);
                /*
                for (Iterator iterator = QD.interesting_orders.getIndexesForTable(tableName).iterator(); iterator.hasNext();) {
                    Index idx = (Index) iterator.next();
                    if (index == 1) {
                        newColumnSet.addAll(idx.getColumns());
                        break;
                    }
                    index--;
                }
                 */
                newColumnSet.add(column);

                System.out.println("\t" + newColumnSet);

                // get all the used columns for the table.
                LinkedHashSet used = new LinkedHashSet(QD.used.getFirstIndexForTable(tableName).getColumns());

                if (QD.group_by != null) {
                    Index gIndex = QD.group_by.getFirstIndexForTable(tableName);
                    LinkedHashSet groupBySet = null;
                    if (gIndex != null) {
                        LinkedHashSet gSet = gIndex.getColumns();
                        groupBySet = new LinkedHashSet(gSet); //make a copy
                    }

                    if (groupBySet != null) {
                        used.removeAll(groupBySet);
                        groupBySet.removeAll(newColumnSet);
                    }
                    newColumnSet.addAll(used);
                    if (groupBySet != null)
                        newColumnSet.addAll(groupBySet);
                } else {
                    newColumnSet.addAll(used);
                }
                //restrict to the first 16 columns.
                int columnCount = 0;
                Iterator nci = newColumnSet.iterator();
                if (newColumnSet.size() > 16) {
                    while (nci.hasNext()) {
                        nci.next();
                        columnCount++;
                        if (columnCount > 16) {
                            nci.remove();
                        }
                    }
                }

                newConfig.addIndex(new Index(tableName, newColumnSet));
            }//end for if index > 0      
        }//end for state.size()
        return newConfig;
    }// end for f createConfigurationForState)

    public ArrayList filterVector(ArrayList vector) {
        ArrayList result = new ArrayList();
        for (int i = 0; i < vector.size(); i++) {
            if (((Integer) vector.get(i)).intValue() > 0)
                result.add(new Integer(1));
            else
                result.add(new Integer(0));
        }
        return result;
    }

    public static void postgresEnumerationGeneratorMain(String filName) throws ParseException, IOException {
        String fileName = filName;
        WorkloadProcessor proc = new WorkloadProcessor(new File(Config.WORKLOAD_DIR, fileName));
        proc.getInterestingOrders();
        proc.generateCandidateIndexesPerQuery();
        autopilot ap = new autopilot();
        ap.init_database();
        boolean update = false;
        if (new File(InumUtils.getEnumerationFileName(fileName)).exists()) {
            /* if (!update) {
              System.err.println("The file aready exists!");
              System.exit(-1);
          } else {*/
            EnumerationGenerator.loadConfigEnumerations(fileName, proc.query_descriptors);
            //}
        }

        PostgresEnumerationGenerator generator = new PostgresEnumerationGenerator();
        generator.AllEnumerateConfigs(proc.query_descriptors, ap);
        EnumerationGenerator.saveConfigEnumerations(fileName, proc.query_descriptors);
    }

    public static void deleteEnumerationFile(String fileName) {
        File file = new File(InumUtils.getEnumerationFileName(fileName));
        if (file.exists())
            if (!file.delete())
                System.out.println("Deleting enumeration file failed...");
    }

    public static void main(String[] args) throws ParseException, IOException {
        String fileName = args[0];
        // idx_cleaner.main(new String[]{});
        WorkloadProcessor proc = new WorkloadProcessor(new File(edu.ucsc.dbtune.tools.cmudb.Config.WORKLOAD_DIR, fileName));
        proc.getInterestingOrders();
        proc.generateCandidateIndexesPerQuery();
        // MatViewAccessGenerator.loadConfigEnumerations(fileName, proc.query_descriptors);
        boolean update = args.length > 1 && args[1].equals("update");

        if (new File(InumUtils.getEnumerationFileName(fileName)).exists()) {
            if (!update) {
                System.err.println("The file aready exists!");
                return;
            } else {
                EnumerationGenerator.loadConfigEnumerations(fileName, proc.query_descriptors);
            }
        }
        autopilot ap = new autopilot();
        ap.init_database();

        PostgresEnumerationGenerator generator = new PostgresEnumerationGenerator();
        generator.AllEnumerateConfigs(proc.query_descriptors, ap);
        EnumerationGenerator.saveConfigEnumerations(fileName, proc.query_descriptors);

        ap.dispose();
    }
}

