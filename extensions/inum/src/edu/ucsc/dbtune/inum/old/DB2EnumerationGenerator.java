package edu.ucsc.dbtune.inum.old;

import Zql.ParseException;
import Zql.ZQuery;
import com.thoughtworks.xstream.XStream;
import edu.ucsc.dbtune.inum.old.autopilot.autopilot;
import edu.ucsc.dbtune.inum.old.autopilot.idx_cleaner;
import edu.ucsc.dbtune.inum.old.commons.ZqlUtils;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.MatView;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.Plan;
import edu.ucsc.dbtune.inum.old.model.QueryDesc;
import edu.ucsc.dbtune.inum.old.model.WorkloadProcessor;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Feb 21, 2008
 * Time: 12:09:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class DB2EnumerationGenerator {
    private boolean limitCE = false;
    public static final Set reallyInterestingTables = new HashSet(Arrays.asList("lineitem", "orders", "part", "partsupp"));
    private static final boolean PRINT_CE_TIMES = false;
    private static final Logger _log = Logger.getLogger("DB2EnumerationGenerator");

    public void AllEnumerateConfigs(WorkloadProcessor proc, final autopilot AP) {
        for (ListIterator qi = proc.query_descriptors.listIterator(); qi.hasNext();) {
            final QueryDesc QD = (QueryDesc) qi.next();

            EnumerateConfigs(QD, AP, null, proc);
        }
    }


    public void EnumerateConfigs(QueryDesc QD, autopilot AP, BlockingQueue dropQueue, WorkloadProcessor proc) {
        long startTime = 0;
        //create a mathing vector ...
        ArrayList input = new ArrayList();
        ArrayList tableNames = new ArrayList(QD.interesting_orders.getIndexedTableNames());
        QD.bestInternalPlanCost = Float.MAX_VALUE;


        for (Iterator iterator = tableNames.iterator(); iterator.hasNext();) {
            String tableName = (String) iterator.next();
            if (limitCE && !reallyInterestingTables.contains(tableName)) {
                //iterator.remove();
                input.add(0);
            } else {
                input.add(QD.interesting_orders.getFirstIndexForTable(tableName).getColumns().size());
            }
        }

        //System.out.println("EnumerateConfigs: computed input " + input);
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
            PhysicalConfiguration newConfig = createConfigurationForState(QD, tableNames, state, false, proc);
            //implement the configuration
            AP.implement_configuration(newConfig);
            _log.info("implemented (mhj): " + newConfig);
            //opt_client C = new opt_client();
            for (Iterator iterator = newConfig.indexes(); iterator.hasNext();) {
                Index index = (Index) iterator.next();
                if (!index.isImplemented()) {
                    System.out.println("index = " + index);
                }
            }
            String Q = AP.prepareQueryForEnumeration(QD, newConfig, false, false);
//            String Q = QD.parsed_query.toString();

            // now put all the plans into the optimizer.
            // Save the MHJ plan ...
            Plan plan = AP.optimizer_cost(Q);
            if(plan.getRoot() == null) {
                _log.warning("Invalid plan: " + newConfig + ", QD " + QD);
                AP.drop_configuration(newConfig);
                state = E.next();
                continue;
            }
            AP.removeQueryPreparation(QD);
            for (Iterator iterator = newConfig.indexes(); iterator.hasNext();) {
                Index index = (Index) iterator.next();
                if(plan.getAccessCost(index.getImplementedName()) <= 0){
                    _log.warning("Couldn't find usage: " + index);
                }

                if (plan.getAccessCost(index.getTableName().toUpperCase()) <= 0) {
                    float cost = plan.getAccessCost(index.getImplementedName());
                    if (cost > 0.0) {
                        plan.setAccessCost(index.getTableName().toUpperCase(), cost);
                    }

                }
            }

            QD.plans.put(state.toString(), plan);

            int indexSum = 0;
            for (Iterator iterator = state.iterator(); iterator.hasNext();) {
                Integer integer = (Integer) iterator.next();
                indexSum += integer;
            }

            if (indexSum == 0) {
                QD.emptyCost = plan.getTotalCost();
            }

            AP.drop_configuration(newConfig);

            newConfig = createConfigurationForState(QD, tableNames, state, true, proc);

            AP.implement_configuration(newConfig);
            _log.info("Implemented (nlj): " + newConfig);
            //FIX: Save the NLJ plan only if all the (interesting) indexes in the configuration are accessed through NL (Index Seek).
            //If not all of them are accessed, there will be anouther interesting order for which this will be the case.
            Q = AP.prepareQueryForEnumeration(QD, newConfig, false, true);
            plan = AP.optimizer_cost(Q);
            if(plan.getRoot() == null) {
                _log.warning("Invalid plan: " + newConfig + ", QD " + QD);
                AP.drop_configuration(newConfig);
                state = E.next();
                continue;
            }
            AP.removeQueryPreparation(QD);
            //System.out.println("EnumerateConfigs: looking for NLJ " + Q.toString());
            for (Iterator iterator = newConfig.indexes(); iterator.hasNext();) {
                Index index = (Index) iterator.next();
                if (plan.getAccessCost(index.getTableName().toUpperCase()) <= 0) {
                    float cost = plan.getAccessCost(index.getImplementedName());
                    if (cost > 0.0) {
                        plan.setAccessCost(index.getTableName().toUpperCase(), cost);
                    }
                }

                if(plan.getAccessCost(index.getImplementedName()) <= 0){
                    _log.warning("Couldn't find usage: " + index);                    
                }
            }

            for (Iterator iterator = newConfig.indexes(); iterator.hasNext();) {
                Index index = (Index) iterator.next();
                if (plan.getAccessCost(index.getTableName().toUpperCase()) <= 0) {
                    float cost = plan.getAccessCost(index.getImplementedName());
                    if (cost > 0.0) {
                        plan.setAccessCost(index.getTableName().toUpperCase(), cost);
                    }
                }
            }

            QD.nljMap.put(state.toString(), plan);

            AP.drop_configuration(newConfig);
            state = E.next();

            try {
                Connection conn = AP.getConnection();
                Statement stmt = conn.createStatement();
                stmt.execute("delete from advise_index");
                stmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        if (QD.mava_costs != null) {
            for (Iterator iterator = QD.mava_costs.keySet().iterator(); iterator.hasNext();) {
                String viewKey = (String) iterator.next();

                Float accessCost = (Float) QD.mava_costs.get(viewKey);
                if (accessCost != null && accessCost > 0.0) {
                    ZQuery query = null;
                    try {
                        query = ZqlUtils.parseSQL(viewKey);
                    } catch (ParseException e) {
                        e.printStackTrace();//To change body of catch statement use File | Settings | File Templates.
                        continue;
                    }
                    MatView view = new MatView(query);
                    Set tables = view.getTables();
                    // now create the sate element again, but with the tables not in tables.
                    input = new ArrayList();

                    boolean covered = true;
                    for (Iterator iterator1 = tableNames.iterator(); iterator1.hasNext();) {
                        String tableName = (String) iterator1.next();
                        if ((limitCE && !reallyInterestingTables.contains(tableName)) || tables.contains(tableName)) {
                            // iterator.remove();
                            input.add(0);
                        } else {
                            input.add(QD.interesting_orders.getFirstIndexForTable(tableName).getColumns().size());
                            covered = false;
                        }
                    }

                    E = new Enumerator(input);
                    state = E.next();
                    while (state != null) {
                        /*
                                            if(QD.plans.get(state.toString()+view.getKey()) != null) {
                                                state = E.next();
                                                continue;
                                            }
                        */
                        PhysicalConfiguration newConfig = createConfigurationForState(QD, tableNames, state, true, proc);

                        newConfig.addMatView(view);

                        //implement the configuration
                        AP.implement_configuration(newConfig);
                        System.out.println("implemented " + newConfig);
                        //opt_client C = new opt_client();
                        String Q = AP.prepareQueryForEnumeration(QD, newConfig, false, true);

                        // now put all the plans into the optimizer.
                        // Save the MHJ plan ...
                        Plan plan = AP.optimizer_cost(Q);
                        AP.removeQueryPreparation(QD);

                        QD.plans.put(state.toString() + view.getKey(), plan);

                        //FIX: Save the NLJ plan only if all the (interesting) indexes in the configuration are accessed through NL (Index Seek).
                        //If not all of them are accessed, there will be anouther interesting order for which this will be the case.
                        Q = AP.prepareQueryForEnumeration(QD, newConfig, false, true);
                        Plan nlPlan = AP.optimizer_cost(Q);
                        AP.removeQueryPreparation(QD);
                        //System.out.println("EnumerateConfigs: looking for NLJ " + Q.toString());

                        boolean nlj = false;

                        nlj = nlPlan.isNLJPlan();
                        if (nlj) {
                            QD.nljMap.put(state.toString() + view.getKey(), nlPlan);
                        }

                        AP.drop_configuration(newConfig);
                        state = E.next();
                    }
                }
            }
        }


    }

    private PhysicalConfiguration createConfigurationForState(QueryDesc QD, ArrayList tableNames, List state, boolean nlj, WorkloadProcessor proc) {
        long startTime;
        System.out.println("EnumerateConfigs state:" + state);
        PhysicalConfiguration newConfig = new PhysicalConfiguration();
        if (PRINT_CE_TIMES) startTime = System.currentTimeMillis();
        for (int si = 0; si < state.size(); si++) {
            int index = ((Integer) state.get(si)).intValue();
            if (index > 0) {
                String tableName = (String) tableNames.get(si);
                LinkedHashSet newColumnSet = new LinkedHashSet();
                Index idx = QD.interesting_orders.getFirstIndexForTable(tableName);
                String column = idx.getColumnByIndex(index - 1);

                Index configIndex = getIndexForColumn(column, proc.getCandidateIndexes(), nlj, QD);

                //_log.info("createConfigurationForState: " + configIndex);
                if (configIndex == null) {
                    _log.info("createConfigurationForState: " + column + ", " + nlj);
                    newColumnSet.add(column);

                    //System.out.println("CostEstimator: table = " + tableName);
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
                    //System.out.println("EnumerateConfig: adding columnSet "
                    //+ newColumnSet + " for table " + tableName);

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
                    configIndex = new Index(tableName, newColumnSet);
                }
                newConfig.addIndex(configIndex);
            }
        }
        return newConfig;
    }

    private Index getIndexForColumn(String column, List candidateIndexes, boolean nlj, QueryDesc qd) {
        Index idx = null;
        float currentAccessCost = nlj ? Float.MAX_VALUE : Float.MIN_VALUE;

        for (Iterator iterator = candidateIndexes.iterator(); iterator.hasNext();) {
            Index idx1 = (Index) iterator.next();
            if (!idx1.getFirstColumn().equals(column)) {
                continue;
            }
            Float cost = qd.access_costs.get(idx1.getKey());
            if (cost == null || cost <= 0.0) {
                continue;
            }
            if (nlj ? (cost < currentAccessCost) : (cost > currentAccessCost)) {
                idx = idx1;
                currentAccessCost = cost;
            }
        }

        return idx;
    }

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


    public static void saveConfigEnumerations(String workload, List query_descriptors) throws IOException {
        String filename = InumUtils.getEnumerationFileName(workload);
        System.out.println("CostEstimator.saveConfigEnumerations(" + filename + ")");
        XStream xstream = new XStream();
        GZIPOutputStream oos = new GZIPOutputStream(new FileOutputStream(filename));
        xstream.toXML(query_descriptors, oos);
        oos.finish();
        oos.close();
    }

    public static void loadConfigEnumerations(String workload, WorkloadProcessor proc) throws IOException {
        String filename = InumUtils.getEnumerationFileName(workload);
        System.out.println("CostEstimator.loadConfigEnumerations(" + filename + ")");
        XStream xstream = new XStream();

        GZIPInputStream ois = new GZIPInputStream(new FileInputStream(filename));
        List loadedQueryDescriptors = (List) xstream.fromXML(ois);
        ois.close();
        for (int i = 0; i < loadedQueryDescriptors.size(); i++) {
            QueryDesc desc = (QueryDesc) loadedQueryDescriptors.get(i);
            QueryDesc current = (QueryDesc) proc.query_descriptors.get(i);
            current.emptyCost = desc.emptyCost;
            current.nljMap = desc.nljMap;
            current.plans = desc.plans;
            current.optimalPlanCosts = desc.optimalPlanCosts;
        }
    }

    public static void main(String[] args) throws ParseException, IOException {
        String fileName = args[0];
        idx_cleaner.main(new String[] {});
        WorkloadProcessor proc = new WorkloadProcessor(new File(Config.WORKLOAD_DIR, fileName));
        proc.getInterestingOrders();
        proc.generateCandidateIndexesPerQuery();
        IndexAccessGenerator.loadIndexAccessCosts(fileName, proc.query_descriptors);
        Map indexSizes = IndexAccessGenerator.loadIndexSizes(fileName);
        // MatViewAccessGenerator.loadConfigEnumerations(fileName, proc.query_descriptors);
        autopilot ap = new autopilot();
        ap.init_database();
        ap.setIndexSizeMap(indexSizes);
        boolean update = args.length > 1 && args[1].equals("update");

        if (new File(InumUtils.getEnumerationFileName(fileName)).exists()) {
            if (!update) {
                System.err.println("The file aready exists!");
                ap.dispose();
                System.exit(-1);
            } else {
                loadConfigEnumerations(fileName, proc);
            }
        }

        DB2EnumerationGenerator generator = new DB2EnumerationGenerator();
        generator.AllEnumerateConfigs(proc, ap);
        saveConfigEnumerations(fileName, proc.query_descriptors);
        ap.dispose();
    }
}
