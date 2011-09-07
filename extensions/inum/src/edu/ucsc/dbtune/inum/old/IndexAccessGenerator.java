package edu.ucsc.dbtune.inum.old;

import Zql.ParseException;
import edu.ucsc.dbtune.inum.old.autopilot.autopilot;
import edu.ucsc.dbtune.inum.old.autopilot.idx_cleaner;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.Plan;
import edu.ucsc.dbtune.inum.old.model.QueryDesc;
import edu.ucsc.dbtune.inum.old.model.WorkloadProcessor;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.collections.CollectionUtils;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Dec 29, 2007
 * Time: 12:41:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class IndexAccessGenerator {
    private List indexes;
    private List queryDescriptors;
    private autopilot ap;
    private static final Logger _log = Logger.getLogger("IndexAccessGenerator");
    private HashMap<String, Integer> sizeMap = new HashMap();
    private static ArrayList<Map<String, Float>> indexAccessCosts;
    private static String indexAccessCostFile;
    private static Map indexSizeMap;
    private static String indexSizeWorkload;

    public IndexAccessGenerator(autopilot ap, List indexes, List queryDescriptors) throws ParseException {
        this.indexes = indexes;
        this.queryDescriptors = queryDescriptors;
        this.ap = ap;
    }

    public void generateIndexAccessCosts() {
        Connection conn = ap.getConnection();
        try {
            idx_cleaner.cleanIndexesMS("idx", conn);
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        for (int i = 0; i < indexes.size(); i++) {
            Index index = (Index) indexes.get(i);
            indexAccessCost(index, queryDescriptors);
        }
    }

    public void indexAccessCost(Index index, List queryQescs) {
        PhysicalConfiguration config = new PhysicalConfiguration();
        config.addIndex(index);
        ap.implement_configuration(config);
        _log.info("Implemented Index: " + index);
        for (int i = 0; i < queryQescs.size(); i++) {
            QueryDesc desc = (QueryDesc) queryQescs.get(i);
            //used replaced interesting_orders because of the bug
            if( !desc.used.getIndexedTableNames().contains(index.getTableName()) ||
                !CollectionUtils.containsAny(desc.used.getFirstIndexForTable(index.getTableName()).getColumns(), index.getColumns())) {
                desc.access_costs.put(index.getKey(), 0.0f);
                sizeMap.put(index.getKey(), ap.getIndexSize(index));
                continue;
            }

            String query = ap.prepareQueryForEnumeration(desc, config, true, false);
            Plan plan = ap.optimizer_cost(query);
            int size = ap.getIndexSize(index);
            sizeMap.put(index.getKey(), size);
            ap.removeQueryPreparation(desc);

            float cost = plan.getAccessCost(index.getImplementedName());
            if(cost <= 0.0) {
                _log.warning("Could not find index usage: " + index);
            }
            desc.access_costs.put(index.getKey(), cost);
            System.out.println(i + "\t" + index.getKey() + "\t" + cost);
        }
        ap.drop_configuration(config);
        Connection conn = ap.getConnection();
        try {
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void loadIndexAccessCosts(String workload, List queryDescriptors) throws IOException {
        if(indexAccessCosts == null || !workload.equals(indexAccessCostFile)) {
            indexAccessCostFile = workload;
            String filename = InumUtils.getIndexAccessCostFile(workload);
            System.out.println("CostEstimator.loadIndexAccessCosts(" + filename + ")");
            indexAccessCosts = new ArrayList<Map<String,Float>>();
            for (int i = 0; i < queryDescriptors.size(); i++) {
                indexAccessCosts.add(new HashMap());
            }
            
            Scanner scanner = new Scanner(new GZIPInputStream(new FileInputStream(filename)));
            scanner.useDelimiter("[\\s&&[^ ]]+");
            while (scanner.hasNext()) {
                int queryId = 0;
                if (scanner.hasNextInt()) {
                    queryId = scanner.nextInt();
                } else {
                    System.err.println("Current token = -->" + scanner.next() + "<--");
                    continue;
                }

                String indexKey = scanner.next();
                float cost = 0;
                try {
                    if (scanner.hasNextFloat())
                        cost = scanner.nextFloat();
                    else {
                        System.err.println("Current token = -->" + scanner.next() + "<--");
                        continue;
                    }
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    continue;
                }

                indexAccessCosts.get(queryId).put(indexKey, cost);
            }
            scanner.close();            
        }

        for (int i = 0; i < queryDescriptors.size(); i++) {
            QueryDesc queryDesc = (QueryDesc) queryDescriptors.get(i);
            queryDesc.access_costs.putAll(indexAccessCosts.get(i));
        }
    }

    public static Map loadIndexSizes(String workload) throws IOException {
        if (indexSizeMap == null || !workload.equals(indexSizeWorkload)) {
            indexSizeWorkload = workload;
            String filename = InumUtils.getIndexSizeFile(workload);
            System.out.println("CostEstimator.loadIndexAccessCosts(" + filename + ")");
            Scanner scanner = new Scanner(new GZIPInputStream(new FileInputStream(filename)));
            scanner.useDelimiter("[\\s&&[^ ]]+");
            Map map = new HashMap();
            while(scanner.hasNext()) {
                String indexKey = scanner.next();
                int size = scanner.nextInt();
                map.put(indexKey, size);
            }

            scanner.close();
            indexSizeMap = map;
        }

        return indexSizeMap;
    }

    public static void saveIndexSizes(String workload, Map map) throws IOException {
        String filename = InumUtils.getIndexSizeFile(workload);
        PrintStream ps = new PrintStream(new GZIPOutputStream(new FileOutputStream(filename)));
        for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry entry = (Map.Entry) iterator.next();
            ps.println(entry.getKey()+"\t"+entry.getValue());
        }
        ps.close();
    }

    public static void saveIndexCosts(String workload, List query_descriptors) throws IOException {
        System.out.println("CostEstimator.saveIndexCosts(" + workload + ")");

        String filename = InumUtils.getIndexAccessCostFile(workload);
        PrintStream ps = new PrintStream(new GZIPOutputStream(new FileOutputStream(filename)));
        for (int j = 0; j < query_descriptors.size(); j++) {
            QueryDesc queryDesc = (QueryDesc) query_descriptors.get(j);
            if (queryDesc.access_costs == null) continue;
            for (Map.Entry entry : queryDesc.access_costs.entrySet()) {
                ps.println(String.valueOf(j) + "\t" + entry.getKey() + "\t" + entry.getValue());
            }
        }
        ps.close();
    }

    public static void main(String[] args) throws ParseException, IOException {
        WorkloadProcessor proc = new WorkloadProcessor(new File(Config.WORKLOAD_DIR, args[0]));
        proc.getInterestingOrders();
        List idxList = new ArrayList();
        if (args.length == 1) {
            proc.generateCandidateIndexes();
            //idxList = proc.getCandidateClusteredIndexes("lineitem");
            idxList.addAll(proc.candidates);
        } else {
            idxList = PhysicalConfiguration.loadIndexesFromFile(args[1]);
        }

        if (new File(InumUtils.getIndexAccessCostFile(args[0])).exists()) {
            return;
        }
        
        autopilot ap = new autopilot();
        ap.init_database();
        IndexAccessGenerator iag = new IndexAccessGenerator(ap, idxList, proc.query_descriptors);
        iag.generateIndexAccessCosts();
        saveIndexCosts(args[0], proc.query_descriptors);
        saveIndexSizes(args[0], iag.sizeMap);
        ap.dispose();
    }
}
