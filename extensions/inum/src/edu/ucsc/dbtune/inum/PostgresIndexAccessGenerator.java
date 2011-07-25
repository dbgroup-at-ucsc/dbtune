/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.ucsc.dbtune.inum;

import Zql.ParseException;
import edu.ucsc.dbtune.inum.autopilot.PostgresPlan;
import edu.ucsc.dbtune.inum.autopilot.autopilot;
import edu.ucsc.dbtune.inum.model.Index;
import edu.ucsc.dbtune.inum.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.model.QueryDesc;
import edu.ucsc.dbtune.inum.model.WorkloadProcessor;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Strings;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ColumnListHandler;

/**
 * @author cristina
 *         use to generate the index costs for a set of queries written from a file
 *         the index costs will be also saved into a file
 */
public class PostgresIndexAccessGenerator {
    private List indexes;
    private List queryDescriptors;
    private autopilot ap;
    private static final Logger _log = Logger.getLogger("IndexAccessGenerator");
    public HashMap<String, Integer> getSizeMap() {
		return sizeMap;
	}

	private HashMap<String, Integer> sizeMap = new HashMap();


    public PostgresIndexAccessGenerator(autopilot ap, List indexes, List queryDescriptors) throws ParseException {
        this.indexes = indexes;
        this.queryDescriptors = queryDescriptors;
        this.ap = ap;
    }
    
	public PostgresIndexAccessGenerator(autopilot ap, List<Index> indexes) throws ParseException {
        this.indexes = indexes;
        this.queryDescriptors = null;
        this.ap = ap;
    }

	public PostgresIndexAccessGenerator(autopilot ap) throws ParseException {
        this.indexes = null;
        this.queryDescriptors = null;
        this.ap = ap;
    }


    public void generateIndexAccessCosts() {
        for (int i = 0; i < indexes.size(); i++) {
            Index index = (Index) indexes.get(i);
            indexAccessCost(index, queryDescriptors);
        }
    }

    public String generateIndexAccessCost(PhysicalConfiguration config, QueryDesc queryDesc, boolean nlj) {
        return indexAccessCost(config, queryDesc, nlj);
    }
 
    //this will  get the cost for an index access configuration
    //Get paln for a particular index and a particular query
    public String indexAccessCost(PhysicalConfiguration config, QueryDesc queryDesc, boolean nlj)
    {
        ap.implement_configuration(config);

        String query = ap.prepareQueryForConfiguration(queryDesc, config, true, nlj);
        PostgresPlan plan = (PostgresPlan) ap.optimizer_cost(query);

        ap.removeQueryPreparation(queryDesc);
        ap.drop_configuration(config);
 
        Connection conn = ap.getConnection();
        try {
        	conn.close(); 
        } catch(Exception ex) 
        	{ ex.printStackTrace(); }
	 
        return plan.getPlan();
    }

    //this will  get the cost for an index access of some queries
    public void indexAccessCost(Index index, List queryQescs) {
        PhysicalConfiguration config = new PhysicalConfiguration();
        config.addIndex(index);
        ap.implement_configuration(config);
        _log.info("Implemented Index: " + index);

        //for each query get the index access cost
        for (int i = 0; i < queryQescs.size(); i++) 
        {
            QueryDesc desc = (QueryDesc) queryQescs.get(i);
            /*   
				String testBug = index.getTableName();
        		PhysicalConfiguration phBug = queryString.used;
        		Set setBug = phBug.getIndexedTableNames();
        		PhysicalConfiguration phBug2 = queryString.used;
        		Index indBug =  phBug2.getFirstIndexForTable(testBug);
        		LinkedHashSet hsBug = indBug.getColumns(); */

            if (!desc.used.getIndexedTableNames().contains(index.getTableName()) ||
                    !CollectionUtils.containsAny(desc.used.getFirstIndexForTable(index.getTableName()).getColumns(), index.getColumns())) {
                desc.access_costs.put(index.getKey(), 0.0f);
                sizeMap.put(index.getKey(), ap.getIndexSize(index));
                continue;
            }

            //get the cost when the nested loops will be disabled for the plan
            //get the plan, and the index size
            String query = ap.prepareQueryForConfiguration(desc, config, true, false);
            PostgresPlan plan = (PostgresPlan) ap.optimizer_cost(query);
            int size = ap.getIndexSize(index);//get the index size
            sizeMap.put(index.getKey(), size);
            ap.removeQueryPreparation(desc);
            //get the  index cost
            float cost = plan.getAccessCost(index.getImplementedName());
            if (cost <= 0.0) {
                _log.warning("Could not find index usage: " + index);
            }
            //put the access cost in the access_costs hashMap
            desc.access_costs.put(index.getKey(), cost);
//            System.out.println(i + "\t" + index.getKey() + "\t" + cost);
            //get the cost when the nested loops will be enabled for the plan
            //get the plan, and the index size
            query = ap.prepareQueryForConfiguration(desc, config, true, true);
            plan = (PostgresPlan) ap.optimizer_cost(query);
            ap.removeQueryPreparation(desc);
            // get the index access cost
            cost = plan.getAccessCost(index.getImplementedName());
            if (cost <= 0.0) {
                _log.warning("Could not find index usage: " + index);
            }
            //put the access cost in the access_costs hashMap
            desc.access_costs.put(index.getKey() + "_NLJ_", cost);
//            System.out.println(i + "\t" + index.getKey() + "_NLJ_\t" + cost);
        }//end for each query

        ap.drop_configuration(config);
        Connection conn = ap.getConnection();
        try {
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }//end f indexAccessGenerator()

    private void printImplementedIndexes() {
        Connection conn = ap.getConnection();
        QueryRunner run = new QueryRunner();
        try {
            List res = (List) run.query(conn, "select name from advise_index", new ColumnListHandler(1));
            _log.info("Indexes: " + res);
            DbUtils.close(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void loadIndexAccessCosts(String workload, List queryDescriptors) throws IOException {
        String filename = InumUtils.getIndexAccessCostFile(workload);

        System.out.println("CostEstimator.loadIndexAccessCosts(" + filename + ")");
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


            ((QueryDesc) queryDescriptors.get(queryId)).access_costs.put(indexKey, cost);
        }
        scanner.close();
    }

    public static Map loadIndexSizes(String workload) throws IOException {
        String filename = InumUtils.getIndexSizeFile(workload);
        System.out.println("CostEstimator.loadIndexAccessCosts(" + filename + ")");
        Scanner scanner = new Scanner(new GZIPInputStream(new FileInputStream(filename)));
        scanner.useDelimiter("[\\s&&[^ ]]+");
        Map map = new HashMap();
        while (scanner.hasNext()) {
            String indexKey = scanner.next();
            int size = scanner.nextInt();
            map.put(indexKey, size);
        }

        scanner.close();
        return map;
    }

    public static void saveIndexSizes(String workload, Map map) throws IOException {
        String filename = InumUtils.getIndexSizeFile(workload);
        PrintStream ps = new PrintStream(new GZIPOutputStream(new FileOutputStream(filename)));
        for (Iterator iterator = map.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry entry = (Map.Entry) iterator.next();
            ps.println(entry.getKey() + "\t" + entry.getValue());
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

    public static void postgresIndexAccessGeneratorMain(String filName) throws ParseException, IOException {

        //WorkloadProcessor proc = new WorkloadProcessor(new File(Config.WORKLOAD_DIR,args[0]));
        //WorkloadProcessor proc = new WorkloadProcessor(new File(Config.WORKLOAD_DIR,"test.sql"));
        WorkloadProcessor proc = new WorkloadProcessor(new File(Config.WORKLOAD_DIR, filName));
        proc.getInterestingOrders();
        proc.generateCandidateIndexes();
        autopilot ap = new autopilot();
        ap.init_database();
        /*
        if(new File(InumUtils.getIndexAccessCostFile(args[0])).exists()) {
            DB2IndexAccessGenerator.loadIndexAccessCosts(args[0], proc.query_descriptors);
        } */
        if (new File(InumUtils.getIndexAccessCostFile(filName)).exists()) {
            PostgresIndexAccessGenerator.loadIndexAccessCosts(filName, proc.query_descriptors);
        } else {
            PostgresIndexAccessGenerator iag = new PostgresIndexAccessGenerator(ap, proc.candidates, proc.query_descriptors);
            iag.generateIndexAccessCosts();
            saveIndexCosts(filName, proc.query_descriptors);
            saveIndexSizes(filName, iag.sizeMap);
        }
        ap.dispose();
    }

    // todo(Huascar) document this since we will be using it in inum what if optimizer
    public static void runPostgresIndexAccessGenerator(autopilot autopilot,
        WorkloadProcessor processor,
        String workloadFilename) throws
        ParseException, IOException {

      final WorkloadProcessor proc = Checks.checkNotNull(processor);
      final autopilot         ap   = Checks.checkNotNull(autopilot);

      final String            filename = Checks.checkArgument(workloadFilename,
          !Strings.isEmpty(workloadFilename),
          "illegal workload filename."
      );

      for(Index each : proc.candidates){
        System.out.println("index = " + each);
      }

      PostgresIndexAccessGenerator iag = new PostgresIndexAccessGenerator(
          ap,
          proc.candidates,
          proc.query_descriptors
      );

      iag.generateIndexAccessCosts();
      saveIndexCosts(filename, proc.query_descriptors);
      saveIndexSizes(filename, iag.sizeMap);

      ap.dispose();
    }

    public static void main(String[] args) throws ParseException, IOException {
        if (new File(InumUtils.getIndexAccessCostFile(args[0])).exists()) {
            return;
        }

        WorkloadProcessor proc = new WorkloadProcessor(args[0]);
        proc.getInterestingOrders();
        proc.generateCandidateIndexes();
        autopilot ap = new autopilot();
        ap.init_database();

        runPostgresIndexAccessGenerator(ap, proc, args[0]);

    }
}


