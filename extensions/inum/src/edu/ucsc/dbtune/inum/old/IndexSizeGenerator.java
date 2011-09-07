package edu.ucsc.dbtune.inum.old;

import Zql.ParseException;
import edu.ucsc.dbtune.inum.old.autopilot.autopilot;
import edu.ucsc.dbtune.inum.old.autopilot.idx_cleaner;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.WorkloadProcessor;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Aug 31, 2008
 * Time: 8:21:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class IndexSizeGenerator {
    private ArrayList<Index> candidates;
    private autopilot ap;
    Map sizeMap = new HashMap();

    public IndexSizeGenerator(autopilot ap, ArrayList<Index> candidates) {
        this.ap = ap;
        this.candidates = candidates;
    }

    public void generateIndexSizes() {
        Connection conn = ap.getConnection();
        try {
        idx_cleaner.cleanIndexesDB2("", conn);
         conn.close(); } catch(Exception ex) { ex.printStackTrace(); }

        for (int i = 0; i < candidates.size(); i++) {
            Index index = (Index) candidates.get(i);
            indexSize(index);
        }
    }

    public void indexSize(Index index) {
        PhysicalConfiguration config = new PhysicalConfiguration();
        config.addIndex(index);
        ap.implement_configuration(config);
        float size = ap.getIndexSize(index);
        sizeMap.put(index.getKey(), size);
    }

    public static void main(String[] args) throws ParseException, IOException {
        WorkloadProcessor proc = new WorkloadProcessor(new File(Config.WORKLOAD_DIR,args[0]));
        proc.getInterestingOrders();
        proc.generateCandidateIndexes();
        autopilot ap = new autopilot();
        ap.init_database();

        IndexSizeGenerator isg = new IndexSizeGenerator(ap, proc.candidates);
        isg.generateIndexSizes();
        saveIndexSizes(args[0]);
    }

    private static void saveIndexSizes(String fileName) {
        
    }

    public static Map loadIndexSizes(String fileName) {
        Map sizeMap = new HashMap();


        return sizeMap;
    }
}
