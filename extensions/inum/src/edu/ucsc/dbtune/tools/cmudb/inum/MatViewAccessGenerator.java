package edu.ucsc.dbtune.tools.cmudb.inum;

import Zql.ParseException;
import com.thoughtworks.xstream.XStream;
import edu.ucsc.dbtune.tools.cmudb.autopilot.autopilot;
import edu.ucsc.dbtune.tools.cmudb.model.Index;
import edu.ucsc.dbtune.tools.cmudb.model.MatView;
import edu.ucsc.dbtune.tools.cmudb.model.PhysicalConfiguration;
import edu.ucsc.dbtune.tools.cmudb.model.Plan;
import edu.ucsc.dbtune.tools.cmudb.model.QueryDesc;
import edu.ucsc.dbtune.tools.cmudb.model.WorkloadProcessor;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Mar 4, 2008
 * Time: 12:53:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class MatViewAccessGenerator {
    public static void main(String[] args) throws ParseException, IOException {
        String workload = args[0];

        autopilot ap = new autopilot();
        ap.init_database();

        WorkloadProcessor proc = new WorkloadProcessor(new File(edu.ucsc.dbtune.tools.cmudb.Config.WORKLOAD_DIR, workload));
        List matViews = proc.generateCandidateMatViews();
        List matViewCosts = new ArrayList();

        for (int i = 0; i < proc.query_descriptors.size(); i++) {
            QueryDesc desc = (QueryDesc) proc.query_descriptors.get(i);

            desc.mava_costs = new HashMap();
        }

        for (int i = 0; i < matViews.size(); i++) {
            MatView view = (MatView) matViews.get(i);

            PhysicalConfiguration config = new PhysicalConfiguration();
            config.addMatView(view);
            ap.implement_configuration(config);

            if (!view.isImplemented()) {
                continue;
            }

            Set tables = view.getTables();

            for (int j = 0; j < proc.query_descriptors.size(); j++) {
                QueryDesc queryDesc = (QueryDesc) proc.query_descriptors.get(j);

                PhysicalConfiguration config1 = new PhysicalConfiguration();
                for (Iterator iterator = queryDesc.interesting_orders.getIndexedTableNames().iterator(); iterator.hasNext();)
                {
                    String tableName = (String) iterator.next();

                    if (!tables.contains(tableName)) {
                        // add all covering indexes with all the
                        LinkedHashSet cols = queryDesc.interesting_orders.getFirstIndexForTable(tableName).getColumns();
                        for (Iterator iterator1 = cols.iterator(); iterator1.hasNext();) {
                            String colName = (String) iterator1.next();

                            LinkedHashSet cols1 = new LinkedHashSet();
                            cols1.add(colName);
                            cols1.addAll(queryDesc.used.getFirstIndexForTable(tableName).getColumns());
                            if (cols1.size() > 15) {
                                int l = 0;
                                for (Iterator iterator2 = cols1.iterator(); iterator2.hasNext();) {
                                    String s = (String) iterator2.next();
                                    if (l++ >= 15) iterator2.remove();
                                }
                            }
                            config1.addIndex(new Index(tableName, cols1));
                        }
                    }
                }

                if (!config1.isEmpty()) ap.implement_configuration(config1);
                System.out.println("config1 = " + config1);

                Plan plan = ap.optimizer_cost(queryDesc.parsed_query.toString());
                float cost = plan.getMateralizedViewAccessCost(view.getImplementedName());
                System.out.println("" + j + ":" + cost + ":" + view.getKey());
                queryDesc.mava_costs.put(view.getKey(), cost);

                if (!config1.isEmpty()) ap.drop_configuration(config1);
            }

            ap.drop_configuration(config);
        }

        saveMaterializedViewAccesses(workload, proc.query_descriptors);
    }

    public static void saveMaterializedViewAccesses(String workload, List query_descriptors) throws IOException {
        String filename = InumUtils.getMatViewAccessCostFile(workload);
        System.out.println("CostEstimator.saveMaterializedViewAccesses(" + filename + ")");
        XStream xstream = new XStream();
        GZIPOutputStream oos = new GZIPOutputStream(new FileOutputStream(filename));
        for (Iterator iterator = query_descriptors.iterator(); iterator.hasNext();) {
            QueryDesc desc = (QueryDesc) iterator.next();
            xstream.toXML(desc.mava_costs, oos);
        }
        oos.finish();
        oos.close();
    }

    public static void loadConfigEnumerations(String workload, List query_descriptors) throws IOException {
        String filename = InumUtils.getMatViewAccessCostFile(workload);
        System.out.println("CostEstimator.saveMaterializedViewAccesses(" + filename + ")");
        XStream xstream = new XStream();
        GZIPInputStream oos = new GZIPInputStream(new FileInputStream(filename));
        List list = (List) xstream.fromXML(oos);
        for (int i = 0; i < query_descriptors.size(); i++) {
            QueryDesc desc = (QueryDesc) query_descriptors.get(i);
            desc.mava_costs = (HashMap) list.get(i);                
        }
        oos.close();
    }
}

