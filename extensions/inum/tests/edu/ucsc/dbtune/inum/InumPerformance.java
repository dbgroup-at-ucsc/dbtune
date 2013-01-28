package edu.ucsc.dbtune.inum;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.MetadataUtils;
import edu.ucsc.dbtune.util.OptimizerUtils;
import edu.ucsc.dbtune.util.TestUtils;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Performance test for testing non-dbms optimizers against the corresponding
 * dbms one. The optimizer being tested is specified by the
 * {@link edu.ucsc.dbtune.util.EnvironmentProperties#OPTIMIZER} property. The
 * base optimizer, i.e. the implementation of {@link Optimizer} that runs right
 * on top of the DBMS (e.g. {@link DB2Optimizer}) is retrieved through the
 * {@link #getBaseOptimizer} utility method.
 * 
 * @author Ivo Jimenez
 * @author Rui Wang
 */
public class InumPerformance
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer optimizer;
    private static Optimizer delegate;
    private static CandidateGenerator candGen;

    /**
     * @throws Exception
     *             if {@link #newDatabaseSystem} throws an exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db = DatabaseSystem.newDatabaseSystem(env);
        optimizer = db.getOptimizer();
        delegate = OptimizerUtils.getBaseOptimizer(optimizer);
        candGen = new OptimizerCandidateGenerator(delegate);
        if (!(optimizer instanceof edu.ucsc.dbtune.optimizer.InumOptimizer))
            throw new Error();
        if (!(delegate instanceof edu.ucsc.dbtune.optimizer.DB2Optimizer))
            throw new Error();

        TestUtils.loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *             if something goes wrong while closing the connection to the
     *             dbms
     */
    @AfterClass
    public static void afterClass() throws Exception
    {
        db.getConnection().close();
    }

    static class Conf
    {
        String name;
        Set<Index> indexs;
    }

    public Conf[] generatePowerSet(Index[] indices)
    {
        int count = indices.length;
        if (count > 31)
            throw new Error("overflow");
        int n = 1;
        for (int i = 0; i < count; i++)
            n *= 2;
        Vector<Conf> cs = new Vector<Conf>();
        for (int i = 0; i < n; i++) {
            HashSet<Index> vs = new HashSet<Index>();
            vs.clear();
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < count; j++) {
                if ((i & (1 << j)) != 0) {
                    if (sb.length() > 0)
                        sb.append(",");
                    sb.append("I" + j);
                    vs.add(indices[j]);
                }
            }
            Conf conf = new Conf();
            conf.name = sb.toString();
            conf.indexs = vs;
            cs.add(conf);
        }
        return cs.toArray(new Conf[cs.size()]);
    }

    private static void enumerateCartesianProduct(Index[][] indexes, int pos,
            Vector<Index> cur, String name, Vector<Conf> cs) {
        if (pos >= indexes.length) {
            Conf conf = new Conf();
            conf.name = name;
            HashSet<Index> vs = new HashSet<Index>();
            vs.addAll(cur);
            conf.indexs = vs;
            cs.add(conf);
            return;
        }
        int len = indexes[pos].length;
        enumerateCartesianProduct(indexes, pos + 1, cur, name, cs);
        for (int i = 0; i < len; i++) {
            int size = cur.size();
            cur.add(indexes[pos][i]);
            enumerateCartesianProduct(indexes, pos + 1, cur, name + ",I" + pos
                    + "-" + i, cs);
            cur.setSize(size);
        }
    }

    private static Conf[] cartesianProduct(Index[][] indexes) {
        Vector<Conf> cs = new Vector<Conf>();
        enumerateCartesianProduct(indexes, 0, new Vector<Index>(), "", cs);
        return cs.toArray(new Conf[cs.size()]);
    }

    /**
     * @throws Exception
     *             if something goes wrong
     * @see OptimizerTest#checkPreparedExplain
     */
    @Test
    public void testPreparedSQLStatement() throws Exception {
        if (delegate == null)
            return;

        List<SQLStatement> wl = TestUtils.workload(env.getWorkloadsFoldername()
                + "/tpch-cophy");
        final Set<Index> allRecommendedIndexes = candGen.generate(wl);

        System.out.println("Total indexes recommended by database: "
                + allRecommendedIndexes.size());

        int statementId = 1;
        long time;

        StringBuilder finalResult = new StringBuilder();
        PrintStream p = System.out;
        for (SQLStatement sql : wl) {
            System.out.println("------------------------------");
            System.out.println("Processing statement " + statementId++);
            p.println(sql.getSQL());

            time = System.nanoTime();
            InumPreparedSQLStatement pSql = (InumPreparedSQLStatement) optimizer
                    .prepareExplain(sql);
            long prepareTime = System.nanoTime() - time;

            List<Table> tablesReferencedInStmt = pSql.getTemplatePlans()
                    .toArray(new InumPlan[0])[0].getTables();
            Set<Index> referenceIndexes = MetadataUtils
                    .getIndexesReferencingTables(allRecommendedIndexes,
                            tablesReferencedInStmt);
            p.println("Total relevant indexes " + referenceIndexes.size()
                    + " as following");
            // Index[] indices = referenceIndexes
            // .toArray(new Index[referenceIndexes.size()]);
            Map<Table, Set<Index>> map = MetadataUtils
                    .getIndexesPerTable(referenceIndexes);
            Index[][] indexPerTable = new Index[map.size()][];
            int id = 0;
            for (Set<Index> set : map.values()) {
                indexPerTable[id++] = set.toArray(new Index[set.size()]);
            }
            for (int i = 0; i < indexPerTable.length; i++) {
                for (int j = 0; j < indexPerTable[i].length; j++) {
                    p.println("I" + i + "-" + j + ": "
                            + indexPerTable[i][j].toString());
                }
            }
            Conf[] configurations = cartesianProduct(indexPerTable);
            p.println("Total configurations: " + configurations.length);
            p.format("Inum prepare time %fs\n", prepareTime / 1000000000.0);
            p.println("inumCost,\tdb2Cost,\tdeviation,\t"
                    + "inumTime,\tdb2Time,\tconfiguration");
            double totalDeviation = 0;
            double totalInumTime = 0;
            double totalDb2Time = 0;
            int totalConfigurations = 0;
            for (Conf c : configurations) {
                time = System.nanoTime();
                double inumCost = pSql.explain(c.indexs).getSelectCost();
                long inumTime = System.nanoTime() - time;

                time = System.nanoTime();
                double db2Cost = delegate.explain(sql, c.indexs)
                        .getSelectCost();
                long db2Time = System.nanoTime() - time;
                p.format("%f,\t%f,\t%f,\t%.5f,\t%.5f,\t\"%s\"\n", inumCost,
                        db2Cost, inumCost / db2Cost, inumTime / 1000000000.0,
                        db2Time / 1000000000.0, c.name);
                totalDeviation += inumCost / db2Cost;
                totalInumTime += inumTime;
                totalDb2Time += db2Time;
                totalConfigurations++;
                if (System.in.available() > 0)
                    break;
            }
            p.println();
            p.format("inumPrepare=%fs\tavgInumTime=%fs\t"
                    + "avgDb2Time=%fs\tavgDeviation=%f\tconfs=%d\n",
                    prepareTime / 1000000000.0, totalInumTime
                            / totalConfigurations / 1000000000.0, totalDb2Time
                            / totalConfigurations / 1000000000.0,
                    totalDeviation / totalConfigurations, totalConfigurations);
            finalResult.append(String.format(
                    "%f,\t %f,\t" + "%f,\t %f,\t %d\n",
                    prepareTime / 1000000000.0, totalInumTime
                            / totalConfigurations / 1000000000.0, totalDb2Time
                            / totalConfigurations / 1000000000.0,
                    totalDeviation / totalConfigurations, totalConfigurations));
            if (System.in.available() > 0)
                break;
        }
        p
                .println("inumPrepare\t avgInumTime\t avgDb2Time\t avgDeviation\t confs");
        p.println(finalResult);
    }
}
