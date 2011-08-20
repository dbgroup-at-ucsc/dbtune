package edu.ucsc.dbtune;

import edu.ucsc.dbtune.advisor.BenefitFunction;
import edu.ucsc.dbtune.advisor.DoiFunction;
import edu.ucsc.dbtune.ibg.IBGBestBenefitFinder;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.InteractionBank;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.DB2Index;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.PGIndex;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Instances;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static edu.ucsc.dbtune.spi.EnvironmentProperties.*;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class DBTuneInstances {
    public static final String DB_NAME       = "superDB";
    public static final String SCHEMA_NAME   = "superSchema";
    public static final String TABLE_NAME    = "R";
    public static final String TABLE_CREATOR = "123";

    /**
     * Utility class
     */
    private DBTuneInstances(){}

    /**
     * Returns a configuration for DB2
     */
    public static Properties configureDB2()
    {
        Properties cfg = new Properties(configureGeneric());

        cfg.setProperty(JDBC_DRIVER, DB2);

        return cfg;
    }

    /**
     * Returns a configuration for an inexistent DBMS
     */
    public static Properties configureInexistentDBMS()
    {
        Properties cfg = new Properties(configureGeneric());

        cfg.setProperty(JDBC_DRIVER, "edu.ucsc.dbtune.superduperdbms");

        return cfg;
    }

    /**
     * Returns a configuration for postgres
     */
    public static Properties configureMySQL()
    {
        Properties cfg = new Properties(configureGeneric());

        cfg.setProperty(JDBC_DRIVER, MYSQL);

        return cfg;
    }

    /**
     * Returns a configuration for postgres
     */
    public static Properties configurePG()
    {
        Properties cfg = new Properties(configureGeneric());

        cfg.setProperty(JDBC_DRIVER, PG);

        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Properties configureGeneric()
    {
        Properties cfg = new Properties();

        cfg.setProperty(URL,      "nothing.com");
        cfg.setProperty(USERNAME, "neo");
        cfg.setProperty(PASSWORD, "neopassword");

        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Properties configureDBMSOptimizer(Properties cfg)
    {
        cfg.setProperty(OPTIMIZER, DBMS);
        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Properties configureIBGOptimizer(Properties cfg)
    {
        cfg.setProperty(OPTIMIZER, IBG);
        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Properties configureINUMOptimizer(Properties cfg)
    {
        cfg.setProperty(OPTIMIZER, INUM);
        return cfg;
    }

    public static IndexBenefitGraph.IBGNode makeRandomIBGNode()
    {
        return makeIBGNode(new Random().nextInt());
    }

    public static IndexBenefitGraph.IBGNode makeIBGNode(int id){
        try {
            final Constructor<IndexBenefitGraph.IBGNode> c = IndexBenefitGraph.IBGNode.class.getDeclaredConstructor(IndexBitSet.class, int.class);
            c.setAccessible(true);
            return c.newInstance(new IndexBitSet(), id);
        } catch (Exception e) {
            throw new IllegalStateException("ERROR: unable to construct an IBGNode");
        }
    }

    public static Index newPGIndex(int indexId, int schemaId, List<Column> cols, List<Boolean> desc) throws Exception {
        return new PGIndex(schemaId, true, cols, desc, indexId, 3.0, 4.5, "Create");
    }

    public static List<Column> generateColumns(int howmany){
        final List<Column> cols = Instances.newList();
        for(int idx = 0; idx < howmany; idx++){
            cols.add(new Column(idx));
        }
        return cols;
    }

    public static List<Boolean> generateDescVals(int howmany){
        final List<Boolean> cols = Instances.newList();
        for(int idx = 0; idx < howmany; idx++){
            cols.add(true);
        }
        return cols;
    }

    public static Index newDB2Index() throws Exception {
        return new DB2Index(DB_NAME, TABLE_NAME, TABLE_CREATOR, new ArrayList<String>(), new ArrayList<Boolean>(), "U", "N", "REG", 1, "no idea", "no idea", "N", 2, 5.0, 1.0);
    }

    public static PGIndex newPGIndex(final int id) throws Exception {
        return new PGIndex(123456, new Random().nextBoolean(), new ArrayList<Column>(), new ArrayList<Boolean>(), id, 0.0, 0.0, "");
    }

    public static PGIndex newPGIndex(final int schemaId, final int id) throws Exception {
        return new PGIndex(schemaId, new Random().nextBoolean(), new ArrayList<Column>(), new ArrayList<Boolean>(), id, 0.0, 0.0, "");
    }

    public static PGIndex newPGIndex(final boolean flag, final int schemaId, final int id) throws Exception {
        return new PGIndex(schemaId, flag, new ArrayList<Column>(), new ArrayList<Boolean>(), id, 0.0, 0.0, "");
    }

    public static PGIndex newPGIndex() throws Exception {
        return newPGIndex(1);
    }

  public static BenefitFunction newTempBenefitFunction(List<IBGPreparedSQLStatement> qinfos, int maxInternalId){
    return new TempBenefitFunction(qinfos, maxInternalId);
  }

    private static class TempBenefitFunction implements BenefitFunction {
        IBGBestBenefitFinder finder = new IBGBestBenefitFinder();
        double[][] bbCache;
        double[] bbSumCache;
        int[][] componentId;
        IndexBitSet[] prevM;
        IndexBitSet diffM;
        List<IBGPreparedSQLStatement> qinfos;

        TempBenefitFunction(List<IBGPreparedSQLStatement> qinfos0, int maxInternalId) {
            qinfos = qinfos0;

            componentId = componentIds(qinfos0, maxInternalId);

            bbCache = new double[maxInternalId+1][qinfos0.size()];
            bbSumCache = new double[maxInternalId+1];
            prevM = new IndexBitSet[maxInternalId+1];
            for (int i = 0; i <= maxInternalId; i++) {
                prevM[i] = new IndexBitSet();
                reinit(i, prevM[i]);
            }
            diffM = new IndexBitSet(); // temp bit set
        }

        private static int[][] componentIds(List<IBGPreparedSQLStatement> qinfos, int maxInternalId) {
            int[][] componentId = new int[qinfos.size()][maxInternalId+1];
            int q = 0;
            for (IBGPreparedSQLStatement qinfo : qinfos) {
                IndexBitSet[] parts = qinfo.getInteractionBank().stablePartitioning(0);
                for (Index index : qinfo.getConfiguration()) {
                    int id = index.getId();
                    componentId[q][id] = -id;
                    for (int p = 0; p < parts.length; p++) {
                        if (parts[p].get(id)) {
                            componentId[q][id] = p;
                            break;
                        }
                    }
                }
                ++q;
            }
            return componentId;
        }

        private void reinit(int id, IndexBitSet M) {
            int q = 0;
            double ben = 0;
            double cache[] = bbCache[id];
            for (IBGPreparedSQLStatement qinfo : qinfos) {
                double bb = finder.bestBenefit(qinfo.getIndexBenefitGraph(), id, M);
                cache[q] = bb;
                ben += bb;
                ++q;
            }
            bbSumCache[id] = ben;
            prevM[id].set(M);
        }

        private void reinitIncremental(int id, IndexBitSet M, int b) {
            int q = 0;
            double ben = 0;
            double cache[] = bbCache[id];
            for (IBGPreparedSQLStatement qinfo : qinfos) {
                if (componentId[q][id] == componentId[q][b]) {
                    // interaction, recompute
                    double bb = finder.bestBenefit(qinfo.getIndexBenefitGraph(), id, M);
                    cache[q] = bb;
                    ben += bb;
                }
                else
                    ben += cache[q];
                ++q;
            }
            prevM[id].set(M);
            bbSumCache[id] = ben;
        }

        public double apply(Index a, IndexBitSet M) {
            int id = a.getId();
            if (!M.equals(prevM)) {
                diffM.set(M);
                diffM.xor(prevM[id]);
                if (diffM.cardinality() == 1) {
                    reinitIncremental(id, M, diffM.nextSetBit(0));
                }
                else {
                    reinit(id, M);
                }
            }
            return bbSumCache[id];
        }
    }

  public static DoiFunction newTempDoiFunction(List<IBGPreparedSQLStatement> qinfos, Configuration candidateSet){
    return new TempDoiFunction(qinfos, candidateSet);
  }

  private static class TempDoiFunction implements DoiFunction {
      private InteractionBank bank;
      TempDoiFunction(List<IBGPreparedSQLStatement> qinfos, Configuration candidateSet) {
          bank = new InteractionBank(1024);
          for (Index a : candidateSet) {
              int id_a = a.getId();
              for (Index b : candidateSet) {
                  int id_b = b.getId();
                  if (id_a < id_b) {
                      double doi = 0;
                      for (IBGPreparedSQLStatement qinfo : qinfos) {
                          doi += qinfo.getInteractionBank().interactionLevel(a.getId(), b.getId());
                      }
                      bank.assignInteraction(a.getId(), b.getId(), doi);
                  }
              }
          }
      }

      public double apply(Index a, Index b) {
          return bank.interactionLevel(a.getId(), b.getId());
      }
  }
}
