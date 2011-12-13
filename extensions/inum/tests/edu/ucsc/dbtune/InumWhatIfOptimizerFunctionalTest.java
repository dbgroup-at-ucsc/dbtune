package edu.ucsc.dbtune;

import com.google.common.io.Files;

import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.spi.Console;
import java.io.File;
import java.sql.Connection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * InumWhatIfOptimizer's Functional Test
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumWhatIfOptimizerFunctionalTest {
  static Connection       CONNECTION;
  static final Environment        ENV;
  static final String             WORKLOAD_PATH;
  static final String             FILE_NAME       = "workload.sql";
  static final String             DESTINATION;
  static final String             WORKLOAD_IN_USE;
  static {
    ENV                 = Environment.getInstance();
    WORKLOAD_PATH       = ENV.getScriptAtWorkloadsFolder("inum/" + FILE_NAME);
    DESTINATION         = ENV.getInumCacheDeploymentDir() + "/";
    WORKLOAD_IN_USE     = DESTINATION + FILE_NAME;

  }

  @Test public void testUseInumToEstimateCostOfWorload_WithHypotheticalIndexes() throws Exception {
    //final InumWhatIfOptimizer optimizer = new InumWhatIfOptimizerImpl(CONNECTION);
    //final String              workload  = WORKLOAD_IN_USE;
    //todo(Huuascar) uncomment this out once the dbms changes are complete.
//    final Iterable<DBIndex>   candidates = configureCandidates();
//    optimizer.estimateCost(workload, candidates);
  }

        /*
  private static Iterable<Index> configureCandidates() {
    final IndexExtractor extractor = CONNECTION.getIndexExtractor();
    final File workloadFile = new File(WORKLOAD_IN_USE);
    try {
      //todo(Huascar) the extractor is to constructing PGTables property...
      // we need the table name and the columns....WE NEED THAT
      // the problem is that PGTable state after DatabaseObject and
      // AbstractDatabase is broken

      final Iterable<DBIndex> candidates = extractor.recommendIndexes(
          Strings.wholeContentAsSingleLine(workloadFile)
      );
      return ImmutableList.copyOf(candidates);
    } catch (Exception e){
      return ImmutableList.of();
    }
  }
      */

  @BeforeClass public static void setUp() throws Exception {
    final File    outputdir    = new File(DESTINATION);
    final File    twinWorkload = new File(WORKLOAD_IN_USE);

    if (outputdir.mkdirs())  { Console.streaming().info(outputdir.toString() + " has been created.");}
    else                    { Console.streaming().info(outputdir.toString() + " already exists.");}


    Files.copy(new File(WORKLOAD_PATH), twinWorkload);
  }

  @AfterClass public static void tearDown() throws Exception {
    if (CONNECTION != null) {
      if (!CONNECTION.isClosed()){
        CONNECTION.close();
      }

      CONNECTION = null;
    }
  }

}
