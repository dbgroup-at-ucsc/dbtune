package edu.ucsc.dbtune.core;

import static edu.ucsc.dbtune.core.JdbcConnectionManager.makeDatabaseConnectionManager;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.SQLScriptExecuter;
import edu.ucsc.dbtune.util.Strings;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * InumWhatIfOptimizer's Functional Test
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumWhatIfOptimizerTestFunctional {
  static DatabaseConnection CONNECTION;
  static final Environment        ENV;
  static {
    ENV        = Environment.getInstance();
    try {
      CONNECTION = makeDatabaseConnectionManager(ENV.getAll()).connect();
    } catch (SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Ignore @Test public void testInum_EmptyHypotheticalIndexes() throws Exception {
    final InumWhatIfOptimizer optimizer = new InumWhatIfOptimizerImpl(CONNECTION);
    final String workloadName = ENV.getScriptAtWorkloadsFolder(
        "one_table/candidate_set_bootstrap_workload.sql");
    final String oneLiner     = Strings.wholeContentAsSingleLine(new File(workloadName));
    final double result       = optimizer.estimateCost(oneLiner);
  }

  @BeforeClass public static void setUp() throws Exception {
    final File outputdir   = new File(ENV.getOutputFoldername() + "/one_table");
    String ddlfilename     = ENV.getScriptAtWorkloadsFolder("one_table/create.sql");

    if(outputdir.mkdirs())  { Console.streaming().info(outputdir.toString() + " has been created.");}
    else                    { Console.streaming().info(outputdir.toString() + " hasn't been created.");}
    
    SQLScriptExecuter.execute(CONNECTION.getJdbcConnection(), ddlfilename);
    CONNECTION.getJdbcConnection().setAutoCommit(false);
  }

  @AfterClass public static void tearDown() throws Exception {
    if(CONNECTION != null) {
      if(CONNECTION.isOpened()){
        CONNECTION.close();
      }

      CONNECTION = null;
    }
  }

}
