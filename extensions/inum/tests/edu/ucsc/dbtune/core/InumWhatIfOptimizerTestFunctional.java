package edu.ucsc.dbtune.core;

import com.google.common.io.Files;
import static edu.ucsc.dbtune.core.JdbcConnectionManager.makeDatabaseConnectionManager;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.spi.core.Console;
import java.io.File;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * InumWhatIfOptimizer's Functional Test
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumWhatIfOptimizerTestFunctional {
  static DatabaseConnection       CONNECTION;
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
    try {
      CONNECTION = makeDatabaseConnectionManager(ENV.getAll()).connect();
    } catch (SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Test public void testInum_EmptyHypotheticalIndexes() throws Exception {
    final InumWhatIfOptimizer optimizer = new InumWhatIfOptimizerImpl(CONNECTION);
    optimizer.estimateCost(extractFilename(WORKLOAD_IN_USE));
  }

  @BeforeClass public static void setUp() throws Exception {
    final File    outputdir    = new File(DESTINATION);
    final File    twinWorkload = new File(WORKLOAD_IN_USE);

    if(outputdir.mkdirs())  { Console.streaming().info(outputdir.toString() + " has been created.");}
    else                    { Console.streaming().info(outputdir.toString() + " already exists.");}


    Files.copy(new File(WORKLOAD_PATH), twinWorkload);
  }

  @AfterClass public static void tearDown() throws Exception {
    if(CONNECTION != null) {
      if(CONNECTION.isOpened()){
        CONNECTION.close();
      }

      CONNECTION = null;
    }
  }


  private static String extractFilename(String fullname){
    final Pattern p = Pattern.compile(".*?([^\\\\/]+)$");
    final Matcher m = p.matcher(fullname);
    return (m.find()) ? m.group(1) : "";

  }

}
