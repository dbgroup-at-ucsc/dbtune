package edu.ucsc.dbtune.tools.cmudb.commons;

import Zql.ParseException;
import edu.ucsc.dbtune.tools.cmudb.autopilot.autopilot;
import edu.ucsc.dbtune.tools.cmudb.model.WorkloadProcessor;

/**
 * a set of helper methods that will allow me pre-configure the
 * autopilot and workload processor objects (initially). In the
 * future, other objects could be configured here.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Initializers {
  private Initializers(){}

  public static autopilot initializeAutopilot(){
    final autopilot autopilot = new autopilot();
    autopilot.init_database();
    return autopilot;
  }

  public static WorkloadProcessor initializeWorkloadProcessor(String workloadFile)
      throws ParseException {
      //C: Workloadprocessor: is set all queries, and all the interesting indexes for each query,
      // and the universe for all queries from the workload

      WorkloadProcessor processor = new WorkloadProcessor(workloadFile);
      processor.generateCandidateIndexes(); //indexes per query
      processor.generateUniverse();         //generate all interesting indexes in all queries select where
      processor.getInterestingOrders();
      return processor;

  }

}
