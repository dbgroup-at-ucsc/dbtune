package edu.ucsc.dbtune.inum.commons;

import Zql.ParseException;
import edu.ucsc.dbtune.inum.EnumerationGenerator;
import edu.ucsc.dbtune.inum.IndexAccessGenerator;
import edu.ucsc.dbtune.inum.InumUtils;
import edu.ucsc.dbtune.inum.PostgresEnumerationGenerator;
import edu.ucsc.dbtune.inum.PostgresIndexAccessGenerator;
import edu.ucsc.dbtune.inum.autopilot.autopilot;
import edu.ucsc.dbtune.inum.model.Plan;
import edu.ucsc.dbtune.inum.model.QueryDesc;
import edu.ucsc.dbtune.inum.model.WorkloadProcessor;
import edu.ucsc.dbtune.util.Checks;
import java.io.File;
import java.io.IOException;

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

  public static WorkloadProcessor startInumCache(String workloadFile, WorkloadProcessor workloadProcessor, autopilot autopilot) throws ParseException,
      IOException {

    WorkloadProcessor processor = Checks.checkNotNull(workloadProcessor);
    if (InumUtils.isIndexAccessCostFileAvailable(workloadFile)) {
      PostgresIndexAccessGenerator.loadIndexAccessCosts(workloadFile, processor.query_descriptors);
    } else /*todo(huascar) confirm that this is okay. it looks to me that it should be generated
             so the next time we create the cost estimator the file will get loaded. I should confirm
             this will Alkis. */ {
      PostgresIndexAccessGenerator.runPostgresIndexAccessGenerator(autopilot, processor, workloadFile);
    }

    IndexAccessGenerator.loadIndexAccessCosts(workloadFile, processor.query_descriptors);
    autopilot.setIndexSizeMap(IndexAccessGenerator.loadIndexSizes(workloadFile));

    if (InumUtils.isEnumerationFileAvailable(workloadFile)) {
      EnumerationGenerator.loadConfigEnumerations(workloadFile, processor.query_descriptors);
    } else {
      PostgresEnumerationGenerator generator = new PostgresEnumerationGenerator();
      generator.AllEnumerateConfigs(processor.query_descriptors, autopilot);
      EnumerationGenerator.saveConfigEnumerations(workloadFile, processor.query_descriptors);
    }

    // test the validity of plan computation.
    for (QueryDesc queryDesc : processor.query_descriptors) {
      for (Object o : queryDesc.plans.values()) {
        Plan plan = (Plan) o;
        float cost = plan.getInternalCost();
        for (String table : queryDesc.getUsedTableNames()) {
          cost += plan.getAccessCost(table);
        }

        if (Math.abs(cost - plan.getTotalCost()) / cost > 0.01) {
          System.out.println("cost = " + cost + ", expected: " + plan.getTotalCost());
        }
      }
    }

    return processor;
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
