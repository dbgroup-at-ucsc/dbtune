package edu.ucsc.dbtune.inum;

import Zql.ParseException;
import com.google.common.collect.HashMultimap;
import edu.ucsc.dbtune.inum.autopilot.autopilot;
import edu.ucsc.dbtune.inum.commons.Initializers;
import edu.ucsc.dbtune.inum.model.Index;
import edu.ucsc.dbtune.inum.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.model.Plan;
import edu.ucsc.dbtune.inum.model.QueryDesc;
import edu.ucsc.dbtune.inum.model.WorkloadProcessor;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * (C) DIAS Lab and LMC Lab., Ecole Polytechnic Federale du Lausanne
 * All rights reserved. Do not distribute the source code without explicit permission of the copyright owners.
 * <p/>
 * User: dash
 * Date: Nov 21, 2009
 * Time: 8:23:04 PM
 */
public class PlanCollector {
    private autopilot AP;
    public WorkloadProcessor processor;

    public PlanCollector(autopilot autopilot, WorkloadProcessor processor) {
      this.AP         = autopilot;
      this.processor  = processor;
    }

    public PlanCollector(String workloadFile) throws IOException, ParseException {
      this(Initializers.initializeAutopilot(), Initializers.initializeWorkloadProcessor(workloadFile));
    }

    public void getAccuracyForQuery(QueryDesc desc, int numTests) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter("pgsql.plans.txt", true));
        writer.println("query: " + desc.queryString);
        int id = processor.query_descriptors.indexOf(desc);
        List<Index> candidates = new ArrayList(processor.candidates);
        HashMultimap multiMap = HashMultimap.create();
        int candidateSize = 0;

        for (Iterator<Index> iterator = candidates.iterator(); iterator.hasNext();) {
            Index index = iterator.next();
            if(desc.getUsedTableNames().contains(index.getTableName())) {
                multiMap.put(index.getTableName(), index);
                candidateSize ++;
            }
        }
        System.out.println("candidates = " + candidateSize);

        ArrayList input = new ArrayList();
        for (Iterator iterator = multiMap.keySet().iterator(); iterator.hasNext();) {
            String tableName = (String) iterator.next();

            input.add(multiMap.get(tableName));
        }

        CollectionEnumerator enumerator = new CollectionEnumerator(input);
        List state = null;
        while ((state = enumerator.next()) != null) {
            PhysicalConfiguration config = new PhysicalConfiguration();
            for (int i = 0; i < state.size(); i++) {
                Index idx = (Index) state.get(i);
                config.addIndex(idx);
            }

            AP.implement_configuration(config);
            AP.prepareQueryForConfiguration(desc, config, false, true);
            Plan plan = AP.optimizer_cost(desc.queryString, false, true);
            AP.removeQueryPreparation(desc);
            AP.drop_configuration(config);
            writer.println("config: " + config);
            writer.println(plan);
            writer.println();
            // System.out.println("cost: " + Join.join(",", id, cost, cost1));
            numTests--;


            if (numTests <= 0) {
                writer.close();
                break;
            }
        }
        if(numTests > 0) {
            System.out.println("The total tests remaining: " + numTests);
        }
    }

    public void testAccuracy() throws IOException {
        for (Iterator<QueryDesc> iterator = processor.query_descriptors.iterator(); iterator.hasNext();) {
            QueryDesc queryDesc = iterator.next();
            getAccuracyForQuery(queryDesc, 100000);
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        PlanCollector ab = new PlanCollector("synth.sql");
        long time = System.currentTimeMillis();
        ab.testAccuracy();
        System.out.println("(System.currentTimeMillis() - time) = " + (System.currentTimeMillis() - time));
        ab.AP.dispose();
    }
}

