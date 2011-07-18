package edu.ucsc.dbtune.tools.cmudb.mathprog;

import Zql.ParseException;
import edu.ucsc.dbtune.tools.cmudb.autopilot.autopilot;
import edu.ucsc.dbtune.tools.cmudb.commons.Initializers;
import edu.ucsc.dbtune.tools.cmudb.commons.Utils;
import edu.ucsc.dbtune.tools.cmudb.greedy.GreedyResult;
import edu.ucsc.dbtune.tools.cmudb.inum.PostgresEnumerationGenerator;
import edu.ucsc.dbtune.tools.cmudb.inum.PostgresIndexAccessGenerator;
import edu.ucsc.dbtune.tools.cmudb.model.PhysicalConfiguration;
import ilog.concert.IloException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;

/**
 * (C) DIAS Lab and LMC Lab., Ecole Polytechnic Federale du Lausanne
 * All rights reserved. Do not distribute the source code without explicit permission of the copyright owners.
 * <p/>
 * User: dash
 * Date: May 12, 2010
 * Time: 4:00:44 PM
 */
public class CoPhy {
  private final autopilot autopilot;
  public CoPhy(autopilot autopilot){
    this.autopilot = autopilot;
  }

  public GreedyResult run(String workloadName, CophyConstraints constr, LogListener listener) throws ParseException,
      IOException, IloException, SQLException {
        listener.onLogEvent(LogListener.COPHY, "STARTING");
        String args[] = new String[] { workloadName };
        PostgresEnumerationGenerator.main(args);
        PostgresIndexAccessGenerator.main(args);
        ProgGenerator gen = new ProgGenerator(autopilot, workloadName, Utils.getPagesFromMBs(constr.indexSize), constr.maxIndexWidth);
        gen.build(listener);
        String cplexFile = workloadName + ".cplex";
        String binFile = workloadName + ".bin";
        String consFile = workloadName + ".cons";
        listener.onLogEvent(LogListener.COPHY, "" + countLines(binFile) + " variables in the program");
        listener.onLogEvent(LogListener.COPHY, "" + countLines(consFile) + " constraints in the program");
        concat(cplexFile, workloadName+".obj", consFile, binFile);
        CPlex cplex = new CPlex(cplexFile, constr);
        cplex.run();
        listener.onLogEvent("CPLEX", "Solved the optimization problem");
        PhysicalConfiguration config = cplex.getTotalConfiguration();
        listener.onLogEvent("CPLEX", "Parsed the generated solutions");
        GreedyResult ret = cplex.processGeneratedIndexes(workloadName);
        listener.onLogEvent(LogListener.COPHY, "FINISHED");
        return ret;
  }


    public static void main(String[] arg) throws IOException, ParseException, SQLException, IloException {
        String workloadName = arg[0];
        GreedyResult res = runCoPhy(workloadName, CophyConstraints.getDefaultConstraints(), new DefaultLogger());
    }

    public static List<ParetoPoint> runSoftCophy(String workloadName, CophyConstraints constraints) throws IOException, ParseException, IloException {
        LogListener listener = new LogListener() {
            public void onLogEvent(String component, String logEvent) {
                //tocode
            }
        };

        String args[] = new String[] { workloadName };
        PostgresEnumerationGenerator.main(args);
        PostgresIndexAccessGenerator.main(args);
        ProgGenerator gen = new ProgGenerator(workloadName, Utils.getPagesFromMBs(constraints.indexSize), constraints.maxIndexWidth);
        gen.build(listener);
        String cplexFile = workloadName + ".cplex";
        String binFile = workloadName + ".bin";
        String consFile = workloadName + ".cons";
        concat(cplexFile, workloadName+".obj", consFile, binFile);
        CPlex cplex = new CPlex(cplexFile, constraints);
        return cplex.runSoft();
    }

     // todo.Huascar...make this adaptable so that it could be used in dbtune.
     // todo..Huascar... this is the example that I need in order to implement INUM.
    public static GreedyResult runCoPhy(String workloadName, CophyConstraints constr, LogListener listener) throws ParseException, IOException, IloException, SQLException {
      return new CoPhy(Initializers.initializeAutopilot()).run(workloadName, constr, listener);
    }

    public static int countLines(String file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        int count = 0;
        String line = null;
        while((line = reader.readLine()) != null) {
            count++;
        }
        reader.close();

        return count;
    }
    
    public static void concat(String target, String... files) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(target));
        for (int i = 0; i < files.length; i++) {
            String file = files[i];
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while((line = reader.readLine()) != null) {
                writer.println(line);
            }
            reader.close();
        }
        writer.close();
    }
}