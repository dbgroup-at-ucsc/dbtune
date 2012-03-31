package edu.ucsc.dbtune.bip;

import interaction.AnalysisMain;
import interaction.CandidateGenerationDBTune;
import interaction.Configuration;
import interaction.cand.Generation;
import interaction.ibg.AnalysisMode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OneColumnCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;

import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.interactions.InteractionBIP;
import edu.ucsc.dbtune.bip.interactions.InteractionOutput;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.BeforeClass;
import org.junit.Test;


import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.workload;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

/**
 * Test for the InteractionBIP class.
 *
 * @author Quoc Trung Tran
 */
public class InteractionComparisonFunctionalTest extends BIPTestConfiguration
{  
    private static DatabaseSystem db;
    private static Environment    en;    
    private static Workload       workload;
    private static String         folder;
    private static String         dbName;
    private static String         tableOwner;
    private static String         subFolder;
    
    private static Set<Index> oneColumnCandidates;  
    private static Set<Index> optimalCandidates;
    private static Set<Index> powerSetCandidates;
    
    public static int   MAX_NUM_INDEX = 300;
    public static List<Generation.Strategy> strategies = 
                    Arrays.asList( 
                            // Generation.Strategy.UNION_OPTIMAL
                                  //, Generation.Strategy.OPTIMAL_1C
                                  Generation.Strategy.POWER_SET
                                  );
    public static double[] deltas = new double[] {
                                    //0.01, 
                                    0.1, 
                                    //1.0
                                    };
    
    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp() throws Exception
    {
        
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
        subFolder = "tpcds-small";
        workload = workload(en.getWorkloadsFoldername() + subFolder);
        folder = en.getWorkloadsFoldername() + subFolder;
        dbName = "TEST";
        tableOwner = "TPCDS";
        
        /*
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
        subFolder = "tpch-small";
        workload = workload(en.getWorkloadsFoldername() + subFolder);
        folder = en.getWorkloadsFoldername() + subFolder;
        dbName = "TEST";
        tableOwner = "TPCH";
        */
    }
    
    /**     
     * 
     * @throws Exception
     *      if an IO error occurs or a DBMS communication failure occurs
     */
    @Test
    public void testInteraction() throws Exception
    {   
        // 1. generate candidate indexes
        generateCandidateIndexes();
        
        // 2. Ask Karl's to read from text file and write into his binary object
        for (Generation.Strategy s : strategies)  
            CandidateGenerationDBTune.readIndexesFromDBTune(s, dbName, tableOwner, subFolder);
        
        // 3. Call Analysis from Karl
        AnalysisMain.setWorkload(workload);
        AnalysisMain.runStepsINUM(tableOwner);
    
        /*
        InteractionOutput result;
        Set<IndexInteraction> ibg;
        
        for (Generation.Strategy s : strategies) 
            for (double delta : deltas) {
                result = analyze(s, delta);
                
                
                if (s.equals(Generation.Strategy.UNION_OPTIMAL))
                    ibg = readIBGInteraction(s, delta, optimalCandidates);
                else if (s.equals(Generation.Strategy.POWER_SET))
                    ibg = readIBGInteraction(s, delta, powerSetCandidates);
                else if (s.equals(Generation.Strategy.OPTIMAL_1C))
                    ibg = readIBGInteraction(s, delta, oneColumnCandidates);
                else
                    ibg = null;
                
                System.out.println("-- Threshold: --- " + delta
                                    + " : " + " f-measure: " + 
                                    result.f_measure(ibg));
                                    
            }
            */
    }
    
    /**
     * @throws Exception
     *      if fails
     */    
    private static InteractionOutput analyze(Generation.Strategy strategy, double delta) 
                                            throws Exception
    {         
        Set<Index>        candidates;
        InteractionOutput output;
        
        Optimizer      io;
        LogListener    logger;
        InteractionBIP bip;
        
        if (strategy.equals(Generation.Strategy.OPTIMAL_1C))
            candidates = new HashSet<Index>(oneColumnCandidates);
        else if (strategy.equals(Generation.Strategy.POWER_SET))
            candidates = new HashSet<Index>(powerSetCandidates);
        else 
            candidates = new HashSet<Index>(optimalCandidates);
        
        System.out.println("InteractionComparison, Number of indexes: " + candidates.size() + 
                           "Number of statements: " + workload.size());
        
        io = db.getOptimizer();
        if (!(io instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
            
        logger = LogListener.getInstance();
        bip    = new InteractionBIP(delta);
        
        bip.setCandidateIndexes(candidates);
        bip.setWorkload(workload);
        bip.setOptimizer((InumOptimizer) io);
        bip.setLogListenter(logger);
        bip.setConventionalOptimizer(io.getDelegate());
        bip.setApproximiationStrategy(true);
        
        output = (InteractionOutput) bip.solve();
        System.out.println("Number of interactions: " + output.size());
        //System.out.println(output);
        System.out.println(logger.toString());        
        
        return output;
    }
    
    
    private static Set<IndexInteraction> readIBGInteraction(Generation.Strategy s, double t,
            Set<Index> indexes) throws Exception
    {   
        Map<Integer, Index> mapIDIndex = new HashMap<Integer, Index>();
        Set<IndexInteraction> result   = new HashSet<IndexInteraction>();
        
        for (Index index : indexes)
            mapIDIndex.put(index.getId(), index);
        
        BufferedReader reader = new BufferedReader(
                                        new FileReader(Configuration.                           
                                                logInteractionFile(s, AnalysisMode.SERIAL, t)));
        String line = null;        
        int id1;
        int id2;
        
        while((line = reader.readLine()) != null) {         
            String[] token = line.split("\\|");
            
            // {id1, id2}
            id1 = Integer.parseInt(token[0]);
            id2 = Integer.parseInt(token[1]);
            
            if (!mapIDIndex.containsKey(id1) || !mapIDIndex.containsKey(id2))
                System.out.println("Runtime error. The inputs to the problems are not identical"
                                     + " id: " + id1 + " vs. " + id2);
            
            // Add interaction
            IndexInteraction pair = new IndexInteraction(mapIDIndex.get(id1),
                                                         mapIDIndex.get(id2));
            result.add(pair);
        }
        
        return result;
    }   
    /**
     * This function generates candidate indexes of different types, write into a text file
     * for Karl's code to read in.
     *  
     * @throws SQLException 
     */
    private static void generateCandidateIndexes() throws Exception
    {   
        CandidateGenerator candGen;
        
        // 1. one column
        if (strategies.contains(Generation.Strategy.OPTIMAL_1C)) {
            candGen = 
                new OneColumnCandidateGenerator(
                        new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer())));
            oneColumnCandidates = candGen.generate(workload);
            System.out.println("InteractionComparison, One column candidates: " + 
                            oneColumnCandidates.size());
            
            try {
                writeIndexesToFile(oneColumnCandidates, folder  + "/candidate-1C.txt");            
            } catch (Exception e) {
                throw e;
            }    
        }
        
        // 2. powerset
        if (strategies.contains(Generation.Strategy.POWER_SET)) {
            candGen = 
                new PowerSetOptimalCandidateGenerator(
                        new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer())), 3);
            powerSetCandidates = candGen.generate(workload);
            System.out.println("InteractionComparison, Power set candidates: " 
                                + powerSetCandidates.size());
            
            if (powerSetCandidates.size() > MAX_NUM_INDEX) {
                Set<Index> temp = getSubSetIndexes(powerSetCandidates, MAX_NUM_INDEX);
                powerSetCandidates = temp;
            }
            
            try {
                writeIndexesToFile(powerSetCandidates, folder  + "/candidate-powerset.txt");            
            } catch (Exception e) {
                throw e;
            }
        }
            
        // 3. optimal
        if (strategies.contains(Generation.Strategy.UNION_OPTIMAL)) {
            candGen = 
                new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
            optimalCandidates = candGen.generate(workload);
            System.out.println("InteractionComparison, Optimal candidates: " 
                                + optimalCandidates.size());
            
            try {
                writeIndexesToFile(optimalCandidates, folder  + "/candidate-optimal.txt");            
                System.out.println(" write to file: " + (folder + "/candidate-optimal.txt"));
            } catch (Exception e) {
                throw e;
            }
        }
    }
    
    
    /**
     * Write the set of indexes into the specified file in the format of:
     * {@code id|table_name|col_name|ASC}
     * 
     * @param indexes
     *            A set of indexes to be serialized into binary file
     * @param folder
     *            The folder of the file to be written
     * @param name
     *            The name of the file
     * 
     * @throws IOException
     *             when there is an error in creating files.
     */
    private static void writeIndexesToFile(Set<Index> indexes, String name) 
                        throws IOException
    {
        StringBuilder sb;
        PrintWriter   out;
          
        out = new PrintWriter(new FileWriter(name), false);
        
        for (Index idx : indexes) {
            sb = new StringBuilder();
            sb.append(idx.getId()).append("|")
              .append(idx.getTable().getName()).append("|");
            
            for (Column c : idx.columns())
                sb.append(c.getName()).append("|")
                        .append(idx.isAscending(c) ? "ASC" : "DESC")
                        .append("|");
            
            sb.delete(sb.length() - 1, sb.length() );
            out.println(sb.toString());
        }
        
        out.close();
    }
    
    
    
    /**
     * Extract a number of {@code maxSize} indexes that have the smallest ID values
     *  
     * @param maxSize
     *      A number of indexes
     * @return
     *      The set of indexes
     */
    private static Set<Index> getSubSetIndexes(Set<Index> indexes, int maxSize)
    {
        int count = 0;
        Set<Index> result = new HashSet<Index>();
        
        for (Index index : indexes) {
            result.add(index);
            count++;
            if (count >= maxSize)
                break;
        }
        
        return result;
    }
}