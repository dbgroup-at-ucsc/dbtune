package edu.ucsc.dbtune.bip;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.Generation;
import edu.ucsc.dbtune.advisor.candidategeneration.OneColumnCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;

import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.interactions.InteractionBIP;
import edu.ucsc.dbtune.bip.interactions.InteractionOutput;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;


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
    
    private static Workload   workload;
    private static Set<Index> oneColumnCandidates;  
    private static Set<Index> optimalCandidates;
    private static Set<Index> powerSetCandidates;
    
    
    /**
     * The test has to check first just one query and one index.     
     * 
     * @throws Exception
     *      if an I/O error occurs or a DBMS communication failure occurs
     */
    @Test
    public void testInteraction() throws Exception
    {
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
        workload = workload(en.getWorkloadsFoldername() + "/tpch-small");
        
        double[] deltas = new double[] {0.01, 0.1, 1};
        
        // 1. one column        
        CandidateGenerator candGen = 
            new OneColumnCandidateGenerator(
                    new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer())));
        oneColumnCandidates = candGen.generate(workload);
        
        
        // 2. powerset
        candGen = 
            new PowerSetOptimalCandidateGenerator(
                    new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer())), 3);
        powerSetCandidates = candGen.generate(workload);
        
                
        // 3. optimal
        candGen = 
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        optimalCandidates = candGen.generate(workload);
        
        InteractionOutput result;
        Set<IndexInteraction> ibg;
        
        // Optimal
        for (double delta : deltas) {
            result = analyze(Generation.Strategy.UNION_OPTIMAL, delta);
            ibg    = readIBGInteraction(
                            Generation.Strategy.UNION_OPTIMAL, delta, optimalCandidates);
            System.out.println("-- Threshold: --- " + delta
                                + " : " + " f-measure: " + 
                                result.f_measure(ibg));
        }
        
        // powerset
        for (double delta : deltas) {
            result = analyze(Generation.Strategy.POWER_SET, delta);
            ibg    = readIBGInteraction(
                            Generation.Strategy.POWER_SET, delta, optimalCandidates);
            System.out.println("-- Threshold: --- " + delta
                                + " : " + " f-measure: " + 
                                result.f_measure(ibg));
        }
    }
    
    /**
     * @throws Exception
     *      if fails
     */    
    public static InteractionOutput analyze(Generation.Strategy strategy, double delta) 
                                    throws Exception
    {   
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
        
        workload = workload(en.getWorkloadsFoldername() + "/tpch-small");
        
        Set<Index> candidates;
        
        if (strategy.equals(Generation.Strategy.OPTIMAL_1C))
            candidates = new HashSet<Index>(oneColumnCandidates);
        else if (strategy.equals(Generation.Strategy.POWER_SET))
            candidates = new HashSet<Index>(powerSetCandidates);
        else 
            candidates = new HashSet<Index>(optimalCandidates);
        
        if (candidates.size() >= 400) {
            Set<Index> temp = new HashSet<Index>();
            int count = 0;
            for (Index index : candidates) {
                temp.add(index);
                count++;
                if (count >= 400)
                    break;
            }
            candidates = temp;
        }
        
        System.out.println("Number of indexes: " + candidates.size() + 
                           "Number of statements: " + workload.size());
        
        //for (Index index : candidates) 
          //  System.out.println("Index: " + index.getId() + " " + index);
        
        InteractionOutput output;
        
        Optimizer io = db.getOptimizer();

        if (!(io instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
            
        LogListener logger = LogListener.getInstance();
        InteractionBIP bip = new InteractionBIP(delta);            
        bip.setCandidateIndexes(candidates);
        bip.setWorkload(workload);
        bip.setOptimizer((InumOptimizer) io);
        bip.setLogListenter(logger);
        bip.setConventionalOptimizer(io.getDelegate());
        
        output = (InteractionOutput) bip.solve();
        System.out.println(output.toString());
        System.out.println(logger.toString());        
        
        return output;
    }
    
    private static Set<IndexInteraction> readIBGInteraction(Generation.Strategy s, double t,
                                         Set<Index> candidates) throws Exception
    {
        // create a map for candidate indexes
        Map<Integer, Index> mapIDIndex = new HashMap<Integer, Index>();
        
        for (Index index : candidates)
            mapIDIndex.put(index.getId(), index);
        
        BufferedReader reader = new BufferedReader(
                                        new FileReader(CompareIBGConfiguration.                           
                                                logInteractionFile(s, t)));
        String line = null;        
        int id1;
        int id2;
        
        Set<IndexInteraction> result = new HashSet<IndexInteraction>();
        
        while((line = reader.readLine()) != null) {         
            String[] token = line.split("\\|");
            
            // {id1, id2}
            id1 = Integer.parseInt(token[0]);
            id2 = Integer.parseInt(token[1]);
            
            if (!mapIDIndex.containsKey(id1) || !mapIDIndex.containsKey(id2))
                continue;
            
            // Add interaction
            IndexInteraction pair = new IndexInteraction(mapIDIndex.get(id1),
                                                         mapIDIndex.get(id2));
            result.add(pair);
        }
        
        return result;
    }   
}