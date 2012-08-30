package edu.ucsc.dbtune.divgdesign;


import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.divgdesign.CoPhyDivgDesign;

import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.DivTestSetting;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;

/**
 * Test the usage of CoPhy in DivgDesign
 * 
 * @author Quoc Trung Tran
 *
 */
public class CoPhyDivgDesignFunctionalTest extends DivTestSetting
{   
    private static int maxIters;
    private static Map<SQLStatement, Set<Index>> recommendedIndexStmt;
    private static CoPhyDivgDesign divg;
    
    static class StatementCandidates implements Serializable
    {
        private static final long serialVersionUID = 1L;
        
        SQLStatement sql;
        Set<Index> candidates;
     
        StatementCandidates(SQLStatement _sql, Set<Index> _candidates)
        {
            sql = _sql;
            candidates = new HashSet<Index>(_candidates);
        }
    }
    
    
    
    /**
     * Run the CoPhyDiv algorithm.
     * 
     * @throws Exception
     */
    public static double testDivgDesign(int _n, double _B) throws Exception
    {
        // 1. Generate candidate indexes
        generateOptimalCandidatesCoPhy();
        maxIters = 5;
        
        B = _B;
        nReplicas = _n;
        loadfactor = (int) Math.ceil( (double) nReplicas / 2);
        LogListener logger; 
        List<CoPhyDivgDesign> divgs = new ArrayList<CoPhyDivgDesign>();
        
        // run at most {@code maxIters} times
        int minPosition = -1;
        double minCost = -1;
        double avgReplicaImbalance = 0.0;
        double avgQueryImbalance = 0.0;
        double avgFailureImbalance = 0.0;
        
        for (int iter = 0; iter < maxIters; iter++) {
            
            logger = LogListener.getInstance();
            divg = new CoPhyDivgDesign(db, (InumOptimizer) io, logger, recommendedIndexStmt);
            divg.recommend(workload, nReplicas, loadfactor, B);
            
            divgs.add(divg);
            
            if (iter == 0 || minCost > divg.getTotalCost()) {
                minPosition = iter;
                minCost = divg.getTotalCost();
            } 
            
            avgReplicaImbalance += divg.getImbalanceReplica();
            avgQueryImbalance += divg.getImbalanceQuery();
            avgFailureImbalance += divg.getFailureImbalance();
        }
        
        avgReplicaImbalance /= maxIters;
        avgQueryImbalance /= maxIters;
        avgFailureImbalance /= maxIters;
        
        // get the best among these runs
        double timeAnalysis = 0.0;
        double timeInum = 0.0;
        for (CoPhyDivgDesign div : divgs) {
            timeInum += div.getInumTime();
            timeAnalysis += div.getAnalysisTime();
            System.out.println("cost: " + div.getTotalCost()
                            + " Number of iterations: " + div.getNumberOfIterations()
                            + " INUM time : " + div.getInumTime()
                            + " Analysis time: " + div.getAnalysisTime());
            
        }
        
        System.out.println(" min iteration: " + minPosition + " cost: " + minCost);
        divg = divgs.get(minPosition);
        
        System.out.println("CoPhy Divergent Design \n"
                            + " INUM time: " + timeInum + "\n"
                            + " ANALYSIS time: " + timeAnalysis + "\n"
                            + " TOTAL running time: " + (timeInum + timeAnalysis) + "\n"
                            + " The objective value: " + divg.getTotalCost() + "\n"
                            + "      QUERY cost:    " + divg.getQueryCost()  + "\n"
                            + "      UPDATE cost:   " + divg.getUpdateCost() + "\n"
                            + " REPLICA IMBALANCE: " + avgReplicaImbalance + "\n"
                            + " QUERY IMBALANCE: " + avgQueryImbalance + "\n"
                            + " FAILURE IMBALANCE: " + avgFailureImbalance + "\n"
                             );
        
        return divg.getTotalCost();
    }
    
    /**
     * Generate optimal candidate indexes for each statement
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected static void generateOptimalCandidatesCoPhy() throws Exception
    {
        String fileName = folder + "/candidate-divg-design.bin";
        File file = new File(fileName); 
        
        if (!file.exists()) {
        
            List<StatementCandidates> candidateCoPhy
                = new ArrayList<StatementCandidates>();
            
            Set<Index> candidates;
            Workload wl;
            CandidateGenerator candGen =
                new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
            
            recommendedIndexStmt = new HashMap<SQLStatement, Set<Index>>();
            List<SQLStatement> sqls;
            
            for (SQLStatement sql : workload) {
            
                sqls = new ArrayList<SQLStatement>();
                sqls.add(sql);
                
                wl = new Workload(sqls);
                
                if (sql.getSQLCategory().isSame(SELECT))
                    candidates = candGen.generate(wl);
                else 
                    candidates = new HashSet<Index>();
                
                recommendedIndexStmt.put(sql, candidates);                
                candidateCoPhy.add(new StatementCandidates(sql, candidates));
            }
            
            // write to file
            ObjectOutputStream write;
            
            try {
                FileOutputStream fileOut = new FileOutputStream(file);
                write = new ObjectOutputStream(fileOut);
                write.writeObject(candidateCoPhy);
                write.close();
                fileOut.close();
            } catch(IOException e) {
                throw new SQLException(e);
            }
        }
        else {
            // read from file
            ObjectInputStream in;
            List<StatementCandidates> candidateCoPhy
                    = new ArrayList<StatementCandidates>();
            
            try {
                FileInputStream fileIn = new FileInputStream(file);
                in = new ObjectInputStream(fileIn);
                candidateCoPhy = (List<StatementCandidates>) in.readObject();
                
                in.close();
                fileIn.close();            
            } catch(IOException e) {
                throw new SQLException(e);
            } catch (ClassNotFoundException e) {
                throw new SQLException(e);
            }
            
            recommendedIndexStmt = new HashMap<SQLStatement, Set<Index>>();
            Map<String, Set<Index>> tempCandidates = new HashMap<String, Set<Index>>();
            for (StatementCandidates sc : candidateCoPhy)
                tempCandidates.put(sc.sql.getSQL(), sc.candidates);
            
            for (SQLStatement stmt : workload) 
                recommendedIndexStmt.put(stmt, tempCandidates.get(stmt.getSQL()));
        }
        
    }
}
