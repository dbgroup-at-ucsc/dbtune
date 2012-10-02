package edu.ucsc.dbtune.bip;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.metadata.ByContentIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.Workload;
import edu.ucsc.dbtune.workload.SQLStatement;

public class OnlineWorkloadCreator extends DIVPaper 
{
    private static Random random;
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 1. Read common parameter
        getEnvironmentParameters();
        
        // 2. generate online workload
        generateOnlineWorkload();
        //partitionWorkload();
    }
    
    /**
     * Generate online workload consists of transition phase (10 queries)
     * and main phase (ratio 4 : 1)
     * 
     * @throws Exception
     */
    protected static void generateOnlineWorkload() throws Exception
    {
        random = new Random();
        
        int median = workload.size() / 2;
        List<SQLStatement> sqlFirst = new ArrayList<SQLStatement>();
        List<SQLStatement> sqlSecond = new ArrayList<SQLStatement>();
        
        for (int i = 0; i < workload.size(); i++)
            if (i < median)
                sqlFirst.add(workload.get(i));
            else 
                sqlSecond.add(workload.get(i));
        
        int numPhase = 4;
        int lengthTransition = 10;
        
        List<Phase> transitions = new ArrayList<Phase>();
        List<Phase> mains = new ArrayList<Phase>();
        
        // transition phase
        for (int id = 0; id < numPhase; id++)
            transitions.add(transitionPhase(workload, lengthTransition, id));
        
        // main phase
        int numFirsts[] = {8, 2, 8, 2};
        int total = 10;
        
        for (int id = 0; id < numPhase; id++){
            mains.add(mainPhase(sqlFirst, sqlSecond, numFirsts[id], 
                    total - numFirsts[id], id));
        }
        
        // create string
        StringBuilder sb = new StringBuilder();
        for (int id = 0; id < numPhase; id++){
            // transition and then main
            sb.append(mains.get(id).toString());
            sb.append(transitions.get(id).toString());
            Rt.p(" main phase " + id + " = " + mains.get(id).sqls.size());
            Rt.p(" transition phase " + id + " = " + transitions.get(id).sqls.size());
        }
        
        // write to file
        File folderDir = new File (en.getWorkloadsFoldername());
        File newDir = new File (folderDir.getParent() + "/tpcds-online");
        if (!newDir.exists())
            newDir.mkdir();
        Rt.p(" new dir: " + newDir.toString());
        PrintWriter out = new PrintWriter(new FileWriter(newDir + "/workload.sql"), false);
        out.println(sb.toString());
        out.close();
    }
    
    /**
     * Create transition phase
     * @param wl
     * @param length
     * @param numPhase
     * @return
     */
    protected static Phase transitionPhase(Workload wl, int length, int id)
    {   
        int sizeWorkload = wl.size();
        int stmtID;
        
        Phase phase = new Phase("transition_phase_" + id);
            
        for (int i = 0; i < length; i++){
            stmtID = random.nextInt(sizeWorkload);
            phase.add(workload.get(stmtID));
        }
            
        return phase;
    }
    
    protected static Phase mainPhase(List<SQLStatement> sqlFirst, List<SQLStatement> sqlSecond, 
                            int timeFirst, int timeSecond, int id)
    {
        Phase phase = new Phase("main_phase_" + id);
        
        List<SQLStatement> common = new ArrayList<SQLStatement>();
        for (int i = 0; i < timeFirst; i++){
            for (SQLStatement stmt : sqlFirst)
                common.add(stmt);
        }
        
        for (int i = 0; i < timeSecond; i++){
            for (SQLStatement stmt : sqlSecond)
                common.add(stmt);
        }
        
        Collections.shuffle(common);
        phase.addAll(common);
        
        return phase;
    }
    
    protected static void partitionWorkload() throws Exception
    {
        int median = workload.size() / 2;
        List<SQLStatement> sqls1 = new ArrayList<SQLStatement>();
        List<SQLStatement> sqls2 = new ArrayList<SQLStatement>();
        
        for (int i = 0; i < workload.size(); i++)
            if (i < median)
                sqls1.add(workload.get(i));
            else 
                sqls2.add(workload.get(i));
        
        testCost(new Workload(sqls1), new Workload(sqls2));
    }
    
    /**
     * todo
     * @param wl1
     * @param wl2
     */
    protected static void testCost(Workload wl1, Workload wl2) throws Exception
    {
        Set<Index> candidates1, candidates2;
        
        db2Advis.process(wl1);
        candidates1 = db2Advis.getRecommendation(-1);
        
        db2Advis.process(wl2);
        candidates2 = db2Advis.getRecommendation(-1);
        
        // Find the intersection
        Set<ByContentIndex> firsts = new HashSet<ByContentIndex>();
        Set<ByContentIndex> seconds = new HashSet<ByContentIndex>();
        for (Index i : candidates1)
            firsts.add(new ByContentIndex(i));
        
        for (Index i : candidates2)
            seconds.add(new ByContentIndex(i));
        
        firsts.retainAll(seconds);
        
        long size = 0;
        for (Index i : firsts)
            size += i.getBytes();
        Rt.p(" Number of common indexes = " + firsts.size()
                + " Total size = " + (size / Math.pow(2, 20)) + " (MB)");
        
        // 1. for the first workload
        Rt.p(" The first workload ---------------------");
        double wl1Right = computeWorkloadCostDB2(wl1, candidates1);
        double wl1Opposite = computeWorkloadCostDB2(wl1, candidates2);
        double wl1FTS = computeWorkloadCostDB2(wl1, new HashSet<Index>());
        showCostInfo(wl1Right, wl1Opposite, wl1FTS);
        
        // 1. for the second workload
        Rt.p(" The second workload ---------------------");
        double wl2Right = computeWorkloadCostDB2(wl2, candidates2);
        double wl2Opposite = computeWorkloadCostDB2(wl2, candidates1);
        double wl2FTS = computeWorkloadCostDB2(wl2, new HashSet<Index>());
        showCostInfo(wl2Right, wl2Opposite, wl2FTS);
    }
    
    protected static void showCostInfo(double wl1Right, double wl1Opposite, double wl1FTS)
    {
        Rt.p(" COST with RIGHT candidates = " + wl1Right);
        Rt.p(" COST with OPPOSITE candidates = " + wl1Opposite);
        Rt.p(" COST with FTS = " + wl1FTS);
        Rt.p(" Cost(RIGHT) / cost(OPPOSITIE) = " + (wl1Right / wl1Opposite));
        Rt.p(" Cost(RIGHT) / Cost (FTS) = " + (wl1Right / wl1FTS));
        Rt.p(" Cost(OPPOSITE) / Cost (FTS) = " + (wl1Opposite / wl1FTS));
    }
        
    public static class Phase
    {
        private List<SQLStatement> sqls;
        private String name;
        
        public Phase(String _name)
        {
            name = _name;
            sqls = new ArrayList<SQLStatement>();
        }
        
        public void add(SQLStatement stmt)
        {
            sqls.add(stmt);
        }
        
        public void addAll(List<SQLStatement> stmts)
        {
            sqls.addAll(stmts);
        }
        
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            
            sb.append("--" + name + "\n");
            for (int i = 0; i < sqls.size(); i++){
                sb.append("-- query " + i + "\n");
                sb.append(sqls.get(i).getSQL() + " ; \n");
            }
              
            return sb.toString();
        }
    }
    
}
