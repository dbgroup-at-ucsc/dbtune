/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ****************************************************************************
 */

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.advisor.DynamicIndexSet;
import edu.ucsc.dbtune.advisor.HotSetSelector;
import edu.ucsc.dbtune.advisor.HotsetSelection;
import edu.ucsc.dbtune.advisor.IndexPartitions;
import edu.ucsc.dbtune.advisor.IndexStatisticsFunction;
import edu.ucsc.dbtune.advisor.InteractionSelection;
import edu.ucsc.dbtune.advisor.InteractionSelector;
import edu.ucsc.dbtune.advisor.ProfiledQuery;
import edu.ucsc.dbtune.advisor.StaticIndexSet;
import edu.ucsc.dbtune.advisor.StatisticsFunction;
import edu.ucsc.dbtune.advisor.WFALog;
import edu.ucsc.dbtune.advisor.WfaTrace;
import edu.ucsc.dbtune.advisor.WorkFunctionAlgorithm;
import edu.ucsc.dbtune.advisor.WorkloadProfiler;
import edu.ucsc.dbtune.advisor.WorkloadProfilerImpl;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.ibg.IBGBestBenefitFinder;
import edu.ucsc.dbtune.ibg.InteractionBank;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Files;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.StopWatch;
import edu.ucsc.dbtune.util.DBUtilities;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

import static edu.ucsc.dbtune.core.JdbcConnectionManager.makeDatabaseConnectionManager;
import static edu.ucsc.dbtune.util.StopWatch.normalize;
import static edu.ucsc.dbtune.util.Strings.str;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Functional test for the WFIT use case.
 *
 * Some of the documentation contained in this class refers to sections of Karl Schnaitter's 
 * Doctoral Thesis "On-line Index Selection for Physical Database Tuning". Specifically to chapter 
 * 6.
 *
 * @see <a href="http://proquest.umi.com/pqdlink?did=2171968721&Fmt=7&clientId=1565&RQT=309&VName=PQD">
 *     "On-line Index Selection for Physical Database Tuning"</a>
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class WFITTestFunctional
{
    public final static Environment env = Environment.getInstance();

    /**
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        File outputdir;

        outputdir = new File(env.getOutputFoldername() + "/" + env.getWorkloadName());

        outputdir.mkdirs();
    }

    /**
     */
    @AfterClass
    public static void tearDown() throws Exception
    {
    }

    /**
     * The test has to check first just one query and one index. We need to make sure that the index 
     * gets recommended at some point and that it is consistent always with what we expect.
     *
     * Then with 2 indexes. We'll have the following scenarios:
     *
     *  * keep both in the same partition:
     *     * 4 states
     *  * keep in different partitions:
     *     * 2 states
     *     * 2 WFIT (one per each partition)
     */
    @Test
    public void testWFIT() throws Exception
    {
        List<ProfiledQuery<DBIndex>>   qinfos;
        CandidatePool<DBIndex>         pool;
        Snapshot<DBIndex>              snapshot;
        IndexPartitions<DBIndex>       parts;
        WorkFunctionAlgorithm<DBIndex> wfa;
        WfaTrace<DBIndex>              trace;

        DatabaseConnection connection;
        WFALog             log;
        File               logFile;
        File               wfFile;
        IndexBitSet[]      wfitSchedule;
        IndexBitSet[]      optSchedule;
        IndexBitSet[]      minSched;
        String             workloadFile;
        double[]           overheads;
        double[]           minWfValues;
        int                maxNumIndexes;
        int                maxNumStates;
        int                queryCount;
        boolean            keepHistory;
        boolean            exportWfit;

        maxNumIndexes = env.getMaxNumIndexes();
        maxNumStates  = env.getMaxNumStates();
        keepHistory   = env.getWFAKeepHistory();
        workloadFile  = env.getFilenameAtWorkloadFolder("workload.sql");
        connection    = makeDatabaseConnectionManager(env.getAll()).connect();
        pool          = getCandidates(connection, workloadFile);
        qinfos        = getOfflineProfiledQueries(connection, pool, workloadFile);
        queryCount    = qinfos.size();
        overheads     = new double[queryCount];
        wfitSchedule  = new IndexBitSet[queryCount];
        snapshot      = pool.getSnapshot();
        parts         = getPartition(snapshot, qinfos, maxNumIndexes, maxNumStates);
        wfa           = new WorkFunctionAlgorithm<DBIndex>(parts,maxNumStates,maxNumIndexes,keepHistory);
        exportWfit    = true;
            
        log("read " + queryCount + " queries");
    
        getRecommendations(qinfos, wfa, wfitSchedule, overheads);
        
        if (exportWfit) {
            log     = WFALog.generateFixed(qinfos, wfitSchedule, snapshot, parts, overheads);
            logFile = new File(env.getFilenameAtOutputFolder(env.getWFITLogFilename()));

            writeLog(logFile, log, snapshot);
            processLog(logFile);

            newLine(); // linebreak on screen
            log("wrote log to " + logFile);

            log.dump();

            for (DBIndex index: snapshot) {
                System.out.println(index.creationText());
            }
        }
        
        if (keepHistory) {
            trace = wfa.getTrace();

            optSchedule = trace.optimalSchedule(parts, qinfos.size(), qinfos);
            logFile     = new File(env.getFilenameAtOutputFolder(env.getOPTLogFilename()));

            log =
                WFALog.generateFixed(
                    qinfos,
                    optSchedule,
                    snapshot, // say zero overhead
                    parts,
                    new double[queryCount]);

            writeLog(logFile, log, snapshot);
            processLog(logFile);

            newLine(); // linebreak on screen
            log("wrote log to " + logFile);

            log.dump();
            
            // write min wf values
            minWfValues = new double[qinfos.size()+1];

            for (int q = 0; q <= qinfos.size(); q++) {
                minSched       = trace.optimalSchedule(parts, q, qinfos);
                minWfValues[q] = wfa.getScheduleCost(snapshot, q, qinfos, parts, minSched);

                log("Optimal cost " + q + " = " + minWfValues[q]);

              for (int i = 0; i < q; i++) {
                  log(str(minSched[i]));
              }
              newLine(); // linebreak on screen
            }

            wfFile = new File(env.getFilenameAtOutputFolder(env.getMinWFFilename()));

            Files.writeObjectToFile(wfFile, (Object) minWfValues);
            processWfFile(wfFile);
        }

        assertThat(true, is(true));
    }

    private void getRecommendations(
        List<ProfiledQuery<DBIndex>>   qinfos,
        WorkFunctionAlgorithm<DBIndex> wfa,
        IndexBitSet[]                  recs,
        double[]                       overheads)
    {
        for (int q = 0; q < recs.length; q++) {

            ProfiledQuery<DBIndex> query = qinfos.get(q);

            log("issuing query: " + query.getSQL());
                
            // analyze the query and get the recommendation
            final StopWatch watch = new StopWatch(); // start chronometer
            wfa.newTask(query);
            Iterable<DBIndex> rec = wfa.getRecommendation();

            final long elapsedTime = watch.elapsedtime();  // peek at how much time has elapsed (in nanoseconds).
            
            recs[q] = new IndexBitSet();

            for (DBIndex idx : rec) {
                recs[q].set(idx.internalId());
            }

            overheads[q] = normalize(elapsedTime, env.getOverheadFactor());
        }
    }

    private IndexPartitions<DBIndex> getPartition(
            Snapshot<DBIndex>            candidateSet,
            List<ProfiledQuery<DBIndex>> qinfos, 
            int                          maxNumIndexes,
            int                          maxNumStates)
    {
        // get the hot set
        StaticIndexSet<DBIndex> hotSet = getHotSet(candidateSet, qinfos, maxNumIndexes);

        InteractionSelection<DBIndex> is;

        StatisticsFunction<DBIndex> doiFunc =
            new TempDoiFunction(qinfos, candidateSet, env.getIndexStatisticsWindow());

        is = 
            new InteractionSelection<DBIndex>(
                new IndexPartitions<DBIndex>(hotSet),
                doiFunc,
                maxNumStates,
                hotSet);
        
        IndexPartitions<DBIndex> parts = 
            InteractionSelector.choosePartitions(is, env.getNumPartitionIterations());
        
        return parts;
    }

    private StaticIndexSet<DBIndex> getHotSet(
            Snapshot<DBIndex>            candidateSet,
            List<ProfiledQuery<DBIndex>> qinfos,
            int                          maxNumIndexes)
    {
        StatisticsFunction<DBIndex> benefitFunc;
        HotsetSelection<DBIndex> hs;
        
        benefitFunc =
            new TempBenefitFunction(
                    qinfos,
                    candidateSet.maxInternalId(),
                    env.getIndexStatisticsWindow());

        hs =
            new HotsetSelection<DBIndex>(
                    candidateSet,
                    new StaticIndexSet<DBIndex>(),
                    new DynamicIndexSet<DBIndex>(),
                    benefitFunc,
                    maxNumIndexes,
                    false);

        StaticIndexSet<DBIndex> hotSet = HotSetSelector.chooseHotSetGreedy(hs);

        return hotSet;
    }

    private static class TempDoiFunction extends IndexStatisticsFunction<DBIndex>
    {
        private InteractionBank bank;

        TempDoiFunction(
                List<ProfiledQuery<DBIndex>> qinfos,
                Snapshot<DBIndex> candidateSet,
                int indexStatisticsWindow )
        {
            super(indexStatisticsWindow);

            bank = new InteractionBank(candidateSet);

            for (DBIndex a : candidateSet) {
                int id_a = a.internalId();
                for (DBIndex b : candidateSet) {
                    int id_b = b.internalId();
                    if (id_a < id_b) {
                        double doi = 0;
                        for (ProfiledQuery<DBIndex> qinfo : qinfos) {
                            doi += qinfo.getBank().interactionLevel(a.internalId(), b.internalId());
                        }
                        bank.assignInteraction(a.internalId(), b.internalId(), doi);
                    }
                }
            }
        }

        @Override public double doi(DBIndex a, DBIndex b) {
            return bank.interactionLevel(a.internalId(), b.internalId());
        }
    }

    private static class TempBenefitFunction extends IndexStatisticsFunction<DBIndex> {
        List<ProfiledQuery<DBIndex>> qinfos;

        IBGBestBenefitFinder finder = new IBGBestBenefitFinder();
        double[][]      bbCache;
        double[]        bbSumCache;
        int[][]         componentId;
        IndexBitSet[]   prevM;
        IndexBitSet     diffM;
        
        TempBenefitFunction(
                List<ProfiledQuery<DBIndex>> qinfos0,
                int maxInternalId,
                int indexStatisticsWindow)
        {
            super(indexStatisticsWindow);
            qinfos = qinfos0;
            
            componentId = componentIds(qinfos0, maxInternalId);
            
            bbCache = new double[maxInternalId+1][qinfos0.size()];
            bbSumCache = new double[maxInternalId+1];
            prevM = new IndexBitSet[maxInternalId+1];
            for (int i = 0; i <= maxInternalId; i++) {
                prevM[i] = new IndexBitSet();
                reinit(i, prevM[i]);
            }
            diffM = new IndexBitSet(); // temp bit set
        }
        
        private static int[][] componentIds(List<ProfiledQuery<DBIndex>> qinfos, int maxInternalId) {
            int[][] componentId = new int[qinfos.size()][maxInternalId+1];
            int q = 0;
            for (ProfiledQuery<DBIndex> qinfo : qinfos) {
                IndexBitSet[] parts = qinfo.getBank().stablePartitioning(0);
                for (DBIndex index : qinfo.getCandidateSnapshot()) {
                    int id = index.internalId();
                    componentId[q][id] = -id;
                    for (int p = 0; p < parts.length; p++) {
                        if (parts[p].get(id)) {
                            componentId[q][id] = p;
                            break;
                        }
                    }
                }
                ++q;
            }
            return componentId;
        }
        
        private void reinit(int id, IndexBitSet M) {
            int q = 0;
            double ben = 0;
            double cache[] = bbCache[id];
            for (ProfiledQuery<DBIndex> qinfo : qinfos) {
                double bb = finder.bestBenefit(qinfo.getIndexBenefitGraph(), id, M);
                cache[q] = bb;
                ben += bb;
                ++q;
            }
            bbSumCache[id] = ben;
            prevM[id].set(M); 
        }
        
        private void reinitIncremental(int id, IndexBitSet M, int b) {
            int q = 0;
            double ben = 0;
            double cache[] = bbCache[id];
            for (ProfiledQuery<DBIndex> qinfo : qinfos) {
                if (componentId[q][id] == componentId[q][b]) {
                    // interaction, recompute
                    double bb = finder.bestBenefit(qinfo.getIndexBenefitGraph(), id, M);
                    cache[q] = bb;
                    ben += bb;
                }
                else 
                    ben += cache[q];
                ++q;
            }
            prevM[id].set(M);
            bbSumCache[id] = ben;
        }
        
        @Override public double benefit(DBIndex a, IndexBitSet M) {
            int id = a.internalId();
            if (!M.equals(prevM)) {
                diffM.set(M);
                diffM.xor(prevM[id]);
                if (diffM.cardinality() == 1) {
                    reinitIncremental(id, M, diffM.nextSetBit(0));
                }
                else {
                    reinit(id, M);
                }
            }
            return bbSumCache[id];
        }

        @Override public double doi(DBIndex a, DBIndex b) {
            throw new RuntimeException("Not implemented");
        }
    }

    private void processLog(File logFile) throws Exception {
        WFALog log = readLog(logFile);

        File logTxtFile = new File(logFile.getAbsolutePath()+".txt");
        PrintStream out = new PrintStream(new FileOutputStream(logTxtFile));
        try { log.dumpPerformance(out); } 
        finally { out.close(); }
        
        File historyTxtFile = new File(logFile.getAbsolutePath()+"_history.txt");
        out = new PrintStream(new FileOutputStream(historyTxtFile));
        try { log.dumpHistory(out); } 
        finally { out.close(); }
        
        System.out.println(log.countRepartitions());
    }

    private void writeLog(File file, WFALog log, Snapshot<DBIndex> snapshot) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(file));
        try {
            out.writeObject(log);
            out.writeObject(snapshot);
        } finally {
            out.close();
        }
    }
    
    private static WFALog readLog(File logFile) throws Exception {
        log("reading from " + logFile);
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(logFile));
        try {
            WFALog log = (WFALog) in.readObject();
            return log;
        } finally {
            in.close(); // closes underlying stream
        }
    }
    
    private void processWfFile(File wfFile) throws Exception {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(wfFile));
        try {
            double[] minWfValues = (double[]) in.readObject();
            
            File wfTxtFile = new File(wfFile.getAbsolutePath()+".txt");
            PrintStream out = new PrintStream(new FileOutputStream(wfTxtFile));
            try {
                for (double val : minWfValues)
                    out.println(val);
            } finally { out.close(); }
        } finally {
            in.close(); // closes underlying stream
        }       
    }

    private static void log(String message){
        Console.streaming().log(message);
    }

    private static void newLine(){
        Console.streaming().skip();
    }

    private CandidatePool<DBIndex> getCandidates(DatabaseConnection con, String workloadFilename)
        throws SQLException, IOException {

        File workloadFile = new File(workloadFilename);

        // run the advisor to get the initial candidates
        // Give a budget of -1 to mean "no budget"

        Iterable<DBIndex> candidateSet = null;

        candidateSet = con.getIndexExtractor().recommendIndexes(workloadFile);

        // add each candidate
        CandidatePool<DBIndex> pool = new CandidatePool<DBIndex>();

        for (DBIndex index : candidateSet) {
            pool.addIndex(index);
        }

        return pool;
    }

    private List<ProfiledQuery<DBIndex>> getOfflineProfiledQueries(
            DatabaseConnection con, CandidatePool<DBIndex> pool, String workloadFilename)
        throws IOException
    {
        WorkloadProfiler<DBIndex> profiler = new WorkloadProfilerImpl<DBIndex>(con, pool, false);
        
        // get an IBG etc for each statement
        List<ProfiledQuery<DBIndex>> qinfos = null;
        List<String>                 lines  = null;

        lines = Files.getLines(new File(workloadFilename));
        
        qinfos = new ArrayList<ProfiledQuery<DBIndex>>();

        for (String sql : lines) {
            qinfos.add(profiler.processQuery(DBUtilities.trimSqlStatement(sql)));
        }

        return qinfos;
    }
}
