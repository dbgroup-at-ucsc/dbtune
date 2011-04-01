package edu.ucsc.satuning;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.ucsc.satuning.admin.BadAdmin;
import edu.ucsc.satuning.admin.GoodAdmin;
import edu.ucsc.satuning.admin.SlowAdmin;
import edu.ucsc.satuning.admin.WorkloadRunner;
import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.db.DBPortal;
import edu.ucsc.satuning.util.DBUtilities;
import edu.ucsc.satuning.engine.AnalyzedQuery;
import edu.ucsc.satuning.engine.CandidatePool;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.engine.bc.BcTuner;
import edu.ucsc.satuning.engine.profiling.Profiler;
import edu.ucsc.satuning.engine.selection.IndexPartitions;
import edu.ucsc.satuning.engine.selection.Selector;
import edu.ucsc.satuning.engine.selection.StaticIndexSet;
import edu.ucsc.satuning.engine.selection.WfaTrace;
import edu.ucsc.satuning.engine.selection.WorkFunctionAlgorithm;
import edu.ucsc.satuning.offline.OfflineAnalysis;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.Debug;
import edu.ucsc.satuning.util.Files;
import edu.ucsc.satuning.util.PasswordPrompt;

public class MainTemplate<I extends DBIndex<I>> {

    protected DatabaseConnection<I> openConnectionOrExit(DBPortal<I> portal) {
        DatabaseConnection<I> conn = null;
        try {
            conn = openConnection(portal);
        } catch (Exception e) {
            Debug.logError("Failed to connect to database");
            e.printStackTrace();
            System.exit(1);
        }
        return conn;
    }

    protected DatabaseConnection<I> openConnection(DBPortal<I> portal) throws SQLException, IOException {
        if (portal.password() == null) {
            if (portal.passwordFile() != null) {
                try {
                    portal.setPassword(Files.readFile(portal.passwordFile()));
                } catch (IOException e) {
                    Debug.println("no password file found at " +portal.passwordFile()+ ", using empty string");
                    portal.setPassword("");
                }
            }
            else {
                portal.setPassword(PasswordPrompt.getPassword());
            }
        }

        DatabaseConnection<I> conn = portal.getConnection();

        return conn;
    }

    private void tryToCloseConnection(DatabaseConnection<I> conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            Debug.logError("Failed to close database");
            e.printStackTrace();
        }
    }
    
    // XXX: this could be made safer by getting DBPortal to cast each item individually
    // That's the only way to be sure each index has the right type
    @SuppressWarnings("unchecked")
    public ArrayList<ProfiledQuery<I>> readProfiledQueries() throws IOException, ClassNotFoundException {
        File file = Configuration.mode.profiledQueryFile();
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
        Debug.println("Reading profiled queries from " + file);
        try {
            return (ArrayList<ProfiledQuery<I>>) in.readObject();
        } finally {
            in.close(); // closes underlying stream
        }
    }
    
    // XXX: this could be made safer by getting DBPortal to cast each index individually
    // That's the only way to be sure each index has the right type
    @SuppressWarnings("unchecked")
    public static <I extends DBIndex<I>> CandidatePool<I> readCandidatePool() throws IOException, ClassNotFoundException {
        File file = Configuration.mode.candidatePoolFile();
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
        try {
            return (CandidatePool<I>) in.readObject();
        } finally {
            in.close(); // closes underlying stream
        }
    }
    
    public void writeLog(File file, WFALog log, Snapshot<I> snapshot) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(file));
        try {
            out.writeObject(log);
            out.writeObject(snapshot);
        } finally {
            out.close();
        }
    }
    
    public static WFALog readLog(File logFile) throws Exception {
        Debug.println("reading from " + logFile);
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(logFile));
        try {
            WFALog log = (WFALog) in.readObject();
            return log;
        } finally {
            in.close(); // closes underlying stream
        }
    }
    
    public void runBC(DBPortal<I> portal) throws Exception {    
        int maxNumIndexes = Configuration.maxHotSetSize;
        DatabaseConnection<I> conn = openConnectionOrExit(portal);
        
        CandidatePool<I> pool = readCandidatePool();
        ArrayList<ProfiledQuery<I>> qinfos = readProfiledQueries();
        int queryCount = qinfos.size();
        
        Debug.println("read " + queryCount + " queries");
        
        Snapshot<I> snapshot = pool.getSnapshot();
        StaticIndexSet<I> hotSet = OfflineAnalysis.getHotSet(snapshot, qinfos, maxNumIndexes);
        IndexPartitions<I> parts = new IndexPartitions<I>(hotSet);
        BcTuner<I> bc = new BcTuner<I>(conn, snapshot, hotSet);

        BitSet[] recs = new BitSet[queryCount];
        double[] overheads = new double[queryCount];
        for (int q = 0; q < queryCount; q++) {
            ProfiledQuery<I> qinfo = qinfos.get(q);
            
            long uStart = System.nanoTime();
            bc.processQuery(qinfo);
            recs[q] = bc.getRecommendation();
            long uEnd = System.nanoTime();
            overheads[q] = (uEnd - uStart) / 1000000.0;
        }
        
        WFALog log = WFALog.generateFixed(qinfos, recs, snapshot, parts, overheads);
        
        File logFile = Configuration.mode.logFile();
        writeLog(logFile, log, pool.getSnapshot());
        processLog(logFile);
        
        log.dump();
    }
    
    public void runWFIT(WorkloadRunner<I> runner, boolean exportWfit, boolean exportOpt) throws Exception {
        int maxNumIndexes = Configuration.maxHotSetSize;
        int maxNumStates = Configuration.maxNumStates;
        
        CandidatePool<I> pool = readCandidatePool();
        ArrayList<ProfiledQuery<I>> qinfos = readProfiledQueries();
        int queryCount = qinfos.size();
        Debug.println("read " + queryCount + " queries");
    
        Snapshot<I> snapshot = pool.getSnapshot();
        IndexPartitions<I> parts = OfflineAnalysis.getPartition(snapshot, qinfos, maxNumIndexes, maxNumStates);

        boolean wfaKeepHistoryOption = exportOpt;
        WorkFunctionAlgorithm<I> wfa = new WorkFunctionAlgorithm<I>(parts, wfaKeepHistoryOption);
            
        // run workload
        double[] overheads = new double[queryCount];
        BitSet[] wfitSchedule = new BitSet[queryCount];
        runner.getRecs(qinfos, wfa, wfitSchedule, overheads);
        
        if (exportWfit) {
            WFALog log = WFALog.generateFixed(qinfos, wfitSchedule, snapshot, parts, overheads);
            File logFile = Mode.WFIT.logFile();
            writeLog(logFile, log, snapshot);
            processLog(logFile);
            Debug.println();
            Debug.println("wrote log to " + logFile);
            log.dump();
            for (I index: snapshot) {
                System.out.println(index.creationText());
            }
        }
        
        if (exportOpt) {
            WfaTrace<I> trace = wfa.getTrace();

            BitSet[] optSchedule = trace.optimalSchedule(parts, qinfos.size(), qinfos);
            WFALog log = WFALog.generateFixed(qinfos, optSchedule, snapshot, parts, new double[queryCount]); // say zero overhead
            File logFile = Mode.OPT.logFile();
            writeLog(logFile, log, snapshot);
            processLog(logFile);
            Debug.println();
            Debug.println("wrote log to " + logFile);
            log.dump();
            
            // write min wf values
            double[] minWfValues = new double[qinfos.size()+1];
            for (int q = 0; q <= qinfos.size(); q++) {
                BitSet[] minSched = trace.optimalSchedule(parts, q, qinfos); 
                minWfValues[q] = wfa.getScheduleCost(snapshot, q, qinfos, parts, minSched);
                Debug.println("Optimal cost " + q + " = " + minWfValues[q]);
//              for (int i = 0; i < q; i++) {
//                  Debug.println(minSched[i]);
//              }
//              Debug.println();
            }
            File wfFile = Configuration.minWfFile();
            Files.writeObjectToFile(wfFile, minWfValues);
            processWfFile(wfFile);
        }
    }
    
    public void runOnlineProfiling(DBPortal<I> portal) throws Exception {
        File workloadFile = Configuration.workloadFile();
        
        DatabaseConnection<I> conn = openConnectionOrExit(portal);
        CandidatePool<I> pool = new CandidatePool<I>();
        Profiler<I> profiler = new Profiler<I>(conn, pool, true);
        
        try {
            List<String> sqlList = Files.getLines(workloadFile);
            String[] sql = new String[sqlList.size()];
            sql = sqlList.toArray(sql);
            ArrayList<ProfiledQuery<I>> qinfos = new ArrayList<ProfiledQuery<I>>();
            
            for (int i = 0; i < sql.length; i++) {
                qinfos.add(profiler.processQuery(DBUtilities.trimSqlStatement(sql[i])));
            }
            
            // write out candidates
            File candFile = Configuration.mode.candidatePoolFile();
            ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(candFile));
            try {
                out.writeObject(pool);
            } finally {
                out.close();
            }
            
            // write out queries
            File queryFile = Configuration.mode.profiledQueryFile();
            Debug.println("Writing profiled queries to " + queryFile);
            out = new ObjectOutputStream(Files.initOutputFile(queryFile));
            try {
                out.writeObject(qinfos);
            } finally {
                out.close();
            }
            
        } finally {
            tryToCloseConnection(conn);
        }
    }
    
    public void runOfflineCandidateGeneration(DBPortal<I> portal) throws Exception {
        File advisorWorklaodFile = Configuration.workloadFile();
        
        DatabaseConnection<I> conn = openConnectionOrExit(portal);
        
        try {
            // get the candidates and profiled queries
            CandidatePool<I> pool = OfflineAnalysis.getCandidates(conn, advisorWorklaodFile);
            
            // write out candidates
            File candFile = Configuration.mode.candidatePoolFile();
            ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(candFile));
            try {
                out.writeObject(pool);
            } finally {
                out.close();
            }
            
            Debug.println();
            Debug.println("wrote " + pool.getDB2IndexSet().size() + " candidates");
        } finally {
            tryToCloseConnection(conn);
        }
    }
    
    public void runOfflineProfiling(DBPortal<I> portal) throws Exception {
        File workloadFile = Configuration.workloadFile();
        
        DatabaseConnection<I> conn = openConnectionOrExit(portal);
        
        try {
            // get the candidates and profiled queries
            CandidatePool<I> pool = readCandidatePool();
            java.util.ArrayList<ProfiledQuery<I>> qinfos = OfflineAnalysis.profileQueries(conn, workloadFile, pool);
            
            // write out queries
            File queryFile = Configuration.mode.profiledQueryFile();
            ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(queryFile));
            try {
                out.writeObject(qinfos);
            } finally {
                out.close();
            }
            
            Debug.println();
            Debug.println("wrote " + qinfos.size() + " queries");
        } finally {
            tryToCloseConnection(conn);
        }
    }

    public void runGoodInterventions() throws Exception {
        CandidatePool<I> pool = readCandidatePool();
        Snapshot<I> snapshot = pool.getSnapshot();
        ArrayList<ProfiledQuery<I>>  qinfos = readProfiledQueries();
        int queryCount = qinfos.size();
        Debug.println("read " + queryCount + " queries");
        
        File inputLogFile = Mode.OPT.logFile();
        Debug.println("Getting log from " + inputLogFile.getAbsolutePath());
        WFALog inputLog = readLog(inputLogFile);
        
        // get the list of recommendations
        BitSet[] recs = new BitSet[queryCount];
        for (int i = 0; i < queryCount; i++) {
            recs[i] = new BitSet();
            recs[i].set(inputLog.getEntry(i).recommendation);
        }

        // print recs to verify
        Debug.println("Recommendations reconstructed from log: ");
        for (BitSet bs : recs) Debug.println(bs);
        Debug.println();
        
        // get the partition in BitSet form
        BitSet[] partitionBitSets = new BitSet[inputLog.getEntry(0).partition.length];
        for (int t = 0; t < partitionBitSets.length; t++) {
            partitionBitSets[t] = new BitSet();
            partitionBitSets[t].set(inputLog.getEntry(0).partition[t]);
        }
        
        IndexPartitions<I> parts = new IndexPartitions<I>(snapshot, partitionBitSets);
        
        // print partitions to verify
        Debug.println("Partitions reconstructed from log: ");
        for (BitSet bs : parts.bitSetArray()) Debug.println(bs);
        Debug.println();

        boolean wfaKeepHistoryOption = false;
        WorkFunctionAlgorithm<I> wfa = new WorkFunctionAlgorithm<I>(parts, wfaKeepHistoryOption);
            
        // run workload
        double[] overheads = new double[queryCount];
        BitSet[] wfitSchedule = new BitSet[queryCount];
        new GoodAdmin<I>(snapshot, recs).getRecs(qinfos, wfa, wfitSchedule, overheads);
        
        WFALog log = WFALog.generateFixed(qinfos, wfitSchedule, snapshot, parts, overheads);
        File logFile = Mode.GOOD.logFile();
        writeLog(logFile, log, snapshot);
        processLog(logFile);
        Debug.println();
        Debug.println("wrote log to " + logFile);
        log.dump();
    }
    
    public void runNoVoting() throws Exception {
        CandidatePool<I> pool = readCandidatePool();
        Snapshot<I> snapshot = pool.getSnapshot();
        ArrayList<ProfiledQuery<I>> qinfos = readProfiledQueries();
        int queryCount = qinfos.size();
        Debug.println("read " + queryCount + " queries");
        
        // get the list of OPT recommendations
        // also get partitions in BitSet form
        BitSet[] optRecs;
        BitSet[] partitionBitSets;
        { 
            File optLogFile = Mode.OPT.logFile();
            WFALog optLog = readLog(optLogFile);
            assert queryCount == optLog.entryCount();
            optRecs = new BitSet[queryCount];
            for (int i = 0; i < queryCount; i++) {
                optRecs[i] = new BitSet();
                optRecs[i].set(optLog.getEntry(i).recommendation);
            }
            partitionBitSets = new BitSet[optLog.getEntry(0).partition.length];
            for (int t = 0; t < partitionBitSets.length; t++) {
                partitionBitSets[t] = new BitSet();
                partitionBitSets[t].set(optLog.getEntry(0).partition[t]);
            }
        }
        
        IndexPartitions<I> parts = new IndexPartitions<I>(snapshot, partitionBitSets);
        
        // get the list of WFIT recommendations
        BitSet[] wfitRecs = new BitSet[queryCount];
        double[] overheads = new double[queryCount];
        {
            File wfitLogFile = Mode.WFIT.logFile();
            WFALog wfitLog = readLog(wfitLogFile);
            assert queryCount == wfitLog.entryCount();
            for (int i = 0; i < queryCount; i++) {
                wfitRecs[i] = new BitSet();
                wfitRecs[i].set(wfitLog.getEntry(i).recommendation);
                overheads[i] = wfitLog.getEntry(i).logicOverhead;
            }
        }
        
        assert wfitRecs.length == optRecs.length;
        
        WFALog log = WFALog.generateDual(qinfos, optRecs, wfitRecs, snapshot, parts, overheads);
        File logFile = Mode.NOVOTE.logFile();
        writeLog(logFile, log, snapshot);
        processLog(logFile);
        Debug.println();
        Debug.println("wrote log to " + logFile);
        log.dump();
    }
    
    public void runBadInterventions() throws Exception {
        CandidatePool<I> pool = readCandidatePool();
        Snapshot<I> snapshot = pool.getSnapshot();
        ArrayList<ProfiledQuery<I>> qinfos = readProfiledQueries();
        int queryCount = qinfos.size();
        Debug.println("read " + queryCount + " queries");
        
        File inputLogFile = Mode.OPT.logFile();
        Debug.println("Getting log from " + inputLogFile.getAbsolutePath());
        WFALog inputLog = readLog(inputLogFile);
        
        // get the list of recommendations
        BitSet[] recs = new BitSet[queryCount];
        for (int i = 0; i < queryCount; i++) {
            recs[i] = new BitSet();
            recs[i].set(inputLog.getEntry(i).recommendation);
        }

        // print recs to verify
        Debug.println("Recommendations reconstructed from log: ");
        for (BitSet bs : recs) Debug.println(bs);
        Debug.println();
        
        // get the partition in BitSet form
        BitSet[] partitionBitSets = new BitSet[inputLog.getEntry(0).partition.length];
        for (int t = 0; t < partitionBitSets.length; t++) {
            partitionBitSets[t] = new BitSet();
            partitionBitSets[t].set(inputLog.getEntry(0).partition[t]);
        }
        
        IndexPartitions<I> parts = new IndexPartitions<I>(snapshot, partitionBitSets);
        
        // print partitions to verify
        Debug.println("Partitions reconstructed from log: ");
        for (BitSet bs : parts.bitSetArray()) Debug.println(bs);
        Debug.println();

        boolean wfaKeepHistoryOption = false;
        WorkFunctionAlgorithm<I> wfa = new WorkFunctionAlgorithm<I>(parts, wfaKeepHistoryOption);
            
        // run workload
        double[] overheads = new double[queryCount];
        BitSet[] wfitSchedule = new BitSet[queryCount];
        new BadAdmin<I>(snapshot, recs).getRecs(qinfos, wfa, wfitSchedule, overheads);
        
        WFALog log = WFALog.generateFixed(qinfos, wfitSchedule, snapshot, parts, overheads);
        File logFile = Mode.BAD.logFile();
        writeLog(logFile, log, snapshot);
        processLog(logFile);
        Debug.println();
        Debug.println("wrote log to " + logFile);
        log.dump();
    }
    
    public void runSlow(boolean vote) throws Exception {
        int maxNumIndexes = Configuration.maxHotSetSize;
        int maxNumStates = Configuration.maxNumStates;
        int lag = Configuration.slowAdminLag;
        
        CandidatePool<I> pool = readCandidatePool();
        ArrayList<ProfiledQuery<I>>  qinfos = readProfiledQueries();
        int queryCount = qinfos.size();
        Debug.println("read " + queryCount + " queries");
        
        Snapshot<I> snapshot = pool.getSnapshot();
        IndexPartitions<I> parts = OfflineAnalysis.getPartition(snapshot, qinfos, maxNumIndexes, maxNumStates);

        boolean wfaKeepHistoryOption = false;
        WorkFunctionAlgorithm<I> wfa = new WorkFunctionAlgorithm<I>(parts, wfaKeepHistoryOption);
            
        // run workload
        double[] overheads = new double[queryCount];
        BitSet[] sched = new BitSet[queryCount];
        new SlowAdmin<I>(snapshot, lag, vote).getRecs(qinfos, wfa, sched, overheads);
        
        WFALog log = WFALog.generateFixed(qinfos, sched, snapshot, parts, overheads);
        File logFile = (vote) ? Mode.SLOW.logFile() : Mode.SLOW_NOVOTE.logFile();
        writeLog(logFile, log, snapshot);
        processLog(logFile);
        Debug.println();
        Debug.println("wrote log to " + logFile);
        log.dump();
    }
    
    public void runAutomatic() throws Exception {
        Selector<I> selector = new Selector<I>();
        
        ArrayList<ProfiledQuery<I>> queries = readProfiledQueries();
        int queryCount = queries.size();
        Debug.println("read "+queryCount+" queries");
        
        ArrayList<AnalyzedQuery<I>> qinfos = new ArrayList<AnalyzedQuery<I>>();
        BitSet[] recs = new BitSet[queryCount];
        double[] overheads = new double[queryCount];
        for (int q = 0; q < queryCount; q++) {
            ProfiledQuery<I> query = queries.get(q);
            Debug.println("issuing query: " + query.sql);
            
            // analyze the query and get the recommendation
            long uStart = System.nanoTime();
            AnalyzedQuery<I> qinfo = selector.analyzeQuery(query);
            Iterable<I> rec = selector.getRecommendation();
            long uEnd = System.nanoTime();
            
            qinfos.add(qinfo);
            recs[q] = new BitSet();
            for (I idx : rec) 
                recs[q].set(idx.internalId());
            overheads[q] = (uEnd - uStart) / 1000000.0;
        }
        
        Snapshot<I> lastCandidateSet = queries.get(queryCount-1).candidateSet;
        
        WFALog log = WFALog.generateDynamic(qinfos, recs, overheads);
        File logFile = Configuration.mode.logFile();
        writeLog(logFile, log, lastCandidateSet);
        ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(logFile));
        try {
            out.writeObject(log);
            out.writeObject(lastCandidateSet);
        } finally {
            out.close();
        }
        
        log.dump();
        for (I index: lastCandidateSet) {
            System.out.println(index.creationText());
        }
    }

    public void runLogging(Mode mode) throws Exception {
        File logFile = mode.logFile();
        if (logFile == null) {
            throw new Error("Mode " + mode + " does not have a log file");
        }
        processLog(mode.logFile());
        if (mode == Mode.OPT) {
            try {
                Debug.print("Trying to get minimum WF values... ");
                processWfFile(Configuration.minWfFile());
                Debug.println("done");
            } catch (java.io.FileNotFoundException e) {
                Debug.println("file not found");
            }
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
}
