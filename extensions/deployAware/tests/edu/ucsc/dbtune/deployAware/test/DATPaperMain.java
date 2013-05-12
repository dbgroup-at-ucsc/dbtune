package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import edu.ucsc.dbtune.deployAware.test.DATPaper.DATExp;
import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
import edu.ucsc.dbtune.deployAware.test.DATPaperParams.Callback;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.util.Rt;


public class DATPaperMain {
    boolean windowOnly = false;
    boolean outputWinCost = false;
    DATPaperParams params = new DATPaperParams();
    boolean rerunAllExp = false;
    SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    String rerunExperimentBeforeTime = "06/22/2013 16:40:01";
    long rerunTime = df.parse(rerunExperimentBeforeTime).getTime();
    long gigbytes = 1024L * 1024L * 1024L;
    long tpchWindowSize = 20 * 3600 * 3000;
    long tpcdsWindowSize = 10 * 3600 * 3000;
    File figsDir;
    TestSet tpch = new TestSet("TPCH", "tpch10g", "deployAware", "tpchPaper.sql", 10 * gigbytes, "tpch", tpchWindowSize);
    TestSet tpcds = new TestSet("TPCDS", "tpcds100", "deployAware", "tpcdsPaper.sql", 100 * gigbytes, "tpcds",
            tpcdsWindowSize);
    TestSet[] sets = { 
//            tpch,
        tpcds,
    };

    public DATPaperMain() throws Exception {
        // TODO: verify by DB2 OPTIMIZER
        DATPaper.verifyByDB2Optimizer= false;
        
        Rt.p("Current time: " + df.format(new Date()));
        //        windowOnly = true;
        //         rerunAllExp=true;
        //        params.spaceFactor.def = 0.1;
        //deployAwareTuning();
        workloadSequence();
        //         figsDir = new File(params.figsDir, "dat");
        //         figsDir.mkdirs();
        //         params.m.def=1;
        //        run(params.spaceFactor, "space", "Varying space budget", new Callback() {
        //            @Override
        //            public void callback(TestSet set, DATExp p, double value) {
        //                p.spaceBudge = set.size * value;
        //            }
        //        });
    }

    Callback defCallback;

    DATExp def(TestSet set, String name, String desc) throws Exception {
        GnuPlot.defaultStyle = GnuPlot.Style.histograms;
        GnuPlot.uniform = true;
        DATExp p = params.def();
        p.alpha = 1;
        p.beta = 1;
        WorkloadLoader loader = new WorkloadLoader(set.dbName, set.workloadName, set.fileName,
                params.generateIndexMethod);
        SeqInumCost cost = loader.loadCost();
        p.loader = loader;
        p.cost = cost;
        p.percentageUpdate = params.percentageUpdate.def;
        p.plot = new GnuPlot(figsDir, set.shortName + "X" + name, name, "cost");
        p.plot.setPlotNames(DATPaper.curNames);
        p.plotWin = new GnuPlot(figsDir, set.shortName + "X" + name + "W", name, "cost");
        p.plotWin.setPlotNames(DATPaper.curNames);
        set.plotNames.add(figsDir.getName() + "/" + p.plot.name);
        set.figureNames.add(desc);
        p.spaceBudge = (long) (set.size * params.spaceFactor.def);
        p.debugFile = new File(figsDir, p.plot.name + "_debug.txt");
        p.skylineFile = new File(figsDir, p.plot.name + "_skyline.txt");
        double sum = 0;
        for (int i = 0; i < cost.indexCount(); i++)
            sum += cost.indices.get(i).createCost;
        p.avgCreateCost = sum / cost.indexCount();
        if (params.winFactor.def < 0)
            p.windowSize = -1;
        else
            p.windowSize = params.winFactor.def * p.avgCreateCost;
        p.rerunExperiment = rerunAllExp || !p.plot.orgDataFile.exists()
                || p.plot.orgDataFile.lastModified() < rerunTime;
        if (defCallback != null)
            defCallback.callback(null, p, 0);
        return p;
    }

    public void run(TestSet set, DATPaperParams.Set inputs, String name, String desc, boolean useRunningTime,
            Callback callback) throws Exception {
        DATExp p = def(set, name, desc);
        p.plot.xName = inputs.name;
        if (useRunningTime) {
            p.plot.yName = "time";
            p.useRunningTime = useRunningTime;
        }
        if (p.rerunExperiment) {
            p.debugFile.delete();
            for (int k = 0; k < inputs.values.length; k++) {
                p.cost = p.loader.loadCost();
                double value = inputs.values[k];
                p.plotX = value;
                p.plotLabel = inputs.names[k];
                if (callback != null)
                    callback.callback(set, p, value);
                inputs.callback.callback(set, p, value);
                if (defCallback != null)
                    defCallback.callback(null, p, 0);
                new DATPaper(p);
            }
        }
        p.plot.finish();
        if (outputWinCost)
            p.plotWin.finish();
    }

    public void run(DATPaperParams.Set inputs, String name, String desc) throws Exception {
        for (TestSet set : sets)
            run(set, inputs, name, desc, false, null);
    }

    public void runUseRunningTime(DATPaperParams.Set inputs, String name, String desc) throws Exception {
        for (TestSet set : sets)
            run(set, inputs, name, desc, true, null);
    }
    
    /**
     * Trung's implementation for skyline
     * TODO
     */
    public void runSkyline(DATPaperParams.Set inputs, String name, String desc) throws Exception {
        for (TestSet set : sets)
            runSkyline(set, inputs, name, desc, false, null);
    }
    
    public class ScheduleCost
    {
        public String intCost;
        public String finalCost;
        
        public ScheduleCost(String icost, String fcost)
        {
            intCost = icost;
            finalCost = fcost;
        }
    }
    
    /**
     * Trung's implementation: Record the intermediate and final cost
     * to derive skyline schedules
     * 
     * TODO
     */
    public void runSkyline(TestSet set, DATPaperParams.Set inputs, String name, String desc, boolean useRunningTime,
            Callback callback) throws Exception {
        DATExp p = def(set, name, desc);
        p.plot.xName = inputs.name;
        
        if (useRunningTime) {
            p.plot.yName = "time";
            p.useRunningTime = useRunningTime;
        }
        if (p.rerunExperiment) {
            p.debugFile.delete();
            
            List<ScheduleCost> costBip = new ArrayList<ScheduleCost>();
            List<ScheduleCost> costGreedy = new ArrayList<ScheduleCost>();
            DecimalFormat twoDForm = new DecimalFormat("#.##");
            
            for (int k = 0; k < inputs.values.length; k++) {
                p.cost = p.loader.loadCost();
                double value = inputs.values[k];
                p.plotX = value;
                p.plotLabel = inputs.names[k];
                if (callback != null)
                    callback.callback(set, p, value);
                inputs.callback.callback(set, p, value);
                if (defCallback != null)
                    defCallback.callback(null, p, 0);
                DATPaper paper = new DATPaper(p);
                Rt.p("GREEDY, int cost = " + paper.intCostGreedy
                        + " final cost = " + paper.finalCostGreedy);
                
                costGreedy.add(new ScheduleCost(
                        twoDForm.format(paper.intCostGreedy / Math.pow(10, 6)), 
                        twoDForm.format(paper.finalCostGreedy / Math.pow(10, 6))));
                
                Rt.p("DAT, int cost = " + paper.intCostBip
                        + " final cost = " + paper.finalCostBip);
                
                costBip.add(new ScheduleCost(
                        twoDForm.format(paper.intCostBip / Math.pow(10, 6)), 
                        twoDForm.format(paper.finalCostBip / Math.pow(10, 6))));                      
            }
            addSkyline(p, costBip, costGreedy);
        }
        p.plot.finish();
        if (outputWinCost)
            p.plotWin.finish();
    }
    
    /**
     * Trung's implementation
     */
    public void addSkyline(DATExp p, List<ScheduleCost> costBip, List<ScheduleCost> costGreedy)
            throws Exception
    {
        // write to skyline file
        p.skylineFile.delete();
        PrintWriter out = new PrintWriter(new FileWriter(p.skylineFile));
        int numResult = costBip.size();
        String tab = " ";
        String newline;
        
        for (int i = 0; i < numResult; i++) {
            newline = costBip.get(i).intCost + tab + costBip.get(i).finalCost;
            newline += tab;
            newline += (costGreedy.get(i).intCost + tab + costGreedy.get(i).finalCost);
            out.println(newline);
        }         
        
        out.close();
    }
    
    public void run2(DATPaperParams.Set inputs1, DATPaperParams.Set inputs2, String name, String desc) throws Exception {
        for (TestSet set : sets) {
            DATExp p = def(set, name, desc);
            Rt.p(p.plot.name);
            p.plot.pm3d = true;
            p.plot.set3dNames(inputs1.names, inputs2.names);
            p.plot.xName = inputs1.name;
            p.plot.yName = inputs2.name;
            if (p.rerunExperiment) {
                p.debugFile.delete();
                for (int i = 0; i < inputs1.values.length; i++) {
                    for (int j = 0; j < inputs2.values.length; j++) {
                        p.cost = p.loader.loadCost();
                        double value1 = inputs1.values[i];
                        double value2 = inputs2.values[j];
                        p.plotX = value1;
                        p.plotLabel = inputs1.names[i];
                        p.plotZ = value2;
                        p.plotLabelZ = inputs2.names[j];
                        inputs1.callback.callback(set, p, value1);
                        inputs2.callback.callback(set, p, value2);
                        if (defCallback != null)
                            defCallback.callback(null, p, 0);
                        new DATPaper(p);
                    }
                }
            }
            p.plot.finish();
            if (outputWinCost)
                p.plotWin.finish();
        }
    }

    public void windowOnly(String name, String desc, Callback callback) throws Exception {
        for (TestSet set : sets) {
            DATExp p = def(set, name, desc);
            p.plot.xName = "window";
            if (callback != null)
                callback.callback(set, p, 0);
            if (defCallback != null)
                defCallback.callback(null, p, 0);
            if (callback == null || p.rerunExperiment) {
                p.debugFile.delete();
                DATPaper run = new DATPaper(p);
                p.plot.vs.clear();
                p.plot.current.clear();
                p.plot.xtics.clear();
                for (int i = 0; i < run.datWindowCosts.length; i++) {
                    p.plot.startNewX(i);
                    p.plot.addY(run.datWindowCosts[i]);
                    // if (plotNames.length == 3)
                    // plot.addY(run.mkpWindowCosts[i]);
                    p.plot.addY(run.greedyWindowCosts[i]);
                }
            }
            p.plot.finish();
        }
    }
    
    /**
     * Create the workload that is used in the paper: workload
     * with queries and updates
     * 
     * @throws Exception
     */
    protected int[][] workloadSeqQ(DATExp p)
    {
        int[][] map;
        
        List<Integer> queryIDs = new ArrayList<Integer>();
        List<Integer> updateIDs = new ArrayList<Integer>();
        
        for (int i = 0; i < p.cost.queries.size(); i++) {
            SeqInumQuery query = p.cost.queries.get(i);
            if (!"select".equalsIgnoreCase(query.sql.getSQLCategory().name())) 
                updateIDs.add(i);
            else
                queryIDs.add(i);
        }
        
        // Assume we have four windows
        int sizeQueryWindow = 20;
        int times = 1;
        int numQueryWindows = queryIDs.size() / sizeQueryWindow;
        // get only 40 queries
        p.m = 2 * numQueryWindows;
        p.m *= times;
        
        // split queryIDs
        List<List<Integer>> splitQueryIDs = new ArrayList<List<Integer>> ();
        for (int c = 0; c < numQueryWindows; c++)
            splitQueryIDs.add(queryIDs.subList(c * sizeQueryWindow, (c + 1) * sizeQueryWindow));
        
        // Distribute statements into maps
        map = new int[(int) p.m][];
        int mapID = 0; 
        
        for (int t = 0; t < times; t++){
            
            for (int j = 0; j < numQueryWindows; j++) {
                
                // queries 
                map[mapID] = new int[sizeQueryWindow];
                for (int i = 0; i < sizeQueryWindow; i++)
                    map[mapID][i] = splitQueryIDs.get(j).get(i);                
                mapID++;
                
                /*
                // then updates
                map[mapID] = new int[1];
                map[mapID][0] = updateIDs.get(j % updateIDs.size()); 
                mapID++;
                */
                map[mapID] = new int[updateIDs.size()];
                for (int i = 0; i < updateIDs.size(); i++)
                    map[mapID][i] = updateIDs.get(i);
                mapID++;
            }
        }   
        
        return map;
    }
    
    
    /**
     * Create the workload that is used in the paper: workload
     * with queries and updates
     * 
     * @throws Exception
     */
    protected int[][] workloadSeqQTest(DATExp p)
    {
        int[][] map;
        
        int times = 3;
        p.m = times * p.cost.queries.size();
        
        // Distribute statements into maps
        map = new int[(int) p.m][];
        int mapID = 0; 
        
        for (int t = 0; t < times; t++){
            
            for (int j = 0; j < p.cost.queries.size(); j++) {
                // queries 
                map[mapID] = new int[1];
                map[mapID][0] = j;
                mapID++;
            }
        }   
        
        return map;
    }

    /**
     * Create the workload that is used in the paper: workload
     * with queries and updates
     * 
     * @throws Exception
     */
    protected int[][] workloadSeqSynthetic(DATExp p)
    {
        int[][] map;
        
        List<Integer> queryIDs = new ArrayList<Integer>();
        List<Integer> updateIDs = new ArrayList<Integer>();
        
        for (int i = 0; i < p.cost.queries.size(); i++) {
            SeqInumQuery query = p.cost.queries.get(i);
            if (!"select".equalsIgnoreCase(query.sql.getSQLCategory().name())) 
                updateIDs.add(i);
            else
                queryIDs.add(i);
        }
        
        p.m = updateIDs.size() + 1;
        map = new int[(int) p.m][];
        for (int i = 0; i < p.m; i++) {
            if (i < p.m - 1) {
                map[i] = new int[] { updateIDs.get(i) };
            } else {
                map[i] = new int[queryIDs.size()];
                for (int j = 0; j < queryIDs.size(); j++)
                    map[i][j] = j;
            }
        }
        
        return map;
        
        /*
        p.m = 50;
        p.l = 2;
        map = new int[(int) p.m][];
        int update = 0;
        for (int i = 0; i < p.cost.queries.size(); i++) {
            SeqInumQuery query = p.cost.queries.get(i);
            if (!"select".equalsIgnoreCase(query.sql.getSQLCategory().name())) {
                update = i;
                break;
            }
        }
        for (int i = 0; i < p.m; i++) {
            if (i < p.m - 1) {
                map[i] = new int[] { update };
            } else {
                map[i] = new int[p.cost.queries.size()];
                for (int j = 0; j < p.cost.queries.size(); j++)
                    map[i][j] = j;
            }
        }
        
        return map;
        */
        
        /*
        p.m = 1;
        map = new int[(int) p.m][p.cost.queries.size()];
        for (int i = 0; i < p.cost.queries.size(); i++)
            map[0][i] = i;
           */
        
        /*
        List<Integer> queryIDs = new ArrayList<Integer>();
        List<Integer> updateIDs = new ArrayList<Integer>();
        
        for (int i = 0; i < p.cost.queries.size(); i++) {
            SeqInumQuery query = p.cost.queries.get(i);
            if (!"select".equalsIgnoreCase(query.sql.getSQLCategory().name())) 
                updateIDs.add(i);
            else
                queryIDs.add(i);
        }
        
        // split queries
        int splitQuery = 10;
        p.m = splitQuery + 1;
        map = new int[(int) (p.m)][];
        
        int numQueriesOneWindow = (int) Math.ceil(queryIDs.size() / splitQuery);
        
        int counter = 0;
        for (int i = 0; i < splitQuery; i++) {
            // Distribute a set of consecutive @numberQueriesOneWindow 
            // statements into a window
            int sizeEntry;
            
            if (i < splitQuery - 1) {
                sizeEntry = numQueriesOneWindow;                            
            }
            else
                sizeEntry = p.cost.queries.size() - 
                        numQueriesOneWindow * (int)(p.m - 1);
            
            map[i] = new int[sizeEntry];
            for (int j = 0; j < sizeEntry; j++) 
                map[i][j] = counter++;
        }
        
        map[splitQuery] = new int[updateIDs.size()];
        for (int i = 0; i < updateIDs.size(); i++)
            map[splitQuery][i] = updateIDs.get(i);
        */
        
        /*
        if (true) {
            p.m = 8;
            // Number of queries (length) of a window
            int numQueriesOneWindow = (int) Math.ceil(p.cost.queries.size() / p.m);
            Rt.p("number of queries = " + p.cost.queries.size());
            // Might distribute the last query more than one times
            // in the last window
            map = new int[(int) p.m][];
            int counter = 0;
            for (int i = 0; i < p.m; i++) {
                // Distribute a set of consecutive @numberQueriesOneWindow 
                // statements into a window
                int sizeEntry;
                
                if (i < p.m - 1) {
                    sizeEntry = numQueriesOneWindow;                            
                }
                else
                    sizeEntry = p.cost.queries.size() - 
                            numQueriesOneWindow * (int)(p.m - 1);
                
                map[i] = new int[sizeEntry];
                for (int j = 0; j < sizeEntry; j++) 
                    map[i][j] = counter++;
            }
        }
        */
        /*
        @Override
        public void callback(TestSet set, DATExp p, double value) {
            int[][] map = new int[p.cost.queries.size()][1];
            for (int i = 0; i < map.length; i++)
                map[i][0] = i;
            p.queryMap = map;
        }
        */

    }
    
    
    public void workloadSequence() throws Exception {
        outputWinCost = false;
        DATPaper.addTransitionCostToObjective = true;
        defCallback = new Callback() {
            
            // TRUNG -- sequence of set of queries
            @Override
            public void callback(TestSet set, DATExp p, double value) 
            {
                int[][] map;
                
                // Find out queries and updates
                // TODO: this is the main workload used in the paper                
                map = workloadSeqQ(p);
                    
                //map = workloadSeqSynthetic(p);
                
                // Another way to distribute queries over windows
                //map = workloadSeqSynthetic(p);
                p.queryMap = map;
            }
        };
        DATPaper.noAlphaBeta = true;

        figsDir = new File(params.figsDir, "seq");
        figsDir.mkdirs();

        for (TestSet set : sets) {
            set.plotNames.clear();
            set.figureNames.clear();
        }
        if (windowOnly) {
            windowOnly("window", "cost of each window", null);
        } else {
            //useDB2Optimizer = true;
            //verifyByDB2Optimizer=true;
            
            Rt.p("=== Expt 1: Varying space budget =============");
            run(params.spaceFactor, "space", "Varying space budget");
            
            /*
            Rt.p("=== Expt 1: Time for varying space budget =============");
            runUseRunningTime(params.spaceFactor, "space", "Varying space budget, use running time");
            */
            
            /* 
             //remove this expt.
            Rt.p("=== Expt 2: Varying window size =============");
            run(params.winFactor, "window", "Varying window size");
            
            // remove this expt.  
            Rt.p("=== Expt 3: Varying number of windows =============");            
            run(params.m, "m", "Varying number of windows");
            */
            /*
            Rt.p("=== Varying number of indexes in a window =============");            
            run(params.l, "l", "Varying number of indexes in a window");
            
            
            Rt.p("=== Expt 4: Varying ratio of updates =============");
            run(params.percentageUpdate, "update", "Varying ratio of udpates");
            */
            
            //run2(params.spaceFactor, params.m, "spaceNm", "Varying space budget and number of windows");
            /*
            run2(params.spaceFactor, params.l, "spaceL", "Varying space budget and number of indexes per window");
            */
            /*
            run2(params.percentageUpdate, params.l, "updateL", "Varying percentage update and number of indexes per window");
            */
            //run2(params.percentageUpdate, params.spaceFactor, "spaceUpdate", "Varying percentage update and space factor");
            
            /*
            Rt.p("=== Expt 5: Varying workload input sizes =============");
            run(tpcds, params.workloadRatio, "inputSize", "Running time w.r.t. input size", true, null);
            */
            /*
            Rt.p("=== Varying index sizes =============");
            run(tpcds, params.indexRatio, "indexSize", "Running time w.r.t. index size", true, null);
              */
            /*
            Rt.p("=== Expt 6: Varying BIP running time =============");
            run(tpcds, params.bipEpGap, "bipTime", "Running time w.r.t. bip EpGap", true, null);
            */
            //run(tpcds, params.bipEpGap, "bipQuality", "Result w.r.t. bip EpGap", false, null);
            
        }
        DATPaperOthers.generatePdf(new File(params.outputDir, "seq.tex"), params, sets);
    }

    public void deployAwareTuning() throws Exception {
        outputWinCost = false;
        DATPaper.addTransitionCostToObjective = false;
        defCallback = new Callback() {
            @Override
            public void callback(TestSet set, DATExp p, double value) {
                int[][] map = new int[(int) p.m][p.cost.queries.size()];
                for (int i = 0; i < p.m; i++)
                    for (int j = 0; j < p.cost.queries.size(); j++)
                        map[i][j] = j;
                p.queryMap = map;
            }
        };
        DATPaper.noAlphaBeta = false;

        figsDir = new File(params.figsDir, "dat");
        figsDir.mkdirs();

        for (TestSet set : sets) {
            set.plotNames.clear();
            set.figureNames.clear();
        }

        if (windowOnly) {
            windowOnly("window", "cost of each window", null);
            return;
        }
        
        /*
        Rt.p("=== Expt 1: varying number of windows");
        run(params.m, "m", "Varying number of windows");
        */
        /*
        // use at most 8 indexes at each window
        // TODO: rerun this expt. 
        Rt.p("=== Expt 1: skylines");
        params.l.def = 4;
        params.spaceFactor.def = 0.2;
        runSkyline(params._1mada, "_1mada", "Varying number of windows");
        */
        /*
        params.m.def = 1;
        run(params.spaceFactor, "space", "Varying space budget");
        */
        // DO NOT need this constraint
        /*
        Rt.p("=== Expt 2: varying window size");
        run(params.winFactor, "window", "Varying window size");
        */
        /*
        Rt.p("=== Expt 3: Varying ratio of updates =============");
        run(params.percentageUpdate, "update", "Varying ratio of udpates");
        */
        
        //run(params.l, "l", "Varying number of indexes in a window");
        
        Rt.p("=== Expt 4: boost first window");
        params.spaceFactor.def = 0.2;
        params._1mada.def = 0.5;
        params.l.def = 4;
        
        windowOnly("firstWindow", "Boost first window", new Callback() {
            @Override
            public void callback(TestSet set, DATExp p, double value) {
                double[] weights = new double[(int) params.m.def];                
                Arrays.fill(weights, 1);
                weights[0] = 1;
                p.windowWeights = weights;
            }
        });
        
        /*
        Rt.p("=== Expt 5: cost must decrease");
        windowOnly("mustDesc", "Cost must decrease", new Callback() {
            @Override
            public void callback(TestSet set, DATExp p, double value) {
                p.costMustDecrease = true;
            }
        });
        */
        
        DATPaperOthers.generatePdf(new File(params.outputDir, "dat.tex"), params, sets);
    }

    public static void main(String[] args) throws Exception {
        new DATPaperMain();
    }
}
