package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import edu.ucsc.dbtune.bip.DIVPaper.DivPaperEntry;
import edu.ucsc.dbtune.bip.DIVPaper.DivPaperEntryDetail;
import edu.ucsc.dbtune.bip.DIVPaper.Point;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.ConstraintDivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;

public class UpdateCostConstraintDivBIPTest extends DIVPaper 
{
    protected static ConstraintDivBIP div;
    protected static String fileName;
    
    @Test
    public void main() throws Exception
    {
        // 1. initialize file locations & necessary 
        // data structures
        initialize();
        
        // get parameters
        getEnvironmentParameters();        
        setParameters();
        
        run();
        
        // not to draw graph
        resetParameterNotDrawingGraph();
    }
    
    /**
     * Call DIVBIP for each pair of replicas and budgets 
     * 
     * 
     * @throws Exception
     */
    public static void run() throws Exception
    {   
        long budget;
        WorkloadCostDetail wc;
        
        
        // TODO: to derive from UNIF
        double baseUpper = 0.0;
        double baseQuery = 0.0;
        
        int n = nReplicas;
        budget = convertBudgetToMB(B);
        
        // 1. Read the result from UNIF file ----------------------
        DivPaperEntry entryBase = new DivPaperEntry(dbName, wlName, n, budget, null);
        unifFile = new File(rawDataDir, wlName + "_" + UNIF_DETAIL_DB2_FILE);
        mapUnifDetail = readDivResultDetail(unifFile);
        Rt.p(" read from file = " + unifFile.getAbsolutePath());
        
        for (Map.Entry<DivPaperEntryDetail, Double> ele : mapUnifDetail.entrySet()) {
            if (ele.getKey().equals(entryBase)) {
                baseUpper = ele.getKey().updateCost;
                baseQuery = ele.getKey().queryCost;
                break;
            }
        }
        Rt.p(" Base update cost = " + baseUpper);
        // --------------------------------------------------
        
        for (double w : constraintUpdateRatios){
            
            detailEntries = new HashMap<DivPaperEntryDetail, Double>();
            
            fileName = wlName + "_" + Double.toString(w) + "_" + 
                                    DIV_UPDATE_COST_CONSTRAINT;
            LogListener logger = LogListener.getInstance();
            wc = testConstraintDiv(nReplicas, B, logger, (double)(baseUpper * w));
            
            if (wc != null){
                DivPaperEntryDetail entry = new DivPaperEntryDetail
                    (dbName, wlName, n, budget, divConf);
                entry.queryCost = wc.queryCost;
                entry.updateCost = wc.updateCost;
                    
                detailEntries.put(entry, wc.totalCost);
                
                Rt.p(" DIV QUERY COST = " + entry.queryCost);
                Rt.p(" UNIF QUERY COST = " + baseQuery);
                Rt.p(" Ratio = " + entry.queryCost / baseQuery);
                
                // 2. store in the file
                divFile = new File(rawDataDir, fileName);
                divFile.delete();
                divFile = new File(rawDataDir, fileName);
                divFile.createNewFile();
                    
                // store in the serialize file
                serializeDivResultDetail(detailEntries, divFile);
            }
        }   
    }
    
    /**
     * Run the BIP with the parameters set by other functions
     * @throws Exception
     */
    public static WorkloadCostDetail testConstraintDiv(int _n, double _B, 
                                        LogListener logger,
                                        double upperUpdate) 
            throws Exception
    {
        div = new ConstraintDivBIP();
        
        // Derive corresponding parameters
        nReplicas = _n;
        B = _B;
        loadfactor = (int) Math.ceil( (double) nReplicas / 2);
        
        Optimizer io = db.getOptimizer();
        div.setCandidateIndexes(candidates);
        div.setWorkload(workload); 
        div.setOptimizer((InumOptimizer) io);
        div.setNumberReplicas(nReplicas);
        div.setLoadBalanceFactor(loadfactor);
        div.setSpaceBudget(B);
        div.setLogListenter(logger);
        div.setCommunicatingInumOnTheFly(false);
        div.setUpperUpdateCost(upperUpdate);
        Rt.p(" upper cost = " + upperUpdate);
        
        IndexTuningOutput output = div.solve();
        Rt.p(logger.toString());
        
        if (isExportToFile)
            div.exportCplexToFile(en.getWorkloadsFoldername() + "/test.lp");
        
        double updateCost;
        double queryCost;
        
        if (output != null) {
            divConf = (DivConfiguration) output;
            Rt.p(divConf.indexAtReplicaInfo());
            
            WorkloadCostDetail wc;
            if (isShowOptimizerCost) {
                queryCost = div.computeOptimizerQueryCost(divConf);
                updateCost = div.computeOptimizerUpdateCost(divConf);
                wc = new WorkloadCostDetail(queryCost, updateCost, 
                                            queryCost + updateCost);    
            }
            else {
                // TODO: derive query cost and update cost
                Rt.p("temporary NOT COMPUTE DB2 COST");
                wc = new WorkloadCostDetail(-1, -1, div.getObjValue());
            }
            
            Rt.p(" n = " + _n + " B = " + _B
                    + " cost in INUM = "
                    + div.getObjValue());
            Rt.p(" cost in DB2 = " + wc.totalCost);
            Rt.p(" REPLICA IMBALANCE = " + div.getNodeImbalance());
            Rt.p(" ROUTING QUERIES = " + div.computeNumberQueriesSpecializeForReplica());
            return wc;
        }
        else {
            Rt.p(" NO SOLUTION ");
            return null;
        }
    }
}
