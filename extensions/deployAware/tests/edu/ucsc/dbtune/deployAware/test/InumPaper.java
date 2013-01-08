package edu.ucsc.dbtune.deployAware.test;

import java.io.File;

import ilog.concert.IloException;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.deployAware.DATParameter;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
import edu.ucsc.dbtune.seq.bip.WorkloadLoaderSettings;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.util.Rt;

public class InumPaper {
    public static void main(String[] args) throws Exception {
        WorkloadLoader.cacheRoot = new File(
                WorkloadLoaderSettings.dataRoot+"/paper/cache/notprune");
        WorkloadLoader.cacheRoot = new File(WorkloadLoaderSettings.dataRoot+"/paper/cache/__tmp");
//        WorkloadLoader.cacheRoot = new File(WorkloadLoaderSettings.dataRoot+"/paper/cache/prune2");
        WorkloadLoader loader = new WorkloadLoader("test", "deployAware",
                "TPCH16.sql", "recommend");
//        WorkloadLoader loader = new WorkloadLoader("test", "deployAware",
//                "TPCDS63.sql", "recommend");

        SeqInumCost cost = loader.loadCost();
        Rt.p(cost.totalPlans());
        Rt.p(cost.totalSlots());
        Rt.p(cost.totalIndexAccessCosts());
        
        int m = 5;
        double alpha = 0.5;
        double beta = 0.5;
        int l = 20;
        double[] windowConstraints = new double[m];
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = 100000000;
        DATParameter params = new DATParameter(cost, windowConstraints, alpha,
                beta, l);
        cost.storageConstraint = 20 * 1024 * 1024 * 1024;

        DAT dat = new DAT();
        RTimer timer = new RTimer();
        DATOutput output = dat.runDAT(params);
        timer.finish("runDat");
    }

}
