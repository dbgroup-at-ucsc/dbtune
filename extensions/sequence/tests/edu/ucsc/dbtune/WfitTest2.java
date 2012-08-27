package edu.ucsc.dbtune;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.bip.SebBIPOutput;
import edu.ucsc.dbtune.seq.bip.SeqBIP;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.Wfit;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.bip.def.SeqInumPlan;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlot;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlotIndexCost;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

public class WfitTest2 {
    public static void testBIP() throws Exception {
        BipTest2.times = 10;
//        BipTest2.times = 1;
//        BipTest2.indexSize = 40;
        BipTest2.indexSize = 10;
        RTimerN timer = new RTimerN();
        BipTest2.en = Environment.getInstance();
        BipTest2.db = newDatabaseSystem(BipTest2.en);

        Workload workload = BipTest2.getWorkload(BipTest2.en);
        Set<Index> indexes = BipTest2.getIndexes(workload, BipTest2.db);

        InumOptimizer optimizer = (InumOptimizer) BipTest2.db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        timer.reset();
        SeqInumCost cost = SeqInumCost.fromInum(BipTest2.db,optimizer, null, indexes);
        Rt.p("%d x %d", workload.size(), cost.indices.size());
        Wfit wfit = new Wfit(cost);
        wfit.setOptimizer(optimizer);
        LogListener logger = LogListener.getInstance();
        wfit.setLogListenter(logger);
        wfit.setWorkload(new Workload("", new StringReader("")));
        SebBIPOutput output = (SebBIPOutput) wfit.solve();
        for (int i = 0; i < workload.size(); i++) {
            wfit.addQuery(workload.get(i));
        }
        wfit.allPossibility();
        System.exit(0);
    }
}
