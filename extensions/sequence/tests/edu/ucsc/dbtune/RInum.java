package edu.ucsc.dbtune;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
//import edu.ucsc.dbtune.bip.sim.SimBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class RInum {
    private static DatabaseSystem inum;
    private static DatabaseSystem dbms;
    private static Environment en;

    public static void main(String[] args) throws Exception, IOException {
        en = Environment.getInstance();
        en.setProperty("optimizer", "inum");
        inum = DatabaseSystem.newDatabaseSystem(en);
        en.setProperty("optimizer", "dbms");
        dbms = DatabaseSystem.newDatabaseSystem(en);
        String workloadFile = en
                .getScriptAtWorkloadsFolder("tpch/workload_seq.sql");
        FileReader fileReader = new FileReader(workloadFile);
        Workload workload = new Workload("a", fileReader);
        Optimizer inumOptimizer = inum.getOptimizer();
        Optimizer dbmsOptimizer = dbms.getOptimizer();

        Set<Index> allIndexes = new HashSet<Index>();
        for (SQLStatement stmt : workload) {
            if (stmt.getSQL().trim().startsWith("exit"))
                break;
            if (!stmt.getSQL().startsWith("select"))
                continue;
            Set<Index> stmtIndexes = inumOptimizer.recommendIndexes(stmt);
            next: for (Index newIdx : stmtIndexes) {
                for (Index oldIdx : allIndexes) {
                    if (oldIdx.equalsContent(newIdx)) {
                        continue next;
                    }
                }
                allIndexes.add(newIdx);
            }
        }

        Index[] indexs = allIndexes.toArray(new Index[allIndexes.size()]);
        for (Index index : indexs) {
            Rt.np("Index : " + index.columns());
        }

        // Index[] indexs = { new Index(new Column(new Table(new Schema(
        // new Catalog(""), "tpch"), "lineitem"), "l_orderkey"), true), };
        // allIndexes.clear();
        // allIndexes.add(indexs[0]);

        for (SQLStatement stmt : workload) {
            if (stmt.getSQL().trim().startsWith("exit"))
                break;
            Rt.np("/***********************************");
            Rt.np(stmt.getSQL());
            Rt.np("***********************************/");
            RTimer timer = new RTimer();
            ExplainedSQLStatement withoutIndex = dbmsOptimizer.explain(stmt);
            timer.finish("without Index");
            timer.reset();
            ExplainedSQLStatement dbmsPlan = dbmsOptimizer.explain(stmt,
                    allIndexes);
            timer.finish("dbms");
            timer.reset();
            ExplainedSQLStatement inumPlan = inumOptimizer.prepareExplain(stmt)
                    .explain(allIndexes);
            timer.finish("inum");
            timer.reset();
            Rt.np("             %15s\t%15s\t%15s", "withoutIndex", "inum",
                    "whatif");
            Rt.np("Update Cost: %15f\t%15f\t%15f",
                    withoutIndex.getUpdateCost(), inumPlan.getUpdateCost(),
                    dbmsPlan.getUpdateCost());
            Rt.np("Select Cost: %15f\t%15f\t%15f",
                    withoutIndex.getSelectCost(), inumPlan.getSelectCost(),
                    dbmsPlan.getSelectCost());
            Rt.np("Total  Cost: %15f\t%15f\t%15f", withoutIndex.getTotalCost(),
                    inumPlan.getTotalCost(), dbmsPlan.getTotalCost());
            if (indexs.length > 0) {
                Rt.np("Update Index: %15f\t%15f\t%15f", withoutIndex
                        .getUpdateCost(indexs[0]), inumPlan
                        .getUpdateCost(indexs[0]), dbmsPlan
                        .getUpdateCost(indexs[0]));
                Rt.np("Index Used: %15s\t%15s\t%15s", withoutIndex
                        .isUsed(indexs[0]) ? "yes" : "no", inumPlan
                        .isUsed(indexs[0]) ? "yes" : "no", dbmsPlan
                        .isUsed(indexs[0]) ? "yes" : "no");
            }
            Rt.np("%f%%", inumPlan.getTotalCost() * 100
                    / dbmsPlan.getTotalCost());
            // Rt.np(inumOptimizer.getClass().getName());
            // show(inumPlan.getPlan(), inumPlan.getPlan().getRootElement(), 0);
            // Rt.np(dbmsPlan.getClass().getName());
            // show(dbmsPlan.getPlan(), dbmsPlan.getPlan().getRootElement(), 0);
            // show(withoutIndex.getPlan(), withoutIndex.getPlan()
            // .getRootElement(), 0);
        }
    }

    static void show(SQLStatementPlan plan, Operator node, int indention) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < indention; i++)
            sb.append("    ");
        String s = sb.toString();
        String name = node.toString().trim();
        name = s + name.replaceAll("\n", "\n" + s);
        System.out.println(name);
        for (Operator operator : plan.getChildren(node)) {
            show(plan, operator, indention + 1);
        }
        assert (true);
    }
}
