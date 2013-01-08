package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.util.MetadataUtils;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class RuntimeTest extends DIVPaper 
{
    /**
    *
    * Generate paper results
    */
   @Test
   public void main() throws Exception
   {   
       // 1. initialize file locations & necessary 
       // data structures
       initialize();
       
       // get database instances, candidate indexes
       getEnvironmentParameters();
       setParameters();
       
       double maxCost = -1;
       int maxPos = -1;
       int pos = 0;
       DB2Optimizer opt = (DB2Optimizer) db.getOptimizer().getDelegate();
       double cost;
       SQLStatement maxSql = null;
       for (SQLStatement sql : workload){
           cost = opt.explain(sql).getTotalCost();
           Rt.p(" position = " + pos + " cost = " + cost);
           if (maxCost < cost) {
               maxPos = pos;
               maxCost = cost;
               maxSql = sql;
           }
           
           pos++;
       }
       
       Rt.p("The slowest query = " + maxPos);
       Rt.p("stmt: " + maxSql.getSQL());
       System.exit(1);
       
       List<SQLStatement> sqls = new ArrayList<SQLStatement>();
       sqls.add(workload.get(maxPos));
       Workload wl = new Workload(sqls);
       db2Advis.process(wl);
       Set<Index> recommendation = db2Advis.getRecommendation(-1);
       
       for (Index i : recommendation)
           Rt.p(MetadataUtils.getCreateStatement(i));
       
   }
}
