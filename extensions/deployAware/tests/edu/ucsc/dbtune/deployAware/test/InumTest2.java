package edu.ucsc.dbtune.deployAware.test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.FileReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

public class InumTest2 {
	public static double getCost(String line) {
		int t = line.indexOf("cost=");
		line = line.substring(t + 5);
		line = line.substring(0, line.indexOf(' '));
		return Double.parseDouble(line);
	}

	public static void main(String[] args) throws Exception {
		if (true) {
			Rx rx = Rx.findRoot(Rt.readResourceAsString(InumTest2.class,
					"nlj.txt"));
			int count=0;
			for (Rx q : rx.findChilds("q")) {
				Rx plan = q.findChild("plan");
				String s = plan.getText();
				String[] ss = s.split("\n");
				for (int i = 0; i < ss.length; i++) {
					int t = ss[i].indexOf("NESTED.LOOP.JOIN");
					if (t > 0) {
						String nlj = ss[i];
						String left = null;
						String right = null;
						for (int j = i + 1; j < ss.length; j++) {
							if (ss[j].charAt(t) == '├') {
								if (left != null)
									throw new Error();
								left = ss[j];
							}
							if (ss[j].charAt(t) == '└') {
								right = ss[j];
								break;
							}
						}
						double ratio = getCost(nlj)
								/ (getCost(left) + getCost(right));
//						if (Math.abs(ratio - 1) > 0.01) {
							Rt.np("nlj/(left+right)=%.5f", ratio);
							Rt.np(nlj);
							Rt.np(left);
							Rt.np(right);
//							Rt.p(count++);
//							Rt.np(s);
//						}
					}
				}
			}
			System.exit(0);
		}
		Environment en = Environment.getInstance();
		String dbName = "test";
		en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
		en.setProperty("username", "db2inst1");
		en.setProperty("password", "db2inst1admin");
		en.setProperty("workloads.dir", "resources/workloads/db2");
		Rt.p(en.getProperty("jdbc.url"));
		DatabaseSystem db = newDatabaseSystem(en);

		Workload workload = new Workload("", new StringReader("select\n"
				+ "l_shipmode,\n" + "sum(case\n"
				+ "when o_orderpriority = '1-URGENT'\n"
				+ "or o_orderpriority = '2-HIGH'\n" + "then 1\n" + "else 0\n"
				+ "end) as high_line_count,\n" + "sum(case\n"
				+ "when o_orderpriority <> '1-URGENT'\n"
				+ "and o_orderpriority <> '2-HIGH'\n" + "then 1\n" + "else 0\n"
				+ "end) as low_line_count\n" + "from\n" + "tpch.orders,\n"
				+ "tpch.lineitem\n" + "where\n" + "o_orderkey = l_orderkey\n"
				+ "and l_shipmode in ('FOB', 'REG AIR')\n"
				+ "and l_commitdate < l_receiptdate\n"
				+ "and l_shipdate < l_commitdate\n"
				+ "and l_receiptdate >= '1993-01-01'\n"
				+ "and l_receiptdate < '1994-01-01'\n" + "group by\n"
				+ "l_shipmode\n" + "order by\n" + "l_shipmode;\n"));
		// DATTest2.workloadName = "tpch-inum";
		Set<Index> indexes = DATTest2.getIndexes(workload, db);
		// workload = DATTest2.getWorkload(en);
		Index index2 = new Index(indexes.toArray(new Index[1])[0].columns()
				.get(0), true);
		indexes.clear();
		indexes.add(index2);
		for (Index index : indexes) {
			Rt.np(index);
		}

		InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
		DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
		ExplainedSQLStatement st = db2optimizer.explain(workload.get(0),
				indexes);
		Rt.np(st);
		db.getConnection().close();
	}
}
