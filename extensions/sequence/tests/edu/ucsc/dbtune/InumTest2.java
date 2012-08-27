package edu.ucsc.dbtune;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;
import static edu.ucsc.dbtune.util.InumUtils.extractInterestingOrders;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;
import edu.ucsc.dbtune.inum.AbstractSpaceComputation;
import edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor;
import edu.ucsc.dbtune.inum.InumInterestingOrder;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainTables;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.ResultTable;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class InumTest2 {

    public static double getCost(String line) {
        int t = line.indexOf("cost=");
        line = line.substring(t + 5);
        line = line.substring(0, line.indexOf(' '));
        return Double.parseDouble(line);
    }

    public static double getRows(String line) {
        int t = line.indexOf("rows=");
        line = line.substring(t + 5);
        line = line.substring(0, line.indexOf(' '));
        return Double.parseDouble(line);
    }

    static void ibg(PrintStream ps, SQLStatement statement, Optimizer delegate,
            Set<Index> indexes) throws SQLException {
        if (indexes.isEmpty())
            return;

        Rt.np(indexes.size());
        ExplainedSQLStatement estmt = delegate.explain(statement, indexes);
        if (estmt.getPlan().contains(NLJ)) {
            ps.println("<q><query>"
                    + statement.getSQL().replaceAll("<", "&lt;") + "</query>");
            for (Index usedIndex : estmt.getPlan().getIndexes()) {
                ps.println("<index>" + usedIndex + "</index>");
            }
            ps.println("<plan>" + estmt.getPlan() + "</plan></q>");
        }
        for (Index usedIndex : estmt.getPlan().getIndexes()) {
            Set<Index> conf = new HashSet<Index>();
            conf.addAll(indexes);
            conf.remove(usedIndex);
            ibg(ps, statement, delegate, conf);
        }
    }

    static void analyzeNLJ() throws Exception {
        Rx rx = Rx
                .findRoot(Rt.readResourceAsString(InumTest2.class, "nlj.txt"));
        int count = 0;
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
                    // if (Math.abs(ratio - 1) > 0.01) {
                    Rt
                            .np(
                                    "nlj/(left+right)=%.5f rows(left=%.0f, right=%.0f, after_join=%.0f)",
                                    ratio, getRows(left), getRows(right),
                                    getRows(nlj));
                    Rt.np(nlj);
                    Rt.np(left);
                    Rt.np(right);
                    Rt.np();
                    // Rt.p(count++);
                    // Rt.np(s);
                    // }
                }
            }
        }
        System.exit(0);
    }

    private static void findNLJOperators(DatabaseSystem db, Workload workload,
            DB2Optimizer db2optimizer) throws Exception {
        // dump = true;
        // find all NLJ operators in TPCH
        PrintStream ps = new PrintStream("/home/wangrui/dbtune/allTPCH_NLJ.txt");
        for (int i = 0; i < workload.size(); i++) {
            Rt.p(i);
            SQLStatement statement = workload.get(i);
            Set<InumInterestingOrder> set = new DerbyInterestingOrdersExtractor(
                    db.getCatalog()).extract(statement);
            ibg(ps, statement, db2optimizer, new HashSet<Index>(set));
            // CandidateGenerator candGen = new
            // OptimizerCandidateGenerator(
            // getBaseOptimizer(db.getOptimizer()));
            // Set<Index> indexes = candGen.generate(workload);
            // ibg(ps, statement, db2optimizer, indexes);
        }
        ps.close();
        System.exit(0);
    }

    static Index createIndex(DatabaseSystem db, String cs) throws SQLException {
        if (cs.startsWith("["))
            cs = cs.substring(1);
        if (cs.startsWith("+"))
            cs = cs.substring(1);
        if (cs.endsWith("]"))
            cs = cs.substring(0, cs.length() - 1);
        String[] columns = cs.split("\\+");
        Vector<Column> v = new Vector<Column>();
        HashMap<Column, Boolean> map = new HashMap<Column, Boolean>();
        for (int i = 0; i < columns.length; i++) {
            String s = columns[i];
            String cname = s.substring(0, s.indexOf('('));
            Column c = (Column) db.getCatalog().findByQualifiedName(cname);
            if (c == null)
                throw new Error(cname);
            v.add(c);
            map.put(c, "(A)".equals(s.substring(s.indexOf('('))));
        }
        return new Index(v, map);
    }

    Set<Index>[] makeTestSet(Set<Index> input) throws Exception {
        Index[] indexs = input.toArray(new Index[input.size()]);
        if (indexs.length > 10) {
            for (Index index : indexs)
                Rt.np(index);
            throw new Error("" + indexs.length);
        }
        int n = (int) Math.pow(2, indexs.length);
        Set<Index>[] sets = new Set[n];
        for (int i = 0; i < n; i++) {
            HashSet<Index> hashSet = new HashSet<Index>();
            for (int j = 0; j < indexs.length; j++) {
                if (((i >> j) & 1) != 0) {
                    hashSet.add(indexs[j]);
                }
            }
            sets[i] = hashSet;
        }
        return sets;
    }

    void compareWorkload(DatabaseSystem db, String query, String name)
            throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        double db2indexAll = 0;
        double db2ftsAll = 0;
        double inumIndexAll = 0;
        double inumFtsAll = 0;
        Workload workload = new Workload("", new StringReader(query));
        CandidateGenerator candGen = new OptimizerCandidateGenerator(
                getBaseOptimizer(db.getOptimizer()));
        Set<Index> indexes = candGen.generate(workload);
        // for (Index index : indexes) {
        // Rt.np(index);
        // }

        // AbstractSpaceComputation.setInumSpacePopulateIndexSet(indexes);

        Rt.p(name);
        Rt.p("queries: " + workload.size());
        Rt.p("indexes: " + indexes.size());
        for (int i = 0; i < workload.size(); i++) {
            ExplainedSQLStatement db2plan = db2optimizer.explain(workload
                    .get(i), indexes);
            double db2index = db2plan.getTotalCost();
            db2indexAll += db2index;

            db2plan = db2optimizer.explain(workload.get(i));
            double db2fts = db2plan.getTotalCost();
            db2ftsAll += db2fts;

            InumPreparedSQLStatement space;
            space = (InumPreparedSQLStatement) optimizer
                    .prepareExplain(workload.get(i));
            ExplainedSQLStatement inumPlan = space.explain(indexes);
            double inumIndex = inumPlan.getTotalCost();
            SQLStatementPlan plan = inumPlan.getPlan();
            InumPlan inumPlan2 = (InumPlan) plan.templatePlan;
            inumPlan = optimizer.prepareExplain(workload.get(i)).explain(
                    new HashSet<Index>());
            double inumFts = inumPlan.getTotalCost();

            Rt.np("query=%d\tDB2(FTS)=%,.0f\tINUM(FTS)=%,.0f"
                    + "\tDB2(Index)=%,.0f\tINUM(Index)=%,.0f\tINUM/DB2=%.2f",
                    i, db2fts, inumFts, db2index, inumIndex, inumIndex
                            / db2index);
            inumIndexAll += inumIndex;
            inumFtsAll += inumFts;
        }
        Rt.np("DB2(FTS)=%,.0f", db2ftsAll);
        Rt.np("INUM(FTS)=%,.0f", inumFtsAll);
        Rt.np("DB2(Index)=%,.0f", db2indexAll);
        Rt.np("INUM(Index)=%,.0f", inumIndexAll);
    }

    void compareSubset(DatabaseSystem db, String query, String testName)
            throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        double db2indexAll = 0;
        double inumIndexAll = 0;
        Workload workload = new Workload("", new StringReader(query));

        // AbstractSpaceComputation.setInumSpacePopulateIndexSet(indexes);

        Rt.p("queries: " + workload.size());
        PrintStream ps = new PrintStream("/home/wangrui/dbtune/inum/cost/"
                + testName + ".txt");
        for (int i = 0; i < workload.size(); i++) {
            Workload workload2 = new Workload("", new StringReader(workload
                    .get(i).getSQL()
                    + ";"));
            CandidateGenerator candGen = new OptimizerCandidateGenerator(
                    getBaseOptimizer(db.getOptimizer()));
            Set<Index> indexes = candGen.generate(workload2);
            Set<Index>[] ss = makeTestSet(indexes);
            int id = 0;
            InumPreparedSQLStatement space;
            space = (InumPreparedSQLStatement) optimizer
                    .prepareExplain(workload.get(i));
            for (Set<Index> index : ss) {
                ExplainedSQLStatement db2plan = db2optimizer.explain(workload
                        .get(i), index);
                double db2index = db2plan.getTotalCost();
                db2indexAll += db2index;

                ExplainedSQLStatement inumPlan = space.explain(index);
                double inumIndex = inumPlan.getTotalCost();
                String s = String
                        .format(
                                "query=%d\tINUM/DB2=%.2f\tDB2=%,.0f\tINUM=%,.0f\tindex=%s",
                                i, inumIndex / db2index, db2index, inumIndex,
                                index.toString());
                ps.println(s);
                Rt.np(id + "/" + ss.length + "\t" + s);
                id++;
                inumIndexAll += inumIndex;
            }
        }
        Rt.np("DB2(Index)=%,.0f", db2indexAll);
        Rt.np("INUM(Index)=%,.0f", inumIndexAll);
        ps.format("DB2(Index)=%,.0f\n", db2indexAll);
        ps.format("INUM(Index)=%,.0f\n", inumIndexAll);
    }

    void tpch(DatabaseSystem db, String query) throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        String[] queries = query.split("--query ");
        // queries = query.split(";");

        PrintStream ps2 = new PrintStream(new FileOutputStream(
                "/home/wangrui/dbtune/inum/all.txt", true));
        for (int testId = 1; testId < queries.length; testId++) {
            String id = queries[testId].substring(0, 2);
            query = queries[testId].substring(2).trim();
            // String id=""+ testId;
            // query=queries[testId]+";";
            // if (query.trim().toLowerCase().startsWith("with"))
            // continue;
            Rt.p(id + "/" + queries.length);
            Rt.p("test " + id);
            Rt.np(query);
            Workload workload = new Workload("", new StringReader(query));
            Set<Index> indexes = new HashSet<Index>();
            if (true) {
                // CandidateGenerator candGen = new OptimizerCandidateGenerator(
                // getBaseOptimizer(db.getOptimizer()));
                // indexes = candGen.generate(workload);
                // AbstractSpaceComputation.setInumSpacePopulateIndexSet(indexes);
                InumPreparedSQLStatement inumPlan = (InumPreparedSQLStatement) optimizer
                        .prepareExplain(workload.get(0));
                String testName = "tpcds_inumspace_interestingorder";
                new File("/home/wangrui/dbtune/inum/" + testName + "/").mkdir();
                File file = new File("/home/wangrui/dbtune/inum/" + testName
                        + "/" + testId + ".xml");
                Rx root = new Rx("query");
                root.setAttribute("id", id);
                inumPlan.save(root);
                Rt.write(file, root.getXml().getBytes());
                continue;
            }
            // query="select ps_suppkey\n" +
            // "from tpch.partsupp where\n" +
            // "    ps_suppkey not in (\n" +
            // "    select\n" +
            // "        s_suppkey\n" +
            // "    from\n" +
            // "        tpch.supplier\n" +
            // "    where\n" +
            // "        s_comment like '%Customer%Complaints%'\n" +
            // "    );";
            // query += ";";

            indexes.addAll(extractInterestingOrders(workload.get(0), db
                    .getCatalog()));

            workload = new Workload("", new StringReader(query));
            for (Index index : indexes) {
                Rt.p(index.toString());
            }
            // |R||S|
            // dump = true;
            ExplainedSQLStatement db2plan = db2optimizer.explain(workload
                    .get(0), indexes);
            ExplainedSQLStatement inumPlan = optimizer.prepareExplain(
                    workload.get(0)).explain(indexes);
            PrintStream ps = new PrintStream("/home/wangrui/dbtune/inum/"
                    + testId + ".txt");
            ps.println("DB2: " + db2plan.getPlan());
            ps.println("INUM: " + inumPlan.getPlan());
            ps.println("INUM TEMPLATE: " + inumPlan.getPlan().templatePlan);
            ps.println("INUM ORG: "
                    + ((InumPlan) inumPlan.getPlan().templatePlan).orgPlan);
            ps.println("DB2: " + db2plan.getTotalCost());
            ps.println("INUM: " + inumPlan.getTotalCost());
            // if (false) {
            indexes.clear();
            // CandidateGenerator candGen = new
            // PowerSetOptimalCandidateGenerator(
            // db.getOptimizer(), new OptimizerCandidateGenerator(
            // getBaseOptimizer(db.getOptimizer())), 2);
            // indexes = candGen.generate(workload);

            CandidateGenerator candGen = new OptimizerCandidateGenerator(
                    getBaseOptimizer(db.getOptimizer()));
            indexes = candGen.generate(workload);
            // AbstractSpaceComputation.setInumSpacePopulateIndexSet(indexes);
            ExplainedSQLStatement db2plan2 = db2optimizer.explain(workload
                    .get(0), indexes);
            ExplainedSQLStatement inumPlan2 = optimizer.prepareExplain(
                    workload.get(0)).explain(indexes);
            // Rt.p(db2plan2);
            ps.println("RECOMMEND_DB2: " + db2plan2.getPlan());
            ps.println("RECOMMEND_INUM: " + inumPlan2.getPlan());
            ps.println("RECOMMEND_INUM TEMPLATE: "
                    + inumPlan2.getPlan().templatePlan);
            ps.println("RECOMMEND_INUM ORG: "
                    + ((InumPlan) inumPlan2.getPlan().templatePlan).orgPlan);
            ps.println("RECOMMEND_DB2: " + db2plan2.getTotalCost());
            ps.println("RECOMMEND_INUM: " + inumPlan2.getTotalCost());
            ps.close();
            // }
            ps2
                    .format(
                            "query %s\tDB2(I1)=%,.0f\tINUM(I1)=%,.0f\tDB2(I2)=%,.0f\tINUM(I2)=%,.0f\n",
                            id, db2plan.getTotalCost(),
                            inumPlan.getTotalCost(), db2plan2.getTotalCost(),
                            inumPlan2.getTotalCost());
        }
        ps2.close();
    }

    public InumTest2() throws Exception {
        Environment en = Environment.getInstance();
        String dbName = "test";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        DatabaseSystem test = newDatabaseSystem(en);
        dbName = "tpch10g";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        DatabaseSystem tpch10g = newDatabaseSystem(en);

        String tpch = Rt.readFile(new File(
                "resources/workloads/db2/tpch/complete.sql"));
        String tpcds = Rt.readFile(new File(
                "resources/workloads/db2/tpcds-inum/workload.sql"));
        // query = Rt.readFile(new File(
        // "resources/workloads/db2/tpch-inum/workload.sql"));
//        sqlTest(tpch10g, tpch, 3);
        // compareSubset(tpch10g, tpch, "tpch10g");
        // compareSubset(test, tpcds, "tpcds");
        // compareSubset(test, tpch);
        compareWorkload(tpch10g, tpch, "tpch");
        compareWorkload(test, tpcds, "tpcds");
        test.getConnection().close();
        tpch10g.getConnection().close();
    }

    void sqlTest(DatabaseSystem db, String query, int queryId) throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        Set<Index> indexes = new HashSet<Index>();
        // String[] names = Rt.readResourceAsLines(InumTest2.class,
        // "index.txt");
        // for (String name : names) {
        // indexes.add(createIndex(db, name));
        // }
        // ExplainTables.showWarnings = true;

        Workload workload = new Workload("", new StringReader(query));
        CandidateGenerator candGen = new OptimizerCandidateGenerator(
                getBaseOptimizer(db.getOptimizer()));
        indexes = candGen.generate(workload);
        for (Index index : indexes) {
            Rt.np(index);
        }

        String sql = workload.get(queryId).getSQL();
        Rt.p(sql);

//        ExplainTables.dump=true;
        ExplainedSQLStatement db2plan = db2optimizer.explain(sql, indexes);
        Rt.p(db2plan);
//        System.exit(0);
        for (Index index2 : db2plan.getUsedConfiguration())
            Rt.np(index2);
        InumPreparedSQLStatement space;
        // space = (InumPreparedSQLStatement) optimizer
        // .prepareExplain(new SQLStatement(sql));
        space = (InumPreparedSQLStatement) optimizer.prepareExplain(workload
                .get(queryId));
        ExplainedSQLStatement inumPlan = space.explain(indexes);
        SQLStatementPlan plan = inumPlan.getPlan();
        Rt.p(plan);
        InumPlan templatePlan = (InumPlan) plan.templatePlan;
        Rt.p(templatePlan);
        Rt.p(templatePlan.orgPlan);
        for (Index index2 : inumPlan.getUsedConfiguration())
            Rt.np(index2);

        Rt.p("%,.0f",db2plan.getTotalCost());
        Rt.p("%,.0f",inumPlan.getTotalCost());
        // Rt.p(plan);

        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        // analyzeNLJ();

        // query = Rt.readFile(new File(
        // "resources/workloads/db2/tpcds/workload.sql"));
        new InumTest2();
    }
}