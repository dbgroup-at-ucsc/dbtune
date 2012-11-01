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
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.DB2AdvisorCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;
import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.advisor.db2.DB2IndexInfo;
import edu.ucsc.dbtune.inum.AbstractSpaceComputation;
import edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor;
import edu.ucsc.dbtune.inum.IBGSpaceComputation;
import edu.ucsc.dbtune.inum.InumInterestingOrder;
import edu.ucsc.dbtune.inum.prune.InumSpacePrune;
import edu.ucsc.dbtune.inum.prune.PrunableInumPlan;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainTables;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.InumUtils;
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

    static void formatTpcds() throws Exception {
        String tpcds = Rt.readFile(new File(
                "resources/workloads/db2/tpcds/db2.sql"));
        String[] ss = { "customer_address", "customer_demographics",
                "date_dim", "warehouse", "ship_mode", "time_dim", "reason",
                "income_band", "item", "store", "call_center", "customer",
                "web_site", "store_returns", "household_demographics",
                "web_page", "promotion", "catalog_page", "inventory",
                "catalog_returns", "web_returns", "web_sales", "catalog_sales",
                "store_sales", };
        for (String s : ss) {
            next: while (true) {
                for (int i = 0; i < tpcds.length() - s.length(); i++) {
                    if (tpcds.substring(i).startsWith(s)) {
                        char c1 = tpcds.charAt(i - 1);
                        char c2 = tpcds.charAt(i + s.length());
                        if ((c1 == ' ' || c1 == ',' || c1 == '(' || c1 == '\n' || c1 == '\t')
                                && (c2 == ' ' || c2 == '.' || c2 == ','
                                        || c2 == '\n' || c2 == '\t')) {
                            tpcds = tpcds.substring(0, i) + "tpcds."
                                    + tpcds.substring(i);
                            continue next;
                        }
                    }
                }
                break;
            }
        }
        Rt.np(tpcds);
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
        Table table = null;
        if (db == null)
            table = new Table(new Schema(new Catalog(""), ""), "");
        for (int i = 0; i < columns.length; i++) {
            String s = columns[i];
            String cname = s.substring(0, s.indexOf('('));
            Column c = null;
            if (db != null)
                c = (Column) db.getCatalog().findByQualifiedName(cname);
            else
                c = new Column(table, cname);
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

        if (false) {
            // test sql
            CandidateGenerator candGen = new OptimizerCandidateGenerator(
                    getBaseOptimizer(db.getOptimizer()));
            for (int i = 512; i < workload.size(); i++) {
                Workload workload2 = new Workload("", new StringReader(workload
                        .get(i).getSQL()
                        + ";"));
                Rt.np(i);
                Rt.np(workload2.get(0).getSQL());
                Set<Index> indexes = candGen.generate(workload2);
                for (Index index : indexes) {
                    Rt.np(index);
                }
            }
            System.exit(0);
        }

        // Statement st=db.getConnection().createStatement();
        // Rt.np("creating index");
        // st.execute("drop index r1");
        // st.execute("drop index r2");
        // st.execute("create index r1 on TPCH.ORDERS (O_ORDERKEY)");
        // st.execute("create index r2 on TPCH.LINEITEM (L_ORDERKEY)");
        // Rt.np("finished index");
        Rt.error("load index");
        Set<Index> indexes = new HashSet<Index>();
        String[] names = Rt.readResourceAsLines(InumTest2.class, "iplant.txt");
        for (String name2 : names) {
            indexes.add(createIndex(db, name2));
        }

        // workload = new Workload("", new
        // StringReader(workload.get(2).getSQL()+";"));
        // CandidateGenerator candGen = new OptimizerCandidateGenerator(
        // getBaseOptimizer(db.getOptimizer()));
        // Set<Index> indexes = candGen.generate(workload);
        // for (Index index : indexes) {
        // Rt.np(index);
        // }

        // DB2Advisor db2Advis = new DB2Advisor(db);
        // db2Advis.process(workload);
        // Set<Index> indexes = db2Advis.getRecommendation(100000);

        // AbstractSpaceComputation.setInumSpacePopulateIndexSet(indexes);

        Rt.p(name);
        Rt.p("queries: " + workload.size());
        Rt.p("indexes: " + indexes.size());
        IBGSpaceComputation.maxTime = 15000;
        for (int i = 0; i < workload.size(); i++) {
            // Rt.p("start db2");
            try {
                ExplainedSQLStatement db2plan = db2optimizer.explain(workload
                        .get(i), indexes);
                double db2index = db2plan.getTotalCost();
                db2plan = db2optimizer.explain(workload.get(i));
                double db2fts = db2plan.getTotalCost();

                // Rt.p("start inum");
                InumPreparedSQLStatement space = (InumPreparedSQLStatement) optimizer
                        .prepareExplain(workload.get(i));
                space.pruneInumSpace(indexes);
                ExplainedSQLStatement inumPlan = space.explain(indexes);
                double inumIndex = inumPlan.getTotalCost();
                SQLStatementPlan plan = inumPlan.getPlan();
                InumPlan inumPlan2 = (InumPlan) plan.templatePlan;
                inumPlan = space.explain(new HashSet<Index>());
                double inumFts = inumPlan.getTotalCost();

                Rt
                        .np(
                                "query=%d\tDB2(FTS)=%,.0f\tINUM(FTS)=%,.0f"
                                        + "\tDB2(Index)=%,.0f\tINUM(Index)=%,.0f\tINUM/DB2=%.2f",
                                i, db2fts, inumFts, db2index, inumIndex,
                                inumIndex / db2index);
                if (0.9 <= inumIndex / db2index && inumIndex / db2index <= 1.2) {
                    // Rt.np(workload.get(i).getSQL() + ";");
                } else {
                    // Rt.np("-- " + workload.get(i).getSQL() + ";");
                }
                db2indexAll += db2index;
                db2ftsAll += db2fts;
                inumIndexAll += inumIndex;
                inumFtsAll += inumFts;
            } catch (Exception e) {
                e.printStackTrace();
                // Rt.np("-- query=%d %s", i, e.getMessage());
                // Rt.np("-- " + workload.get(i).getSQL() + ";");
            }
            // Rt.np("query=%d\tDB2(FTS)=%,.0f" + "\tDB2(Index)=%,.0f", i,
            // db2fts,
            // db2index);
        }
        Rt.np("DB2(FTS)=%,.0f", db2ftsAll);
        Rt.np("INUM(FTS)=%,.0f", inumFtsAll);
        Rt.np("DB2(Index)=%,.0f", db2indexAll);
        Rt.np("INUM(Index)=%,.0f", inumIndexAll);
        Rt.np("Derby failed count=%,d",
                AbstractSpaceComputation.derbyFailedCount);
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

    void compareInterestingOrders(DatabaseSystem db, String query)
            throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();

        Workload workload = new Workload("", new StringReader(query));
        for (int queryId = 0; queryId < workload.size(); queryId++) {
            Rt.np("query " + queryId);
            Set<InumInterestingOrder> orders = InumUtils
                    .extractInterestingOrders(workload.get(queryId), db
                            .getCatalog());
            Set<InumInterestingOrder> orders2 = AbstractSpaceComputation
                    .extractInterestingOrderFromDB(workload.get(queryId),
                            db2optimizer);
            for (InumInterestingOrder o : orders) {
                if (!orders2.contains(o))
                    Rt.np("MISSING " + o);
            }
            for (InumInterestingOrder o : orders2) {
                if (!orders.contains(o))
                    Rt.np("INCORRECT " + o);
            }
        }
        System.exit(0);
    }

    public InumTest2() throws Exception {
        Environment en = Environment.getInstance();
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        DatabaseSystem test = null, tpch10g = null, iplant = null;
        String dbName = "test";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        // test = newDatabaseSystem(en);
        dbName = "tpch10g";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        // tpch10g = newDatabaseSystem(en);
        dbName = "iplant";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        iplant = newDatabaseSystem(en);

        String tpch = Rt.readFile(new File(
                "resources/workloads/db2/tpch/complete.sql"));
        String tpcds = Rt.readFile(new File(
                "resources/workloads/db2/tpcds/db2.sql"));
        String tpcds40 = Rt.readFile(new File(
                "resources/workloads/db2/deployAware/tpcds_40.sql"));
        String update = Rt.readFile(new File(
                "resources/workloads/db2/tpcds/update.sql"));
        String updateT = Rt.readFile(new File(
                "resources/workloads/db2/tpch-benchmark-mix/update-only.sql"));
        String otab = Rt.readFile(new File(
                "resources/workloads/db2/online-benchmark-100/workload.sql"));
        String iplantWorkload = Rt.readFile(new File(
                "resources/workloads/db2/iplant/workload.sql"));
        // sqlTest(tpch10g, tpch, 1);
        // sqlTest(test, update, 0);
        // sqlTest(test, update, 15);
        // pruneTest(test, update);
        // sqlTest(test, tpcds40, 33);
        // sqlTest(
        // test,
        // "SELECT 3, COUNT(*)  FROM tpch.lineitem WHERE  tpch.lineitem.l_tax BETWEEN 0.01596699754645524 AND 0.029823160830179565 AND tpch.lineitem.l_extendedprice BETWEEN 19178.598547906164 AND 19756.721297981876 AND tpch.lineitem.l_receiptdate BETWEEN 'Thu Mar 24 14:48:29 PST 1994' AND 'Mon Jan 30 14:48:29 PST 1995';",
        // 0);
        // compareSubset(tpch10g, tpch, "tpch10g");
        // compareSubset(test, tpcds, "tpcds");
        // compareSubset(test, tpch);
        // compareSubset(test, update,"update");
        // compareWorkload(test, tpch, "tpch");
        // compareInterestingOrders(test, tpch);
        // compareWorkload(tpch10g, tpch, "tpch10g");
        // compareWorkload(test, tpcds40, "tpcds40");
        // compareWorkload(test, update, "update");
        compareWorkload(iplant, iplantWorkload, "iplantWorkload");
        // compareWorkload(tpch10g, updateT, "update");
        // compareWorkload(test, tpcds, "tpcds");
        // compareWorkload(test, update, "update");
        if (test != null)
            test.getConnection().close();
        if (tpch10g != null)
            tpch10g.getConnection().close();
        if (iplant != null)
            iplant.getConnection().close();
    }

    void pruneTest(DatabaseSystem db, String query) throws Exception {
        InumOptimizer optimizer = db == null ? null : (InumOptimizer) db
                .getOptimizer();
        // Set<Index> indexes = new HashSet<Index>();
        // Rt.error("load index");
        // String[] names = Rt.readResourceAsLines(InumTest2.class,
        // "tpcds40index.txt");
        // for (String name : names) {
        // indexes.add(createIndex(db, name));
        // }
        // ExplainTables.showWarnings = true;

        Workload workload = new Workload("", new StringReader(query));

        Set<Index> indexes = new HashSet<Index>();
        CandidateGenerator candGen = new OptimizerCandidateGenerator(
                getBaseOptimizer(db.getOptimizer()));
        indexes = candGen.generate(workload);
        Rt.np("Generated Indexes:");
        for (Index index : indexes) {
            Rt.np(index);
        }

        Hashtable<String, Index> map = new Hashtable<String, Index>();
        for (Index index : indexes)
            map.put(index.toString(), index);
        int[] count = new int[3];
        int[] count1 = new int[3];
        int[] count2 = new int[3];
        int[] count3 = new int[3];
        for (int i = 0; i < workload.size(); i++) {

            File file = new File("/home/wangrui/dbtune/inum/prune/update/" + i
                    + ".xml");
            file.getParentFile().mkdirs();
            if (!file.exists()) {
                InumPreparedSQLStatement space = (InumPreparedSQLStatement) optimizer
                        .prepareExplain(workload.get(i));
                PrunableInumPlan[] plans = InumSpacePrune.getPrunableInumSpace(
                        space.getTemplatePlans(), indexes);
                InumSpacePrune.save(file, plans);
            }
            PrunableInumPlan[] plans = InumSpacePrune.load(db == null ? null
                    : db.getCatalog(), file, map);
            int[] is = InumSpacePrune.statistics(plans);
            for (int j = 0; j < 3; j++)
                count[j] += is[j];
            plans = InumSpacePrune.prune1(plans, indexes);
            is = InumSpacePrune.statistics(plans);
            for (int j = 0; j < 3; j++)
                count1[j] += is[j];
            plans = InumSpacePrune.prune2(plans, indexes);
            is = InumSpacePrune.statistics(plans);
            for (int j = 0; j < 3; j++)
                count2[j] += is[j];
            plans = InumSpacePrune.prune3(plans, indexes);
            is = InumSpacePrune.statistics(plans);
            for (int j = 0; j < 3; j++)
                count3[j] += is[j];
            Rt.np(i + " " + plans.length);
            for (PrunableInumPlan plan : plans) {
                plan.print();
            }
            // break;
            // HashSet<InumPlan> inumSpace = new HashSet<InumPlan>();
            // for (PrunableInumPlan p : plans)
            // inumSpace.add(p);
            // InumPreparedSQLStatement space;
            // space = (InumPreparedSQLStatement) optimizer.prepareExplain(
            // workload.get(i), inumSpace);
            // ExplainedSQLStatement inumPlan = space.explain(indexes);
            // double inumIndex = inumPlan.getTotalCost();
            // inumPlan = space.explain(new HashSet<Index>());
            // double inumFts = inumPlan.getTotalCost();
            //
            // Rt.np("query=%d\tINUM(FTS)=%,.0f\tINUM(Index)=%,.0f", i, inumFts,
            // inumIndex);
        }
        Rt.np("Without prune: plans=%d\tslots=%,d\tslot-index=%,d", count[0],
                count[1], count[2]);
        Rt.np("Best vs worst: plans=%d\tslots=%,d\tslot-index=%,d\t%.0f%%",
                count1[0], count1[1], count1[2], 100 - (double) count1[2]
                        / count[2] * 100);
        Rt.np("Remove index : plans=%d\tslots=%,d\tslot-index=%,d\t%.0f%%",
                count2[0], count2[1], count2[2], 100 - (double) count2[2]
                        / count1[2] * 100);
        Rt.np("Plan covering: plans=%d\tslots=%,d\tslot-index=%,d\t%.0f%%",
                count3[0], count3[1], count3[2], 100 - (double) count3[2]
                        / count2[2] * 100);

        System.exit(0);
    }

    void indexTest(DatabaseSystem db) throws Exception {
        Statement st = db.getConnection().createStatement();
        st.execute("select max() from ");

        st
                .execute("delete from tpcds.customer_address where CA_ADDRESS_SK=25000001");

        RTimerN timer = new RTimerN();
        st.execute("INSERT INTO tpcds.customer_address \n" + "(\n"
                + "    CA_ADDRESS_SK,\n" + "    CA_ADDRESS_ID,\n"
                + "    CA_STREET_NUMBER,\n" + "    CA_STREET_NAME,\n"
                + "    CA_STREET_TYPE,\n" + "    CA_SUITE_NUMBER,\n"
                + "    CA_CITY,\n" + "    CA_COUNTY,\n" + "    CA_STATE,\n"
                + "    CA_ZIP,\n" + "    CA_COUNTRY,\n"
                + "    CA_GMT_OFFSET,\n" + "    CA_LOCATION_TYPE\n"
                + ") values (\n" + "    25000001,\n"
                + "    'AAAAAAAACLAJAAAA',\n" + "    '326       ',\n"
                + "    'Chestnut Main',\n" + "    'Ln             ',\n"
                + "    'Suite I   ',\n" + "    'Spring Hill',\n"
                + "    'Leflore County',\n" + "    'MS',\n"
                + "    '56787     ',\n" + "    'United States',\n"
                + "    -6.00,\n" + "    'apartment           '\n" + ")");
        double time1 = timer.getSecondElapse();
        Rt.p(time1);
        st
                .execute("delete from tpcds.customer_address where CA_ADDRESS_SK=25000001");
        timer.reset();
        st
                .execute("create index tpcds.indexr1 on tpcds.customer_address (CA_ADDRESS_SK)");
        st
                .execute("create index tpcds.indexr2 on tpcds.customer_address (CA_ADDRESS_ID)");
        st
                .execute("create index tpcds.indexr3 on tpcds.customer_address (CA_STREET_NUMBER)");
        time1 = timer.getSecondElapse();
        Rt.p(time1);
        timer.reset();
        st.execute("INSERT INTO tpcds.customer_address \n" + "(\n"
                + "    CA_ADDRESS_SK,\n" + "    CA_ADDRESS_ID,\n"
                + "    CA_STREET_NUMBER,\n" + "    CA_STREET_NAME,\n"
                + "    CA_STREET_TYPE,\n" + "    CA_SUITE_NUMBER,\n"
                + "    CA_CITY,\n" + "    CA_COUNTY,\n" + "    CA_STATE,\n"
                + "    CA_ZIP,\n" + "    CA_COUNTRY,\n"
                + "    CA_GMT_OFFSET,\n" + "    CA_LOCATION_TYPE\n"
                + ") values (\n" + "    25000001,\n"
                + "    'AAAAAAAACLAJAAAA',\n" + "    '326       ',\n"
                + "    'Chestnut Main',\n" + "    'Ln             ',\n"
                + "    'Suite I   ',\n" + "    'Spring Hill',\n"
                + "    'Leflore County',\n" + "    'MS',\n"
                + "    '56787     ',\n" + "    'United States',\n"
                + "    -6.00,\n" + "    'apartment           '\n" + ")");
        time1 = timer.getSecondElapse();
        Rt.p(time1);
        st
                .execute("delete from tpcds.customer_address where CA_ADDRESS_SK=25000001");
        st.execute("drop index tpcds.indexr1");
        st.execute("drop index tpcds.indexr2");
        st.execute("drop index tpcds.indexr3");
        System.exit(0);
    }

    void sqlTest(DatabaseSystem db, String query, int queryId) throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        // indexTest(db);
        Set<Index> indexes = new HashSet<Index>();
        // Rt.error("load index");
        // String[] names = Rt.readResourceAsLines(InumTest2.class,
        // "index.txt");
        // for (String name : names) {
        // indexes.add(createIndex(db, name));
        // }
        // ExplainTables.showWarnings = true;

        // db.getCatalog().findByQualifiedName("TPCDS.INDEXR1");
        Statement st = db.getConnection().createStatement();
        Rt.np("creating index");
        // st
        // .execute("create index tpcds.indexr1 on tpcds.customer_address (CA_ADDRESS_SK)");
        // st
        // .execute("create index tpcds.indexr2 on tpcds.customer_address (CA_ADDRESS_ID)");
        // st
        // .execute("create index tpcds.indexr3 on tpcds.customer_address (CA_STREET_NUMBER)");

        // st.execute("drop index tpcds.indexr1");

        // st.execute("drop index r2");
        // st.execute("create index tpcds.indexr1 on TPCDS.STORE_SALES (SS_SOLD_DATE_SK)");
        // st.execute("create index r2 on TPCH.LINEITEM (L_ORDERKEY)");
        Rt.np("finished index");

        Workload workload = new Workload("", new StringReader(query));
        workload.get(0).setStatementWeight(1000000);
        // indexes = new HashSet<Index>();
        // Set<InumInterestingOrder> orders = AbstractSpaceComputation
        // .extractInterestingOrderFromDB(workload.get(queryId), db2optimizer);
        // for (InumInterestingOrder order : orders) {
        // indexes.add(order);
        // }

        String sql = workload.get(queryId).getSQL();
        Rt.np("SQL:");
        Rt.p(sql);

        // Set<InumInterestingOrder> orders =
        // InumUtils.extractInterestingOrders(
        // workload.get(queryId), db.getCatalog());
        // Set<InumInterestingOrder> orders2 = AbstractSpaceComputation
        // .extractInterestingOrderFromDB(workload.get(queryId),
        // db2optimizer, true);
        // for (InumInterestingOrder o : orders) {
        // if (!orders2.contains(o))
        // Rt.np("MISSING " + o);
        // }
        // for (InumInterestingOrder o : orders2) {
        // if (!orders.contains(o))
        // Rt.np("INCORRECT " + o);
        // }
        // System.exit(0);

        // CandidateGenerator candGen = new OptimizerCandidateGenerator(
        // getBaseOptimizer(db.getOptimizer()));
        // indexes = candGen.generate(workload);

        DB2Advisor db2Advis = new DB2Advisor(db);
        db2Advis.process(workload);
        indexes = db2Advis.getRecommendation(100000);

        Rt.np("Generated Indexes:");
        for (Index index : indexes) {
            Rt.np(index);
        }
        // indexes.add(createIndex(db,
        // "[+TPCDS.CUSTOMER_ADDRESS.CA_STREET_NAME(A)]"));

        // ExplainTables.dumpPlanId=211;
        ExplainTables.dump = true;
        ExplainedSQLStatement db2plan = db2optimizer.explain(sql, indexes);
        Rt.np("DB2 plan:");
        Rt.p(db2plan.getPlan());
        // System.exit(0);
        Rt.np("Index used by DB2 plan:");
        for (Index index2 : db2plan.getUsedConfiguration())
            Rt.np(index2);

        System.exit(0);

        InumPreparedSQLStatement space;
        // space = (InumPreparedSQLStatement) optimizer
        // .prepareExplain(new SQLStatement(sql));
        space = (InumPreparedSQLStatement) optimizer.prepareExplain(workload
                .get(queryId));
        Rt.p("Plans: " + space.getTemplatePlans().size());
        for (InumPlan plan : space.getTemplatePlans()) {
            // Rt.p(plan);
        }
        ExplainedSQLStatement inumPlan = space.explain(indexes);
        SQLStatementPlan plan = inumPlan.getPlan();
        Rt.np("INUM plan:");
        Rt.p(plan);
        InumPlan templatePlan = (InumPlan) plan.templatePlan;
        Rt.np("Template plan:");
        Rt.p(templatePlan);
        Rt.p(templatePlan.orgPlan);
        Rt.np("Index used by INUM:");
        for (Index index2 : inumPlan.getUsedConfiguration())
            Rt.np(index2);

        for (Index index2 : indexes)
            Rt
                    .np("%,.0f\t%s", db2plan.getUpdateCost(index2), index2
                            .toString());

        Rt.p("DB2: %,.0f", db2plan.getTotalCost());
        Rt.p("INUM: %,.0f", inumPlan.getTotalCost());
        // Rt.p(plan);

        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        // formatTpcds();
        // analyzeNLJ();

        // query = Rt.readFile(new File(
        // "resources/workloads/db2/tpcds/workload.sql"));
        new InumTest2();
    }
}