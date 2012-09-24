package edu.ucsc.dbtune.optimizer;

import static edu.ucsc.dbtune.optimizer.plan.Operator.TEMPORARY_TABLE_SCAN;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsc.dbtune.InumTest2;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InterestingOrder;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.ResultTable;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;

public class ExplainTables {
    public static boolean showWarnings = false;
    public static boolean dump = false;
    public static int dumpPlanId = -1;
    public static int optimizationLevel = -1;

    public static void dumpResult(ResultSet rs) throws SQLException {
        ResultSetMetaData m = rs.getMetaData();
        String[] names = new String[m.getColumnCount()];
        int max = 0;
        for (int i = 0; i < names.length; i++) {
            String s = m.getColumnName(i + 1);
            if (s.length() > max)
                max = s.length();
            names[i] = s;
        }
        if (names.length <= 10) {
            ResultTable table = new ResultTable(names);
            while (rs.next()) {
                String[] ss = new String[names.length];
                for (int i = 0; i < names.length; i++)
                    ss[i] = rs.getString(i + 1);
                table.addRow(ss);
            }
            table.print();
        } else {
            while (rs.next()) {
                for (int i = 0; i < names.length; i++) {
                    Rt.np("%" + max + "s %s", names[i], rs.getString(i + 1));
                }
                Rt.np("---------------");
            }
        }
    }

    public static void dumpTable(Connection connection, String table,
            String columns) throws SQLException {
        Statement st = connection.createStatement();
        st.execute("select " + columns + " from " + table);
        ResultSet rs = st.getResultSet();
        Rt.np(table);
        dumpResult(rs);
        st.close();
    }

    public static void dumpExplainTables(Connection connection)
            throws SQLException {
        // dumpTable(connection, "systools.EXPLAIN_INSTANCE", "*");
        // dumpTable(connection, "systools.EXPLAIN_STATEMENT", "*");
        // dumpTable(connection, "systools.EXPLAIN_ARGUMENT", "*");
        // dumpTable(connection, "systools.EXPLAIN_OBJECT", "*");
        // dumpTable(connection, "systools.EXPLAIN_OPERATOR", "*");
        // dumpTable(connection, "systools.EXPLAIN_STREAM", "*");
        // System.exit(0);
        dumpTable(connection, "systools.EXPLAIN_ARGUMENT",
                "OPERATOR_ID,ARGUMENT_TYPE,ARGUMENT_VALUE");
        dumpTable(connection, "systools.EXPLAIN_OPERATOR",
                "OPERATOR_ID,OPERATOR_TYPE,TOTAL_COST,IO_COST,CPU_COST,BUFFERS");
        dumpTable(
                connection,
                "systools.EXPLAIN_PREDICATE",
                "OPERATOR_ID,PREDICATE_ID,HOW_APPLIED,WHEN_EVALUATED,RELOP_TYPE,SUBQUERY,FILTER_FACTOR,PREDICATE_TEXT");
        dumpTable(
                connection,
                "systools.EXPLAIN_STREAM",
                "SOURCE_ID,TARGET_ID,STREAM_COUNT,COLUMN_COUNT,COLUMN_NAMES,object_schema,object_name");
        dumpTable(connection, "systools.advise_index",
                "TBNAME,COLNAMES,COLCOUNT");

        // dumpTable(connection, "systools.EXPLAIN_DIAGNOSTIC");
        // dumpTable(connection, "systools.EXPLAIN_DIAGNOSTIC_DATA");
        // dumpTable(connection, "systools.OBJECT_METRICS");
        // dumpTable(connection, "systools.ADVISE_INSTANCE");
        // dumpTable(connection, "systools.ADVISE_INDEX");
        // dumpTable(connection, "systools.ADVISE_WORKLOAD");
        // dumpTable(connection, "systools.ADVISE_MQT");
        // dumpTable(connection, "systools.ADVISE_PARTITION");
        // dumpTable(connection, "systools.ADVISE_TABLE");
    }

    public ExplainTables() {

    }

    private static int nextPlanId = 0;

    public SQLStatementPlan getPlan(Connection connection, SQLStatement sql,
            Catalog catalog, Set<Index> indexes) throws SQLException {
        Statement stmt = connection.createStatement();
        HashSet<Integer> usedIds = new HashSet<Integer>();

        if (optimizationLevel >= 0)
            stmt.execute("SET CURRENT QUERY OPTIMIZATION " + optimizationLevel);
        stmt.execute("SET CURRENT EXPLAIN MODE = EVALUATE INDEXES");
        try {
            stmt.execute(sql.getSQL());
        } catch (SQLException e) {
            Rt.p(sql.getSQL());
            throw e;
        }
        stmt.execute("SET CURRENT EXPLAIN MODE = NO");
        stmt.close();

        if (dump || nextPlanId == dumpPlanId)
            dumpExplainTables(connection);

        Vector<Operator> operators = new Vector<Operator>();
        Hashtable<Integer, Operator> hash = new Hashtable<Integer, Operator>();
        Statement st = connection.createStatement();
        st.execute("select OPERATOR_ID,OPERATOR_TYPE,"
                + "TOTAL_COST,IO_COST,CPU_COST,BUFFERS"
                + " from systools.EXPLAIN_OPERATOR");
        ResultSet rs = st.getResultSet();
        while (rs.next()) {
            int id = rs.getInt("OPERATOR_ID");
            String name = rs.getString("OPERATOR_TYPE");
            double accomulatedCost = rs.getDouble("TOTAL_COST");
            double cpuCost = rs.getDouble("CPU_COST");
            double ioCost = rs.getDouble("IO_COST");
            double buffers = rs.getDouble("BUFFERS");

            Operator op = new Operator(DB2Optimizer
                    .getOperatorName(name.trim()), accomulatedCost, 0);
            usedIds.add(id);
            op.id = id;
            op.ioCost = ioCost;
            op.cpuCost = cpuCost;
            op.buffers = buffers;
            if (hash.containsKey(id))
                throw new Error("duplicate id");
            hash.put(id, op);
            operators.add(op);
        }
        rs.close();
        st.close();

        st = connection.createStatement();
        st.execute("select OPERATOR_ID,ARGUMENT_TYPE,ARGUMENT_VALUE"
                + " from systools.EXPLAIN_ARGUMENT"
                + " where ARGUMENT_TYPE in ('JN INPUT','NUMROWS','ROWWIDTH')");
        rs = st.getResultSet();
        while (rs.next()) {
            int id = rs.getInt("OPERATOR_ID");
            String type = rs.getString("ARGUMENT_TYPE");
            String value = rs.getString("ARGUMENT_VALUE");
            type = type.trim();
            Operator op = hash.get(id);
            if ("JN INPUT".equals(type)) {
                op.joinInput = value;
            } else if ("NUMROWS".equals(type)) {
                op.rows = Integer.parseInt(value);
            } else if ("ROWWIDTH".equals(type)) {
                op.rowWidth = Integer.parseInt(value);
            } else
                throw new Error(type);
        }
        rs.close();
        st.close();

        st = connection.createStatement();
        st.execute("select OPERATOR_ID,PREDICATE_ID,HOW_APPLIED,"
                + "WHEN_EVALUATED,RELOP_TYPE,SUBQUERY,"
                + "FILTER_FACTOR,PREDICATE_TEXT"
                + " from systools.EXPLAIN_PREDICATE");
        rs = st.getResultSet();
        while (rs.next()) {
            int id = rs.getInt("OPERATOR_ID");
            int predicate_id = rs.getInt("PREDICATE_ID");
            String how_applied = rs.getString("HOW_APPLIED");
            String when_evaluated = rs.getString("WHEN_EVALUATED");
            String relop_type = rs.getString("RELOP_TYPE");
            String subquery = rs.getString("SUBQUERY");
            double filterFactor = rs.getDouble("FILTER_FACTOR");
            String predicate_text = rs.getString("PREDICATE_TEXT");
            Operator op = hash.get(id);
            if (op.rawPredicateList == null)
                op.rawPredicateList = new LinkedList<String>();
            if (op.filterFactorList == null)
                op.filterFactorList = new LinkedList<Double>();
            op.rawPredicateList.add(predicate_text);
            op.filterFactorList.add(filterFactor);
        }
        rs.close();
        st.close();

        st = connection.createStatement();
        st.execute("select SOURCE_ID,TARGET_ID,STREAM_COUNT,"
                + "COLUMN_COUNT,COLUMN_NAMES," + "object_schema,object_name"
                + " from systools.EXPLAIN_STREAM");
        rs = st.getResultSet();
        Hashtable<Operator, Operator> srcToDest = new Hashtable<Operator, Operator>();
        Vector<Operator[]> extraStream = new Vector<Operator[]>();
        int leafId = -1;
        while (rs.next()) {
            int source_id = rs.getInt("SOURCE_ID");
            int target_id = rs.getInt("TARGET_ID");
            double cardinality = rs.getDouble("STREAM_COUNT");
            String columnNames = rs.getString("COLUMN_NAMES");
            String dboSchema = rs.getString("object_schema");
            String dboName = rs.getString("object_name");
            if (target_id < 0)
                continue;
            Operator src = source_id < 0 ? null : hash.get(source_id);
            Operator dest = hash.get(target_id);
            if (src == null) {
                src = new Operator("", 0, cardinality);
                src.id = leafId;
                operators.add(src);
                leafId--;
            }
            src.cardinality = cardinality;
            src.cardinalityNLJ = cardinality;
            if (src.cardinalityNLJ < 1)
                src.cardinalityNLJ = 1;
            src.rawColumnNames = columnNames;
            if (srcToDest.get(src) != null) {
                extraStream.add(new Operator[] { src, dest });
                // Rt.p(src);
                // Rt.p(srcToDest.get(src));
                // Rt.p(dest);
                // if (ExplainTables.showWarnings)
                // Rt.error("plan is not a tree");
            } else {
                srcToDest.put(src, dest);
            }

            if (dboSchema != null && dboName != null) {
                dboSchema = dboSchema.trim();
                dboName = dboName.trim();

                if (dboSchema.equalsIgnoreCase("SYSIBM")) {
                    if (dboName.equalsIgnoreCase("GENROW"))
                        src.name = TEMPORARY_TABLE_SCAN;
                    else {
                        // An index which used to enforce the primary key constraint on a table
                        src.name = TEMPORARY_TABLE_SCAN;
                    }
                    if (columnNames != null && columnNames.length() > 0) {
                        Pattern tableReferencePattern = Pattern
                                .compile("Q\\d+\\.");
                        Matcher matcher = tableReferencePattern
                                .matcher(columnNames);
                        if (matcher.find()) {
                            src.aliasInExplainTables = columnNames.substring(
                                    matcher.start(), matcher.end() - 1);
                        }
                    }
                } else {
                    DatabaseObject dbo = DB2Optimizer
                            .extractDatabaseObjectReferenced(catalog, indexes,
                                    dboSchema, dboName);
                    src.add(dbo);
                    InterestingOrder columnsFetched = DB2Optimizer
                            .extractColumnsUsedByOperator(src, columnNames,
                                    dbo, catalog);
                    if (columnsFetched != null)
                        src.addColumnsFetched(columnsFetched);
                }
            }

            if (src.rawPredicateList != null) {
                src.add(DB2Optimizer.extractPredicatesUsedByOperator(
                        src.rawPredicateList, catalog));
            }

            // double first_row_cost = rs.getDouble("FIRST_ROW_COST");
            // double re_total_cost = rs.getDouble("RE_TOTAL_COST");
            // double re_io_cost = rs.getDouble("RE_IO_COST");
            // double re_cpu_cost = rs.getDouble("RE_CPU_COST");
        }
        rs.close();
        st.close();

        Operator root = null;
        for (Operator o : operators) {
            if (srcToDest.get(o) == null) {
                if (root != null)
                    throw new Error("tree error");
                root = o;
            }
        }
        SQLStatementPlan plan = new SQLStatementPlan(root);
        plan.id = "" + (nextPlanId++);
        for (Operator operator : operators) {
            if (operator == root)
                continue;
            setChild(operator, srcToDest, plan);
        }
        checkNodes(plan, plan.getRootOperator());
        for (Operator operator : operators) {
            if (operator.id < 0) {
                Operator parent = srcToDest.get(operator);
                if (parent.objects != null && parent.objects.size() > 0) {
                    Rt.p(plan);
                    throw new Error("" + operator.id);
                }
                if (parent.columnsFetched != null)
                    throw new Error("" + operator.id);
                parent.objects.addAll(operator.objects);
                parent.columnsFetched = operator.columnsFetched;
                if (parent.rawColumnNames == null)
                    parent.rawColumnNames = operator.rawColumnNames;
                else {
                    parent.rawColumnNames += operator.rawColumnNames;
                }
                if (operator.rawPredicateList != null) {
                    parent.rawPredicateList.addAll(operator.rawPredicateList);
                }
            }
        }
        if (extraStream.size() > 0) {
            for (Operator[] srcDest : extraStream) {
                Operator o2 = srcDest[0].duplicate();
                while (usedIds.contains(o2.id))
                    o2.id += 10000;
                usedIds.add(o2.id);
                plan.setChild(srcDest[1], o2);
                copySubTree(plan, srcDest[0], o2, usedIds);
            }
        }
        DB2Optimizer.calculateOperatorInternalCost(plan,
                plan.getRootOperator(), 1);
        return plan;
    }

    static void checkNodes(SQLStatementPlan plan, Operator node)
            throws SQLException {
        List<Operator> children = plan.getChildren(node);
        if (children.size() == 0 && node.getDatabaseObjects().size() > 0)
            return;
        int problemNodes = 0;
        double sum = 0;
        for (Operator operator : children) {
            if (operator.accumulatedCost > node.accumulatedCost)
                problemNodes++;
            else
                sum += operator.accumulatedCost;
        }
        if (problemNodes > 1)
            throw new SQLException("Invalid plan, too many invalid nodes");
        if (problemNodes == 1) {
            for (Operator operator : children) {
                if (operator.accumulatedCost > node.accumulatedCost) {
                    operator.accumulatedCost = node.accumulatedCost - sum;
                    break;
                }
            }
        }
        for (Operator operator : children)
            checkNodes(plan, operator);
    }

    private static void setChild(Operator operator,
            Hashtable<Operator, Operator> srcToDest, SQLStatementPlan plan)
            throws SQLException {
        Operator parent = srcToDest.get(operator);
        if (operator.id < 0) {
            parent.aliasInExplainTables2 = operator.aliasInExplainTables;
            return;
        }
        if (plan.elements.get(operator) != null)
            return;
        if (plan.elements.get(parent) == null)
            setChild(parent, srcToDest, plan);
        plan.setChild(parent, operator);
    }

    static void copySubTree(SQLStatementPlan p1, Operator src, Operator dest,
            HashSet<Integer> usedIds) {
        List<Operator> childern = p1.getChildren(src);
        for (Operator operator : childern) {
            // Rt.p(o1.id+" "+ operator.id);
            Operator o2 = operator.duplicate();
            while (usedIds.contains(o2.id))
                o2.id += 10000;
            usedIds.add(o2.id);
            p1.setChild(dest, o2);
            copySubTree(p1, operator, o2, usedIds);
        }
    }
}
