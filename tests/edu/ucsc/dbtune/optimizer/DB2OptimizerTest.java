package edu.ucsc.dbtune.optimizer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;

import org.junit.BeforeClass;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.DBTuneInstances.configureCatalogWithoutIndexes;
import static edu.ucsc.dbtune.DBTuneInstances.makeResultSet;
import static edu.ucsc.dbtune.metadata.Index.ASC;
import static edu.ucsc.dbtune.metadata.Index.DESC;
import static edu.ucsc.dbtune.optimizer.DB2Optimizer.DELETE_FROM_ADVISE_INDEX;
import static edu.ucsc.dbtune.optimizer.DB2Optimizer.DELETE_FROM_EXPLAIN_INSTANCE;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.text.IsEqualIgnoringCase.equalToIgnoringCase;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ivo Jimenez
 */
public class DB2OptimizerTest
{
    private static Catalog cat;
    private static Column a;
    private static Column b;
    private static Column c;
    private static String[] h;
    private static String[] h2;

    /**
     * @throws Exception
     *      if the creation of the mock fails
     *
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        cat = configureCatalog();

        a = cat.<Column>findByName("schema_0.table_0.column_0");
        b = cat.<Column>findByName("schema_0.table_0.column_1");
        c = cat.<Column>findByName("schema_0.table_0.column_2");

        h = new String[8];

        h[0] = "node_id";
        h[1] = "parent_id";
        h[2] = "operator_name";
        h[3] = "object_schema";
        h[4] = "object_name";
        h[5] = "cardinality";
        h[6] = "cost";
        h[7] = "column_names";

        h2 = new String[2];

        h2[0] = "node_id";
        h2[1] = "predicate_text";
    }

    /**
     * Checks that the extraction of {@link Operator} objects is done correctly.
     *
     * @throws Exception
     *      if the creation of the mock fails
     */
    @Test
    public void testNodeParsing() throws Exception
    {
        // CHECKSTYLE:OFF
        ResultSet rs = makeResultSet(
        Arrays.asList(h[0], h[1],       h[2],       h[3],                       h[4], h[5],   h[6], h[7]),
        Arrays.asList(   1,    0,   "RETURN", "schema_0", null                      , 10l , 2000.0,   ""),
        Arrays.asList(   2,    1,   "TBSCAN", "schema_0", "table_0"                 , 10l , 2000.0,   ""),
        Arrays.asList(   3,    2,   "SORT"  , "schema_0", null                      , 10l , 1500.0,   ""),
        Arrays.asList(   4,    3,   "RIDSCN", "schema_0", null                      , 10l , 1400.0,   ""),
        Arrays.asList(   5,    4,   "FETCH" , "schema_0", "table_0"                 , 100l,  700.0,   ""),
        Arrays.asList(   6,    5,   "IXSCAN", "SYSTEM",   "schema_0.table_0_index_0", 100l,  700.0,   ""));
        // CHECKSTYLE:ON
        
        Operator op;

        rs.next();

        op = DB2Optimizer.parseNode(cat, rs, new ArrayList<String>(), new HashSet<Index>());

        assertThat(op.getName(), is("RETURN"));
        assertThat(op.getCardinality(), is(10L));
        assertThat(op.getAccumulatedCost(), is(2000.0));
        assertThat(op.getDatabaseObjects().isEmpty(), is(true));

        rs.next();

        op = DB2Optimizer.parseNode(cat, rs, new ArrayList<String>(), new HashSet<Index>());

        assertThat(op.getName(), is(Operator.TABLE_SCAN));
        assertThat(op.getCardinality(), is(10L));
        assertThat(op.getAccumulatedCost(), is(2000.0));
        assertThat(op.getDatabaseObjects().isEmpty(), is(false));
        assertThat(op.getDatabaseObjects().size(), is(1));
        assertThat(op.getDatabaseObjects().get(0).getName(), is("table_0"));

        rs.next();

        op = DB2Optimizer.parseNode(cat, rs, new ArrayList<String>(), new HashSet<Index>());

        assertThat(op.getDatabaseObjects().isEmpty(), is(true));
        assertThat(op.getName(), is("SORT"));

        rs.next();

        op = DB2Optimizer.parseNode(cat, rs, new ArrayList<String>(), new HashSet<Index>());

        assertThat(op.getName(), is(Operator.RID_SCAN));
        assertThat(op.getDatabaseObjects().isEmpty(), is(true));

        rs.next();

        op = DB2Optimizer.parseNode(cat, rs, new ArrayList<String>(), new HashSet<Index>());

        assertThat(op.getName(), is(Operator.FETCH));
        assertThat(op.getDatabaseObjects().isEmpty(), is(false));
        assertThat(op.getDatabaseObjects().size(), is(1));
        assertThat(op.getDatabaseObjects().get(0).getName(), is("table_0"));

        rs.next();

        op = DB2Optimizer.parseNode(cat, rs, new ArrayList<String>(), new HashSet<Index>());

        assertThat(op.getName(), is(Operator.INDEX_SCAN));
        assertThat(op.getCardinality(), is(100L));
        assertThat(op.getAccumulatedCost(), is(700.0));
        assertThat(op.getDatabaseObjects().isEmpty(), is(false));
        assertThat(op.getDatabaseObjects().size(), is(1));
        assertThat(op.getDatabaseObjects().get(0).getName(), is("table_0_index_0"));

        assertThat(rs.next(), is(false));
    }

    /**
     * Checks that the extraction of a plan is done correctly.
     * @throws Exception
     *      if the creation of the mock fails
     */
    @Test
    public void testPlanParsing() throws Exception
    {
        // CHECKSTYLE:OFF
        ResultSet rs = makeResultSet(
        Arrays.asList(h[0], h[1],      h[2],        h[3],                        h[4],  h[5],    h[6], h[7]),
        Arrays.asList(   1, null,  "RETURN",  "schema_0",  null                      ,  10l ,  2000.0,   ""),
        Arrays.asList(   2,    1,  "GRPBY" ,  "schema_0",  null                      ,  10l ,  2000.0,   ""),
        Arrays.asList(   3,    2,  "SORT"  ,  "schema_0",  null                      ,  10l ,  1500.0,   ""),
        Arrays.asList(   4,    3,  "TBSCAN",  "schema_0",  "table_2"                 ,  100l,   700.0,   "+Q1.column_0(A)+Q2.column_3(D)+Q3.column_2"),
        Arrays.asList(   5,    3,  "IXSCAN",    "SYSTEM",  "schema_0.table_0_index_0",  100l,   700.0,   "+Q2.column_1"));

        ResultSet rs2 = makeResultSet(
        Arrays.asList(h2[0], h2[1]),
        Arrays.asList(    1, null),
        Arrays.asList(    2, null),
        Arrays.asList(    3, null),
        Arrays.asList(    4, "Q1.SOME_COL = Q1.SOME_OTHER_COL"),
        Arrays.asList(    4, "Q1.BAR = 10"),
        Arrays.asList(    4, "Q1.FOO > 20"),
        Arrays.asList(    5, "Q2.TAA BETWEEN 10 AND 100000"),
        Arrays.asList(    5, "Q2.SOME_COL > Q2.OTHER_COL"),
        Arrays.asList(    5, "EXISTS (SELECT $RID$ FROM Q2.SOME_TABLE)"));
        // CHECKSTYLE:ON

        SQLStatementPlan plan = DB2Optimizer.parsePlan(cat, rs, rs2, new HashSet<Index>());

        assertThat(plan.size(), is(5));
        assertThat(plan.getIndexes().size(), is(1));
        assertThat(plan.getRootOperator().getName(), is("RETURN"));

        assertThat(plan.leafs().size(), is(2));

        for (Operator o : plan.leafs()) {

            if (o.getName().equals(Operator.INDEX_SCAN)) {
                assertThat(o.getDatabaseObjects().size(), is(1));
                assertThat(o.getPredicates().size(), is(3));
                assertThat(o.getPredicates().get(2).getText().contains("RID()"), is(true));
            }
            else if (o.getName().equals(Operator.TABLE_SCAN)) {
                assertThat(o.getDatabaseObjects().size(), is(1));
                assertThat(o.getPredicates().size(), is(3));
            }
            else {
                fail("Unexpected operator at leaf: " + o);
            }
        }
    }

    /**
     * Checks that the extraction of a plan is done correctly.
     * @throws Exception
     *      if the creation of the mock fails
     */
    @Test
    public void testMoreThanOneParent() throws Exception
    {
        // CHECKSTYLE:OFF
        ResultSet rs = makeResultSet(
        Arrays.asList(h[0], h[1],      h[2],        h[3],                        h[4],  h[5],    h[6], h[7]),
        Arrays.asList(   1, null,  "RETURN",  "schema_0",  null                      ,  10l ,  2000.0,   ""),
        Arrays.asList(   2,    1,  "GRPBY" ,  "schema_0",  null                      ,  10l ,  2000.0,   ""),
        Arrays.asList(   3,    1,  "SORT"  ,  "schema_0",  null                      ,  10l ,  1500.0,   ""),
        Arrays.asList(   4,    2,  "TBSCAN",  "schema_0",  "table_2"                 ,  100l,   700.0,   "+Q1.column_0(A)+Q2.column_3(D)+Q3.column_2"),
        Arrays.asList(   4,    3,  "IXSCAN",    "SYSTEM",  "schema_0.table_0_index_0",  100l,   700.0,   "+Q2.column_1"));

        ResultSet rs2 = makeResultSet(
        Arrays.asList(h2[0], h2[1]),
        Arrays.asList(    1, null),
        Arrays.asList(    2, null),
        Arrays.asList(    3, null),
        Arrays.asList(    4, "Q1.SOME_COL = Q1.SOME_OTHER_COL"),
        Arrays.asList(    4, "Q1.BAR = 10"),
        Arrays.asList(    4, "Q1.FOO > 20"),
        Arrays.asList(    5, "Q2.TAA BETWEEN 10 AND 100000"),
        Arrays.asList(    5, "Q2.SOME_COL > Q2.OTHER_COL"),
        Arrays.asList(    5, "EXISTS (SELECT $RID$ FROM Q2.SOME_TABLE)"));
        // CHECKSTYLE:ON

        SQLStatementPlan plan = DB2Optimizer.parsePlan(cat, rs, rs2, new HashSet<Index>());

        assertThat(plan.size(), is(5));
        assertThat(plan.getIndexes().size(), is(1));
        assertThat(plan.getRootOperator().getName(), is("RETURN"));

        assertThat(plan.leafs().size(), is(2));

        for (Operator o : plan.leafs()) {

            if (o.getName().equals(Operator.INDEX_SCAN)) {
                assertThat(o.getDatabaseObjects().size(), is(1));
                assertThat(o.getPredicates().size(), is(3));
            }
            else if (o.getName().equals(Operator.TABLE_SCAN)) {
                assertThat(o.getDatabaseObjects().size(), is(1));
                assertThat(o.getPredicates().size(), is(3));
            }
            else {
                fail("Unexpected operator at leaf: " + o);
            }
        }
    }

    /**
     * Checks that clearing the ADVISE_INDEX and EXPLAIN tables is done correctly.
     *
     * @throws Exception
     *      if error
     */
    @Test
    public void testClearAdviseAndExplainTables() throws Exception
    {
        Connection con = mock(Connection.class);
        Statement stmt = mock(Statement.class);

        when(stmt.executeUpdate(anyString())).thenReturn(0);
        when(con.createStatement()).thenReturn(stmt);

        DB2Optimizer.clearAdviseAndExplainTables(con);

        verify(stmt, times(1)).executeUpdate(DELETE_FROM_ADVISE_INDEX);
        verify(stmt, times(1)).executeUpdate(DELETE_FROM_EXPLAIN_INSTANCE);
        verify(stmt, times(1)).close();
    }

    /**
     * @throws Exception
     *      if error
     */
    @Test
    public void testBuildColumnNamesValue() throws Exception
    {
        Catalog catWithout = configureCatalogWithoutIndexes();
        Index idx;

        idx = new Index(catWithout.<Column>findByName("schema_0.table_0.column_0"), ASC);

        assertThat(DB2Optimizer.buildColumnNamesValue(idx), is(equalToIgnoringCase("+column_0")));
        cat.schemas().get(0).remove(idx);

        idx = new Index(catWithout.<Column>findByName("schema_0.table_0.column_0"), DESC);

        assertThat(DB2Optimizer.buildColumnNamesValue(idx), is(equalToIgnoringCase("-column_0")));
        cat.schemas().get(0).remove(idx);

        idx = new Index("e", newArrayList(a, b, c), newArrayList(ASC, DESC, DESC));

        assertThat(
                DB2Optimizer.buildColumnNamesValue(idx),
                is(equalToIgnoringCase("+column_0-column_1-column_2")));
        cat.schemas().get(0).remove(idx);

        idx = new Index("e", newArrayList(a, b, c), newArrayList(DESC, ASC, DESC));

        assertThat(
                DB2Optimizer.buildColumnNamesValue(idx),
                is(equalToIgnoringCase("-column_0+column_1-column_2")));
        cat.schemas().get(0).remove(idx);
    }

    // TODO:
    //    * BuildAdviseIndexInsertStatement
    //    * InsertIntoAdviseTable
    //    * ExtractColumnsUsedByOperator
    //    * ExtractOperatorToPredicateListMap
    //    * ExtractPredicatesUsedByOperator
    //    * ParseColumnNames
    //    * ReadAdviseIndexTable
    //    * renameClosestJoinAndRemoveBranchComingFrom
    //    * removeGENROW
    //    * rewriteNonLeafTableScans
    //    * optimization profiles
}
