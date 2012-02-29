package edu.ucsc.dbtune.optimizer.plan;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.Before;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.metadata.Index.ASC;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class SQLStatementPlanTest
{
    private Catalog catalog;
    private SQLStatement sql;
    private SQLStatementPlan plan;
    private Operator root;
    private Operator join;
    private Operator one;
    private Operator two;
    private Operator three;
    private Table tbl1;
    private Table tbl3;
    private Index index2;
    private List<Predicate> predicates;
    private InterestingOrder interestingOrder;

    /**
     * @throws Exception
     *      if fails
     */
    @Before
    public void setUp() throws Exception
    {
        root = new Operator("Root", 20.67, 2);
        join = new Operator("MultiWayJoin", 2345, 2);
        one = new Operator("SeqScan", 190.50, 2202);
        two = new Operator("IndexScan", 1.50, 924);
        three = new Operator("SeqScan", 234.50, 88);
        sql = new SQLStatement("select * from test", SQLCategory.SELECT);

        catalog = configureCatalog();
        tbl1 = catalog.<Table>findByName("schema_0.table_1");
        tbl3 = catalog.<Table>findByName("schema_0.table_2");
        index2 = catalog.schemas().get(0).indexes().get(0);
        predicates = new ArrayList<Predicate>();
        interestingOrder =
            new InterestingOrder(catalog.<Column>findByName("schema_0.table_2.column_0"), ASC);

        predicates.add(new Predicate(null, "A > B"));
        predicates.add(new Predicate(null, "B > C"));

        one.add(tbl1);
        one.add(predicates);
        two.add(index2);
        two.addColumnsFetched(interestingOrder);
        three.add(tbl3);

        plan = new SQLStatementPlan(sql, root);

        plan.setChild(root, join);
        plan.setChild(join, one);
        plan.setChild(join, two);
        plan.setChild(join, three);
    }

    /**
     */
    @Test
    public void testConstruction()
    {
        assertThat(plan.size(), is(5));

        assertThat(plan.getStatement(), is(sql));

        assertThat(plan.getRootOperator(), is(root));

        assertThat(plan.getParent(one), is(join));
        assertThat(plan.getParent(two), is(join));
        assertThat(plan.getParent(three), is(join));
        assertThat(plan.getParent(join), is(root));
        assertThat(plan.getParent(root), is(nullValue()));

        assertThat(plan.getChildren(root).size(), is(1));
        assertThat(plan.getChildren(join).size(), is(3));
        assertThat(plan.getChildren(one).size(), is(0));
        assertThat(plan.getChildren(two).size(), is(0));
        assertThat(plan.getChildren(three).size(), is(0));

        assertThat(plan.contains("Root"), is(true));
        assertThat(plan.contains("MultiWayJoin"), is(true));
        assertThat(plan.contains("SeqScan"), is(true));
        assertThat(plan.contains("IndexScan"), is(true));
        assertThat(plan.contains("OtherNode"), is(false));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testFindingAncestor() throws Exception
    {
        assertThat(plan.findAncestorWithName(one, "Root"), is(root));
        assertThat(plan.findAncestorWithName(one, "MultiWayJoin"), is(join));
        assertThat(plan.findAncestorWithName(two, "Root"), is(root));
        assertThat(plan.findAncestorWithName(two, "MultiWayJoin"), is(join));
        assertThat(plan.findAncestorWithName(three, "Root"), is(root));
        assertThat(plan.findAncestorWithName(three, "MultiWayJoin"), is(join));
        assertThat(plan.findAncestorWithName(join, "Root"), is(root));
        assertThat(plan.findAncestorWithName(root, "Root"), is(nullValue()));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testCopyConstructor() throws Exception
    {
        assertThat(new SQLStatementPlan(plan), is(plan));
        assertThat(new SQLStatementPlan(plan).hashCode(), is(plan.hashCode()));

        // check that elements are duplicated, i.e. new Operator instances are created instead of 
        // just copying references
        SQLStatementPlan copy = new SQLStatementPlan(plan);

        List<Operator> planList = plan.toList();
        List<Operator> copyList = copy.toList();

        assertThat(planList, is(copyList));

        for (int i = 0; i < planList.size(); i++) {
            assertThat(
                System.identityHashCode(planList.get(i)) != 
                System.identityHashCode(copyList.get(i)), is(true));
        }
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testDatabaseObjects() throws Exception
    {
        assertThat(plan.getDatabaseObjects().size(), is(3));
        assertThat(plan.getDatabaseObjects().contains(tbl1), is(true));
        assertThat(plan.getDatabaseObjects().contains(tbl3), is(true));
        assertThat(plan.getDatabaseObjects().contains(index2), is(true));

        assertThat(plan.getIndexes().size(), is(1));
        assertThat(plan.getIndexes().contains(index2), is(true));

        assertThat(plan.getTables().size(), is(3));
        assertThat(plan.getTables().contains(tbl1), is(true));
        assertThat(plan.getTables().contains(tbl3), is(true));
        assertThat(plan.getTables().contains(index2.getTable()), is(true));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testPlanModification() throws Exception
    {
        SQLStatementPlan copy = new SQLStatementPlan(plan);

        copy.rename(copy.getRootOperator(), "Ruth");

        assertThat(copy, is(not(plan)));
        assertThat(copy.hashCode(), is(not(plan.hashCode())));

        copy = new SQLStatementPlan(plan);

        copy.assignCost(copy.getRootOperator(), 134.523);

        assertThat(copy, is(not(plan)));
        assertThat(copy.hashCode(), is(not(plan.hashCode())));

        copy = new SQLStatementPlan(plan);

        copy.removeDatabaseObject(copy.find(one));

        assertThat(copy, is(not(plan)));
        assertThat(copy.hashCode(), is(not(plan.hashCode())));

        copy = new SQLStatementPlan(plan);

        copy.removePredicates(copy.find(one));

        assertThat(copy, is(not(plan)));
        assertThat(copy.hashCode(), is(not(plan.hashCode())));

        copy = new SQLStatementPlan(plan);

        copy.removeColumnsFetched(copy.find(two));

        assertThat(copy, is(not(plan)));
        assertThat(copy.hashCode(), is(not(plan.hashCode())));
    }
}
