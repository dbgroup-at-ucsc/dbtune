package edu.ucsc.dbtune.optimizer.plan;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class OperatorTest
{
    /**
     */
    @Test
    public void testBasicUsage()
    {
        Operator root = new Operator("Root", 20.67, 2);
        SQLStatementPlan plan = new SQLStatementPlan(null, root);

        Operator tblScan1 = new Operator("SeqScan", 19.50, 2202);
        Operator tblScan2 = new Operator("SeqScan", 1.50, 88);

        assertThat(tblScan1.getName(), is(tblScan2.getName()));
        assertThat(root.getName(), is(not(tblScan2.getName())));

        plan.setChild(root, tblScan1);
        plan.setChild(root, tblScan2);

        assertThat(plan.size(), is(3));

        assertThat(tblScan1, is(tblScan1));
        assertThat(tblScan1, is(not(root)));
        assertThat(tblScan1, is(not(tblScan2)));

        assertThat(root, is(root));
        assertThat(root, is(not(tblScan1)));
        assertThat(root, is(not(tblScan2)));

        assertThat(root.hashCode(), is(not(tblScan1.hashCode())));
        assertThat(tblScan2.hashCode(), is(not(tblScan1.hashCode())));
    }
}
