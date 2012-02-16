package edu.ucsc.dbtune.optimizer.plan;

import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class SQLStatementPlanTest
{

    /**
     */
    @Test
    public void testBasicUsage()
    {
        Operator root = new Operator("Root", 20.67, 2);
        SQLStatementPlan plan =
            new SQLStatementPlan(new SQLStatement("select * from test", SQLCategory.SELECT), root);

        assertThat(plan.size(), is(1));

        Operator left = new Operator("SeqScan", 19.50, 2202);
        Operator right = new Operator("SeqScan", 1.50, 88);

        plan.setChild(root, left);
        plan.setChild(root, right);

        assertThat(plan.getParent(left), is(root));

        assertThat(plan.size(), is(3));

        assertThat(plan.getChildren(plan.getRootOperator()).size(), is(2));
    }
}
