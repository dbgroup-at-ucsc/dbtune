package edu.ucsc.dbtune.inum.autopilot;

import javax.swing.tree.DefaultMutableTreeNode;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Jan 2, 2008
 * Time: 12:52:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class OrclPlan extends DB2Plan {
    protected String getTable(PlanDesc currentPlan) {
        OrclPlanDesc desc = (OrclPlanDesc) currentPlan;
        if(desc.getOperator().contains("TABLE")) {
            return desc.getTarget();
        } else if(desc.getOperator().equals("INDEX")) {
            return desc.getTargetAlias().split("\\W")[0];
        } else {
            return desc.getTarget();
        }
    }

    protected boolean parentInternal(DefaultMutableTreeNode node1) {
        return super.parentInternal(node1) || ((OrclPlanDesc) node1.getUserObject()).getOperator().equals("SORT");
    }

    protected boolean isIndexAccess(PlanDesc currentPlan) {
        return currentPlan.getOperator().equals("INDEX");
    }

    protected boolean isCummulative() {
        return false;
    }

    protected boolean isTableAccess(PlanDesc desc) {
        return desc.getOperator().equals("TABLE");
    }
}
