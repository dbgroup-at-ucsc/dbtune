package edu.ucsc.dbtune.inum.old.autopilot;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Dec 20, 2007
 * Time: 7:11:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class OrclPlanDesc extends PlanDesc {
    private String objectAlias;

    public OrclPlanDesc(int rowId, int parent, String operator, String target, String objectAlias, float cost) {
        super(rowId, parent, operator, target, cost);
        this.objectAlias = objectAlias;
    }

    public String getTargetAlias() {
        return objectAlias;
    }
}