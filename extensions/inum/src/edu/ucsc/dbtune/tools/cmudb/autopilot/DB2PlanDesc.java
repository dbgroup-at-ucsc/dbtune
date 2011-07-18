package edu.ucsc.dbtune.tools.cmudb.autopilot;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Dec 20, 2007
 * Time: 7:11:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class DB2PlanDesc extends PlanDesc {
    public DB2PlanDesc(int rowId, int parent, String operator, String target, double cardinality, float cost) {
        super(rowId, parent, operator, target, cost);
        super.cardinality = cardinality;
    }
}