package edu.ucsc.dbtune.inum.old.autopilot;

import com.google.common.base.Joiner;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Dec 22, 2007
 * Time: 10:11:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class PlanDesc {
    private int rowId;
    private int parent;
    private String operator;
    private String target;
    protected double cardinality;
    private float cost;

    public PlanDesc(int rowId, int parent, String operator, String target, float cost) {
        this.rowId = rowId;
        this.parent = parent;
        this.operator = operator != null ? operator.trim() : operator;
        this.target = target;
        this.cost = cost;
    }

    public float getCost() {
        return cost;
    }

    public int getNodeId() {
        return rowId;
    }

    public int getParentId() {
        return parent;
    }

    public String getOperator() {
        return operator;
    }

    public String getTarget() {
        return target;
    }

    //i think, this means the number of rows
    public double getCardinality() {
        return cardinality;
    }

    public String toString() {
        return Joiner.on(", ").join(rowId, parent, operator, target, cardinality, cost);
    }
}

