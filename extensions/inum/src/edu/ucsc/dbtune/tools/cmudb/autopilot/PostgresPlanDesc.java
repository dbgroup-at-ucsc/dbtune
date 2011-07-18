/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.ucsc.dbtune.tools.cmudb.autopilot;

/**
 *
 * @author cristina
 */
public class PostgresPlanDesc extends PlanDesc {
float initCost;
    public PostgresPlanDesc(int rowId, int parent, String operator, String target, double cardinality, float cost, float costi)
    {
     super(rowId, parent, operator, target, cost);
     super.cardinality = cardinality;
     initCost = costi;
    }


}
