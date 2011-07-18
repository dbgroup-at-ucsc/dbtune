package edu.ucsc.dbtune.tools.cmudb.model;

import Zql.ZQuery;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class QueryDesc implements Serializable {
    private static final long serialVersionUID = 1715780173594976416L;
    transient public HashMap<String, Float> access_costs;
    transient public ZQuery parsed_query;
    public String queryString;
    public PhysicalConfiguration interesting_orders;
    public PhysicalConfiguration group_by; //includes groupby
    public HashMap plans; // a indexes from interesting_orders to Plans (resultsets)
    public PhysicalConfiguration used; //all the used columns (including interesting, groupby)
    public float emptyCost; //cost of the empty configuration
    public HashMap nljMap;
    public float[] candidateCosts;
    public float bestInternalPlanCost;
    public String bestInternalCombination;
    public HashMap optimalPlanCosts;
    public HashMap mava_costs;

    public synchronized void put_access_cost(String key, Float c) {
        access_costs.put(key, c);
    }

    public Set<String> getUsedTableNames() {
        return interesting_orders.getIndexedTableNames();
    }


    public QueryDesc() {
        plans = new HashMap();
        access_costs = new HashMap();
        nljMap = new HashMap();
	    optimalPlanCosts = new HashMap();
    }


    public void setup(List candidates) { 
	candidateCosts = new float[candidates.size()];
	for (int i=0; i<candidateCosts.length; i++)
	    candidateCosts[i] = Float.MAX_VALUE;
	bestInternalPlanCost = Float.MAX_VALUE;
	//bestInternalCombination = new String();
    }

    public float getEmptyCost() {
        if(emptyCost <= 0) {
            int width = this.used.getIndexedTableNames().size();
            List sizeList = new ArrayList(width);
            for(int i=0;i<width;i++) {
                sizeList.add(0);
            }
            String key = sizeList.toString();
            Plan plan = (Plan) plans.get(key);
            Plan nljplan = (Plan) nljMap.get(key);

            if(plan != null) {
                emptyCost = plan.getTotalCost();
            }

            if(nljplan != null && emptyCost < nljplan.getTotalCost()) {
                emptyCost = nljplan.getTotalCost();
            }
        }

        return emptyCost;
    }
}
