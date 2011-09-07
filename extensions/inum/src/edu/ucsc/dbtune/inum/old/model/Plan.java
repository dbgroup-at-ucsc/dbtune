package edu.ucsc.dbtune.inum.old.model;

import com.google.common.base.Joiner;
import edu.ucsc.dbtune.inum.old.autopilot.PlanDesc;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.swing.tree.DefaultMutableTreeNode;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Dec 10, 2007
 * Time: 7:22:51 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Plan implements Serializable {
    protected List<PlanDesc> list = new ArrayList<PlanDesc>();
    protected PlanDesc root;
    protected float internal = Float.NaN;
    protected Map<String, Float> accessCosts = new HashMap<String, Float>();
    protected boolean nljPlan;

    //i think this is a plan node
    public void add(PlanDesc pd) {
        this.list.add(pd);
    }

    //the cost from the first node
    public float getTotalCost() {
        return root.getCost();
    }

    //nu stiu ce e asta
    public float getInternalCost() {
        return internal;
    }

    //this will be implemented in the Classes that extend the Plan class
    public abstract void analyzePlan();

    //
    public void fixAccessCosts(PhysicalConfiguration config, QueryDesc desc) {
        float internal = getInternalCost();
        for (String tname : desc.getUsedTableNames()) {
            if(getAccessCost(tname) <= 0) {
                // check if there is an index on it.
                Index idx = config.getFirstIndexForTable(tname);
                if(idx != null && idx.isImplemented()) {
                    float idxAccessCost = getAccessCost(idx.getImplementedName());
                    if(idxAccessCost >= 0) {
                        setAccessCost(tname.toUpperCase(), idxAccessCost);
                    }
                }
            }

            internal -= getAccessCost(tname);
        }
    }

    public float getAccessCost(String name) {
        if(name == null) {
            new NullPointerException().printStackTrace();
            return 0.0f;
        }
        Float cost = accessCosts.get(name);
        return cost != null ? cost : ((cost = accessCosts.get(name.toUpperCase())) != null? cost : 0.0f);
    }

    public void setAccessCost(String tableName, float cost) {
        accessCosts.put(tableName, cost);
    }

    public String toString() {
        return Joiner.on("\n").join(list);
    }

    public boolean isNLJPlan() {
        return nljPlan;
    }

    public PlanDesc getRoot() {
        return root;
    }

    public float getMateralizedViewAccessCost(String viewname) {
        float cost = 0;
        for (int i = 0; i < list.size(); i++) {
            PlanDesc desc = list.get(i);
            String target = desc.getTarget();
            if(target != null && target.contains(viewname)) {
                return cost += desc.getCost();
            }
        }

        return cost;
    }

    public boolean isNLJTable(String tableName) {
        return false;
    }

    public double getNLJMultiple(String tableName) {
        return 1.0;
    }

    public void fixDoubleCounting() {
        Set<String> keys = new HashSet(accessCosts.keySet());
        for (String key : keys) {
            if(key.contains("_")) {
                // find the key which starts with this and has same
                // cost
                for (Map.Entry<String, Float> entry : accessCosts.entrySet()) {
                    if(entry.getValue().equals(accessCosts.get(key))
                            && !entry.getKey().equals(key)
                            && entry.getKey().startsWith(key.substring(0,1))) {
                        entry.setValue(entry.getValue()/2);
                    }
                }
            }
        }
    }

    public Set getAccessedEntries() {
        return accessCosts.keySet();
    }

    public DefaultMutableTreeNode getPlanTree() {
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                PlanDesc d1 = (PlanDesc) o1;
                PlanDesc d2 = (PlanDesc) o2;
                return d1.getNodeId() - d2.getNodeId();
            }
        });
        if(list.isEmpty()) {
            return null;
        }

        DefaultMutableTreeNode root = null;
        Map<Integer, DefaultMutableTreeNode> map = new HashMap();
        for (int i = 0; i < list.size(); i++) {
            PlanDesc desc = list.get(i);
            DefaultMutableTreeNode node = new DefaultMutableTreeNode(desc);
            if (i == 0) {
                root = node;
            } else {
                DefaultMutableTreeNode parent = map.get(desc.getParentId());
                if(parent == null) {
                    if(desc.getParentId() > desc.getNodeId()) {
                        System.err.println("Cannot handle: " + desc);
                        continue;
                    } else {
                        throw new AssertionError("This is not possible -- parent not built: " + desc);
                    }
                }
                parent.add(node);
            }
            map.put(desc.getNodeId(), node);
        }
        return root;
    }
}