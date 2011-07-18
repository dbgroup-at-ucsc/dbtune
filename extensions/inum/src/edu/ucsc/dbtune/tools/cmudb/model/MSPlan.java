package edu.ucsc.dbtune.tools.cmudb.model;

import edu.ucsc.dbtune.tools.cmudb.autopilot.MSPlanDesc;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.ListIterator;
import java.util.Set;
import java.util.Stack;
import javax.swing.tree.DefaultMutableTreeNode;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Feb 22, 2008
 * Time: 12:00:26 AM
 * To change this template use File | Settings | File Templates.
 */
public class MSPlan extends Plan {

        private Set joinSet = new HashSet();
    private static Set msJoinSet = new HashSet(Arrays.asList(MSPlanDesc.NESTED_LOOP, MSPlanDesc.MERGE_JOIN, MSPlanDesc.HASH_MATCH));

    public void analyzePlan() {
        if (Float.isNaN(internal)) {
            float totalSubtreeCost = 0;

            super.root = list.get(0);
            totalSubtreeCost = ((MSPlanDesc) list.get(0)).totalSubtreeCost;
            //System.out.println("ec: totalSubtreeCost initial " + totalSubtreeCost + " empty " + QD.emptyCost);
            for (ListIterator pi = list.listIterator(); pi.hasNext();) {
                MSPlanDesc PD = (MSPlanDesc) pi.next();
                //System.out.println("ec: " + PD.physicalOp + " " + PD.argument + " " + (PD.estimateIO + PD.estimateCPU) + " " + PD.totalSubtreeCost);
                if ((PD.getOpType() & (MSPlanDesc.INDEX_SCAN | MSPlanDesc.INDEX_SEEK | MSPlanDesc.TABLE_SCAN | MSPlanDesc.BKMK_LOOKUP)) != 0) {

                    //remove only those leaves that correspond to an index in the configuration ..,
                    String tableName = PD.getTable();
                    // totalSubtreeCost -= PD.estimateIO + PD.estimateCPU;
                    totalSubtreeCost -= PD.totalSubtreeCost;

                    if ((PD.getOpType() & (MSPlanDesc.INDEX_SEEK | MSPlanDesc.INDEX_SCAN)) != 0) {
                        String index = PD.getIndexName();
                        if (index != null) {
                            Float cost = super.accessCosts.get(index);
                            if (cost == null) {
                                super.accessCosts.put(index, PD.totalSubtreeCost);
                            } else {
                                super.accessCosts.put(index, cost + PD.totalSubtreeCost);
                            }
                        }
                    }

                    Float cost = super.accessCosts.get(tableName);
                    if (cost == null) {
                        super.accessCosts.put(tableName, PD.totalSubtreeCost);
                    } else {
                        super.accessCosts.put(tableName, cost + PD.totalSubtreeCost);
                    }
                }

                if (PD.getOpType() == MSPlanDesc.NESTED_LOOP) {
                    super.nljPlan = true;
                }
            }

            super.internal = totalSubtreeCost;
        }
    }

    public String getPlanXML() {
        return getPlanXML(filterTreeForProfile(), 0);
    }

    public DefaultMutableTreeNode filterTreeForProfile() {
        DefaultMutableTreeNode root = getPlanTree();
        Stack<DefaultMutableTreeNode> stack = new Stack();
        Enumeration postEnum = root.postorderEnumeration();
        while (postEnum.hasMoreElements()) {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) postEnum.nextElement();
            MSPlanDesc desc = (MSPlanDesc) node.getUserObject();
            String operator = desc.getOperator();
            DefaultMutableTreeNode parent = (DefaultMutableTreeNode) node.getParent();

            if (desc.isLeaf()) {
                stack.push((DefaultMutableTreeNode) node.clone());
                continue;
            } else if (desc.isJoin()) {
                DefaultMutableTreeNode node1 = (DefaultMutableTreeNode) node.clone();
                for(int i = 0; i< node.getChildCount(); i++) {
                    if(stack.isEmpty())
                        System.out.println("!!!! !!!!");

                    node1.add(stack.pop());
                }
                stack.push(node1);
            }
        }
        return stack.pop();
    }

    private String getPlanXML(DefaultMutableTreeNode node, int level) {
        StringBuffer buffer = new StringBuffer();
        MSPlanDesc desc = (MSPlanDesc) node.getUserObject();
        String oper = desc.getOperator();
        for (int i = 0; i < level; i++) {
            //buffer.append("  ");
        }

        if (msJoinSet.contains(desc.getOpType())) {
            buffer.append("<" + oper + ">");
            final Enumeration children = node.children();
            while (children.hasMoreElements()) {
                DefaultMutableTreeNode treeNode = (DefaultMutableTreeNode) children.nextElement();
                String substr = getPlanXML(treeNode, level + 1);
                buffer.append(substr);
            }
            buffer.append("<" + oper + "/>");
        }

        if (desc.getOpType() == MSPlanDesc.TABLE_SCAN) {
            buffer.append("<" + oper + " TABLE=\"" + desc.getTable() + "\">");
        } else if (desc.getOpType() == MSPlanDesc.INDEX_SCAN || desc.getOpType() == MSPlanDesc.INDEX_SEEK) {
            buffer.append("<" + oper + " TABLE=\"" + desc.getTableNameFromIndexName(desc.getIndexName()) + "\">");
        }
        return buffer.toString();
    }
}