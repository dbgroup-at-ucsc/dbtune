package edu.ucsc.dbtune.tools.cmudb.autopilot;

import edu.ucsc.dbtune.tools.cmudb.commons.Utils;
import edu.ucsc.dbtune.tools.cmudb.model.Plan;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import javax.swing.tree.DefaultMutableTreeNode;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Dec 20, 2007
 * Time: 9:47:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class DB2Plan extends Plan {
    private static final String NLJOIN = "NLJOIN";
    private static final String HSJOIN = "HSJOIN";
    private static final String MSJOIN = "MSJOIN";
    private static final String TBSCAN = "TBSCAN";
    private static final String IXSCAN = "IXSCAN";
    private static final String IXSEEK = "IXSEEK";
    private static final String FETCH = "FETCH";
    private static final String RIDSCAN = "RIDSCAN";

    private static final Logger _log = Logger.getLogger("DB2Plan");

    protected static final Set<String> joinSet = new HashSet<String>(Arrays.asList(MSJOIN, HSJOIN, NLJOIN));
    private Set tableOperators = new HashSet(Arrays.asList(TBSCAN, FETCH, RIDSCAN));
    private Map nljAccesses = new HashMap();

    public void analyzePlan() {
        StringBuffer str = new StringBuffer();
        nljAccesses.clear();
        accessCosts.clear();
        internal = 0;
        //if (Float.isNaN(internal)) {
        if (true) {
            try {
                DefaultMutableTreeNode root = getPlanTree();
                if(root == null) {
                    return;
                }
                super.root = (PlanDesc) root.getUserObject();
                internal = this.root.getCost();
                Enumeration preorder = root.postorderEnumeration();
                HashSet<Object> internalNodes = new HashSet<Object>();
                String currentIndex = null, currentTable = null;
                float subtreeCost = 0;
                double nljCount = 0;
                while (preorder.hasMoreElements()) {
                    DefaultMutableTreeNode node = (DefaultMutableTreeNode) preorder.nextElement();
                    DefaultMutableTreeNode node1 = (DefaultMutableTreeNode) node.getParent();
                    if (internalNodes.contains(node.getUserObject())) {
                        if (node1 != null)
                            internalNodes.add(node1.getUserObject());
                    } else {
                        final PlanDesc currentPlan = (PlanDesc) node.getUserObject();

                        if (node.isLeaf()) {
                            subtreeCost = 0;
                        }

                        if (isIndexAccess(currentPlan)) {
                            currentIndex = currentPlan.getTarget();
                            currentTable = getTable(currentPlan);
                            if(currentTable != null && (currentTable.toLowerCase().startsWith("idx") || currentTable.contains("_"))) {
                                currentTable = null;
                            }
                        } else if (isTableAccess(currentPlan) && currentPlan.getTarget() != null) {
                            currentTable = getTable(currentPlan);
                        }

                        if(currentPlan.getCardinality() > 0.0 ) {
                            nljCount = currentPlan.getCardinality();
                        }

                        if (!isCummulative()) {
                            subtreeCost += currentPlan.getCost();
                        } else {
                            subtreeCost = currentPlan.getCost();
                        }

                        // we are still at the edge.
                        if (parentInternal(node1)) {
                            currentPlan.cardinality = nljCount;
                            
                            if (node1 != null) {
                                PlanDesc parentDesc = (PlanDesc) node1.getUserObject();
                                String oper = parentDesc.getOperator();

                                if(oper.equals("NLJOIN")) {
                                    // how to get that we need the left or right?
                                    if(node1.getChildAt(1) == node) {
                                        // ok.. this is NLJ
                                        PlanDesc desc2 = (PlanDesc) ((DefaultMutableTreeNode)node1.getChildAt(0)).getUserObject();
                                        nljCount = desc2.getCardinality();
                                    }
                                }
                            }

                            // we got to the edge.
                            // find the table used.
                            if (currentIndex != null) {
                                accessCosts.put(currentIndex, subtreeCost);
                            }

                            if (currentTable != null) {
                                if(nljCount > 0)
                                    nljAccesses.put(currentTable, nljCount);
                                if(accessCosts.containsKey(currentTable)) {
                                    accessCosts.put(currentTable, (accessCosts.get(currentTable)+subtreeCost));
                                } else {
                                    accessCosts.put(currentTable, subtreeCost);
                                }
                            }

                            currentIndex = currentTable = null;

                            internal -= subtreeCost;
/*
                            if(nljCount > 0) {
                                internal -= subtreeCost * nljCount;
                            } else {
                                internal -= subtreeCost;
                            }
*/

                            if (node1 != null)
                                internalNodes.add(node1.getUserObject());
                        }
                    }
                }
            } catch (RuntimeException th) {
                System.err.println(list);
                throw th;
            }

            _log.info("analyzePlan: " + list);
        }
    }

    public DefaultMutableTreeNode filterTreeForProfile() {
        DefaultMutableTreeNode root = getPlanTree();
        Stack<DefaultMutableTreeNode> stack = new Stack();
        Enumeration postEnum = root.postorderEnumeration();
        while (postEnum.hasMoreElements()) {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) postEnum.nextElement();
            PlanDesc desc = (PlanDesc) node.getUserObject();
            String operator = desc.getOperator();
            DefaultMutableTreeNode parent = (DefaultMutableTreeNode) node.getParent();

            if ((TBSCAN.equals(operator) && desc.getTarget() != null)
                || isIndexAccess(desc)) {
                stack.push((DefaultMutableTreeNode) node.clone());
                continue;
            } else if (joinSet.contains(operator)) {
                DefaultMutableTreeNode node1 = (DefaultMutableTreeNode) node.clone();
                DefaultMutableTreeNode child1 = stack.pop();
                DefaultMutableTreeNode child2 = stack.pop();
                node1.add(child2);
                node1.add(child1);
                stack.push(node1);
            }
        }
        return stack.pop();
    }

    public String getPlanXML(DefaultMutableTreeNode root) {
        return getPlanXML(root, 0);
    }

    private String getPlanXML(DefaultMutableTreeNode node, int level) {
        StringBuffer buffer = new StringBuffer();
        PlanDesc desc = (PlanDesc) node.getUserObject();
        String oper = desc.getOperator();
        for (int i = 0; i < level; i++) {
            buffer.append("  ");
        }
        if (joinSet.contains(oper)) {
            buffer.append("<" + oper + ">" + Utils.NL);
            final Enumeration children = node.children();
            while (children .hasMoreElements()) {
                DefaultMutableTreeNode treeNode = (DefaultMutableTreeNode) children.nextElement();
                String substr = getPlanXML(treeNode, level+1);
                buffer.append(substr);
            }
            buffer.append("<" + oper + "/>" + Utils.NL);
        }

        if ("TBSCAN".equals(oper)) {
            buffer.append("<" + oper + " TABLE=\"" + desc.getTarget() + "\">" + Utils.NL);
        }
        return buffer.toString();
    }

    protected boolean isCummulative() {
        return true;
    }

    protected boolean isTableAccess(PlanDesc desc) {
        return tableOperators.contains(desc.getOperator());
    }

    protected String getTable(PlanDesc currentPlan) {
        return currentPlan.getTarget();
    }

    protected boolean parentInternal(DefaultMutableTreeNode node1) {
        return node1 == null || node1.getChildCount() > 1;
    }

    protected boolean isIndexAccess(PlanDesc currentPlan) {
        return currentPlan.getOperator().startsWith("IX");
    }

    @Override
    public boolean isNLJTable(String tableName) {
        return nljAccesses.containsKey(tableName) || nljAccesses.containsKey(tableName.toUpperCase());
    }

    public double getNLJMultipler(String tableName) {
        Double card = (Double) nljAccesses.get(tableName);
        if(card == null) {
            card = (Double) nljAccesses.get(tableName.toUpperCase());
        }
        if( card == null )
            return 0.0;
        else
            return card.doubleValue();
    }

    public void reset() {
        this.internal = Float.NaN;
    }
}