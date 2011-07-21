/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.ucsc.dbtune.inum.autopilot;

import edu.ucsc.dbtune.inum.commons.Utils;
import edu.ucsc.dbtune.inum.model.Plan;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.logging.Logger;
import javax.swing.tree.DefaultMutableTreeNode;


/**
 *
 * @author cristina
 */
/**
 * @author Yannis
 *
 */
public class PostgresPlan extends Plan {

	/*
	"RESULT", "APPEND", "BITMAP_AND", "BITMAP_OR"
	"NLJOIN" //Nested Loop, Nested Loop Left Join, Nested Loop Full Join, Nested Loop Right Join, Nested Loop IN Join
	"MSJOIN" //Merge Join, Merge Left Join, Merge Full Join, Merge Right Join, Merge IN Join
	"HSJOIN" //Hash Join, Hash Left Join, Hash Full Join, Hash Right Join, Hash IN Join
	"HASH"
	"TBSCAN" //Seq Scan 
	"BP_IXSCAN" //Bitmap Index Scan 
	"BP_HSCAN" //Bitmap Heap Scan 
	"IXSCAN" //Index Scan 
	"TIDSCAN" //Tid Scan 
	"SUBQUERY_SCAN" , "FUNCTION_SCAN", "VALUES_SCAN", 
	"MATERIALIZE", "SORT";
	"GROUP", "AGGREGATE", "GP_AGGREGATE", "H_AGGREGATE", "UNIQUE";
	"SETOP", "LIMIT" 
	 */
	/*
		"IXSCAN", "TBSCAN", "BP_HSCAN", "TIDSCAN"
		"BP_IXSCAN", "FUNCTION_SCAN", "VALUES_SCAN"
		"SUBQUERY_SCAN"
	*/
	private static final String NLJOIN = "NLJOIN"; //Nested loop join
	private static final String MSJOIN = "MSJOIN"; //Merge join
	private static final String HSJOIN = "HSJOIN"; //Hash join
	
	private static final String TBSCAN = "TBSCAN"; //Seq scan
	private static final String BP_HSCAN = "BP_HSCAN"; //Bitmap Heap Scan
	private static final String TIDSCAN = "TIDSCAN"; //Tid Scan
	
	private static final String FUNCTION_SCAN = "FUNCTION_SCAN"; //Function Scan
	private static final String VALUES_SCAN = "VALUES_SCAN"; //Values Scan
	
	private static final String BP_IXSCAN = "BP_IXSCAN"; //Bitmap Index Scan 
	private static final String IXSCAN = "IXSCAN"; //Index scan
	
	private static final Logger _log = Logger.getLogger("PostgresPlan");// nu stiu ce e asta
	
	protected static final Set<String> joinSet = new HashSet<String>(Arrays.asList(MSJOIN, HSJOIN, NLJOIN));
	
	//private Set tableOperators = new HashSet(Arrays.asList(TBSCAN, BP_HSCAN, TIDSCAN)); 
	private Set tableOperators = new HashSet(Arrays.asList(TBSCAN, TIDSCAN)); 
	private Set scanOperators = new HashSet(Arrays.asList(FUNCTION_SCAN, VALUES_SCAN)); 
	private Set indexOperators = new HashSet(Arrays.asList(IXSCAN,BP_IXSCAN)); 
	private Map nljAccesses = new HashMap();
	//Plan
	private String plan = "";
    
    public String getPlan() {
		return plan;
	}

	public void setPlan(String plan) {
		this.plan = plan;
	}

    /*Compute internal plan cost (subtract table access operations)*/
	public void analyzePlan()
	{
        accessCosts.clear();
        internal = 0;
        float totalAccessesCosts = 0;
        if(list.size() > 0)
        	super.root = (PlanDesc) list.get(0);
        for (int i = 0; i < list.size(); i++)
        {
        	PlanDesc temp = list.get(i);
        	if (temp.getOperator().equals("NLJOIN"))
        		super.nljPlan = true;
        	String operator = temp.getOperator();
        	if(tableOperators.contains(operator) || scanOperators.contains(operator) || indexOperators.contains(operator)){
        		totalAccessesCosts += temp.getCost();
        		accessCosts.put(temp.getTarget().toLowerCase(), temp.getCost());
        	}
        }
        
        internal = root.getCost() - totalAccessesCosts;
	}    



	public DefaultMutableTreeNode filterTreeForProfile() 
	{
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

	//TODO: dash said this is only for DB2. I will delete it
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

     //returns true if the operator is a table access operator
    protected boolean isTableAccess(PlanDesc desc) {
        return tableOperators.contains(desc.getOperator());
    }

    //returns the target table name
    protected String getTable(PlanDesc currentPlan) {
        return currentPlan.getTarget();
    }

   protected boolean parentInternal(DefaultMutableTreeNode node1) {
        return node1 == null || node1.getChildCount() > 1;
    }

    //returns if it is an index access
    protected boolean isIndexAccess(PlanDesc currentPlan) {
        return currentPlan.getOperator().startsWith("IX");
    }

    //returns true if the nljAccesses hashMap contains the table name
    @Override
    public boolean isNLJTable(String tableName) {
        return nljAccesses.containsKey(tableName) || nljAccesses.containsKey(tableName.toUpperCase());
    }

    //TODO: understands what nljAccesses contains
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



