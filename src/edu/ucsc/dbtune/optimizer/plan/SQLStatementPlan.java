package edu.ucsc.dbtune.optimizer.plan;

import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Tree;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.util.List;
import java.util.ArrayList;

/**
 * Represents a plan for SQL statements of a RDBMS.
 */
public class SQLStatementPlan extends Tree<Operator>
{
    /** to keep a register of inserted operators */
    private int globalId = 1;

    /** the statement this plan corresponds to */
    private SQLStatement sql;

    /**
     * Creates a SQL statement plan with one (given root) node.
     *
     * @param root
     *     root of the plan
     */
    public SQLStatementPlan(Operator root)
    {
        super(root);

        this.sql = null;
        elements.clear();
        root.setId(globalId++);
        elements.put(root,this.root);
    }

    /**
     * Creates a SQL statement plan with one (given root) node.
     *
     * @param root
     *     root of the plan
     */
    public SQLStatementPlan(SQLStatement sql, Operator root)
    {
        super(root);

        this.sql = sql;
        elements.clear();
        root.setId(globalId++);
        elements.put(root,this.root);
    }

    /**
     * Returns the statement that this plan corresponds to.
     *
     * @return
     *     statement from which this plan was obtained
     */
    public SQLStatement getStatement()
    {
        return sql;
    }

    /**
     * Assigns the statement that this plan corresponds to.
     *
     * @param sql
     *     statement from which this plan was obtained
     */
    public void setStatement(SQLStatement sql)
    {
        this.sql = sql;
    }

    /**
     * Returns the operator at the root of the plan.
     *
     * @return
     *     root node of the plan
     */
    public Operator getRootOperator()
    {
        return super.getRootElement();
    }

    /**
     * Aggregates the set of database objects referenced by all the operators in a list and returns 
     * it.
     *
     * @return
     *     list of objects referenced by one or more operators in the plan.
     */
    public List<DatabaseObject> getDatabaseObjects()
    {
        List<DatabaseObject> objects = new ArrayList<DatabaseObject>();

        for (Operator op : toList()) {
            objects.addAll(op.getDatabaseObjects());
        }

        return objects;
    }

    /**
     * Returns the set of indexes referenced by the plan.
     *
     * @return
     *     list of indexes referenced by the operators in the plan.
     */
    public List<Index> getIndexes()
    {
        List<Index> indexes = new ArrayList<Index>();

        for (DatabaseObject ob : getDatabaseObjects()) {
            if (ob instanceof Index) {
                indexes.add((Index) ob);
            }
        }

        return indexes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Entry<Operator> setChild(Operator parentValue, Operator childValue)
    {
        Entry<Operator> e;

        childValue.setId(globalId++);
        
        e = super.setChild(parentValue, childValue);

        return e;
    }
}
