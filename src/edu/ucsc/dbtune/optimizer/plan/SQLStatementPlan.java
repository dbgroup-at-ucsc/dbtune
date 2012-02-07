package edu.ucsc.dbtune.optimizer.plan;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Tree;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Represents a plan for SQL statements of a RDBMS.
 *
 * @author Ivo Jimenez
 */
public class SQLStatementPlan extends Tree<Operator>
{
    /** to keep a register of inserted operators. */
    private int globalId = 1;

    /** the statement this plan corresponds to. */
    private SQLStatement sql;

    /**
     * Creates a SQL statement plan with one (given root) node.
     *
     * @param root
     *     root of the plan
     */
    public SQLStatementPlan(Operator root)
    {
        this(null, root);
    }

    /**
     * Creates a SQL statement plan with one (given root) node.
     *
     * @param sql
     *     statement that corresponds to the plan
     * @param root
     *     root of the plan
     */
    public SQLStatementPlan(SQLStatement sql, Operator root)
    {
        super(root);

        this.sql = sql;
        elements.clear();
        root.setId(globalId++);
        elements.put(root, this.root);
    }

    /**
     * Creates a SQL statement plan with one (given root) node.
     *
     * @param other
     *     root of the plan
     */
    public SQLStatementPlan(SQLStatementPlan other)
    {
        super(other);
        this.sql = other.sql;
        this.globalId = other.globalId;
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

        for (DatabaseObject ob : getDatabaseObjects())
            if (ob instanceof Index && !(ob instanceof InterestingOrder))
                indexes.add((Index) ob);

        return indexes;
    }

    /**
     * Return the list of tables referenced by the statement.
     *
     * @return
     *     the list of referenced tables
     */
    public List<Table> getTables()
    {
        List<Table> tables = new ArrayList<Table>();

        for (DatabaseObject ob : getDatabaseObjects())
            if (ob instanceof Table)
                tables.add((Table) ob);

        return tables;
    }

    /**
     * Checks if the plan contains the given operator.
     *
     * @param operatorName
     *      one of the possible defined operator names ({@link Operator})
     * @return
     *      {@code true} if the operator identified by the given name is contained in this plan; 
     *      {@code false} otherwise
     * @see Operator#NLJ
     * @see Operator#HJ
     * @see Operator#MJ
     */
    public boolean contains(String operatorName)
    {
        // TODO: make this more efficient by using a Map<String,Boolean>
        for (Operator o : toList())
            if (o.getName() == operatorName)
                return true;

        return false;
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
