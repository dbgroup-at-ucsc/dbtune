package edu.ucsc.dbtune.optimizer.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

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
        return getRootElement();
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

        for (Operator op : toList())
            objects.addAll(op.getDatabaseObjects());

        return objects;
    }

    /**
     * Aggregates the set of database objects referenced by all the operators in a list and returns 
     * it.
     *
     * @return
     *     list of objects referenced by one or more operators in the plan.
     */
    public List<DatabaseObject> getDatabaseObjectsAtLeafs()
    {
        List<DatabaseObject> objects = new ArrayList<DatabaseObject>();

        for (Operator op : leafs())
            objects.addAll(op.getDatabaseObjects());

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
            if (ob instanceof Index)
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
            else if (ob instanceof Index)
                tables.add(((Index) ob).getTable());

        return tables;
    }

    /**
     * Renames the given operator.
     *
     * @param op
     *      operator to be renamed
     * @param newName
     *      new name to give to the operator
     */
    public void rename(Operator op, String newName)
    {
        // elements is a hash table and if we rename the operator we need to do it with care since 
        // by modifying the name of the operator we modify its hashCode. So what we do here is to
        // first remove the value from the hash
        Entry<Operator> entry = elements.get(op);

        if (entry == null)
            throw new NoSuchElementException("Can't find " + op);

        op.setName(newName);

        elements.put(op, entry);
    }

    /**
     * Assigns the cost to the given operator.
     *
     * @param op
     *      operator to be renamed
     * @param cost
     *      cost of the operator
     */
    public void assignCost(Operator op, double cost)
    {
        // elements is a hash table and if we rename the operator we need to do it with care since 
        // by modifying the name of the operator we modify its hashCode. So what we do here is to
        // first remove the value from the hash
        Entry<Operator> entry = elements.get(op);

        if (entry == null)
            throw new NoSuchElementException("Can't find " + op);

        op.setAccumulatedCost(cost);

        elements.put(op, entry);
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
}
