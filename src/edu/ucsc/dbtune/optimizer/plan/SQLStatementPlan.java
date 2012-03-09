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
    }

    /**
     * Creates a SQL statement plan with one (given root) node. Every node in the plan is duplicated 
     * by calling the {@link Operator#duplicate} method.
     *
     * @param other
     *     root of the plan
     */
    public SQLStatementPlan(SQLStatementPlan other)
    {
        this(other.sql, other.getRootOperator().duplicate());

        duplicateRecursively(root, other.root);
    }

    /**
     * Copies the subtree that hangs from {@code otherParent} and makes it a subtree of {@code 
     * thisParent}. This method is used only by the copy constructor.
     *
     * @param thisParent
     *      entry whose is expanded (whose children are being populated)
     * @param otherParent
     *      another entry whose children are copied to {@code thisParent}
     */
    private void duplicateRecursively(Entry<Operator> thisParent, Entry<Operator> otherParent)
    {
        Entry<Operator> thisChild;

        for (Entry<Operator> otherChild : otherParent.getChildren()) {
            thisChild = new Entry<Operator>(thisParent, otherChild.getElement().duplicate());

            thisParent.getChildren().add(thisChild);

            duplicateRecursively(thisChild, otherChild);
        }

        elements.put(thisParent.getElement(), thisParent);
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
     * Finds an operator that is an ascendant of the given one and has the given name.
     *
     * @param operator
     *      operator whose ascendants are looked for on matching the looked
     * @param name
     *      name of the operator that is being looked for
     * @return
     *      the node that was found; {@code null} otherwise.
     * @throws NoSuchElementException
     *      if {@code operator} is not a node in the plan
     */
    public Operator findAncestorWithName(Operator operator, String name)
    {
        if (!contains(operator))
            throw new NoSuchElementException("Element " + operator + " not in the plan");

        Operator ascendant = getParent(operator);

        while (ascendant != null)
            if (ascendant.getName().equals(name))
                return ascendant;
            else
                ascendant = getParent(ascendant);

        return null;
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
     * Returns the list of tables referenced by the statement.
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
     * Returns the list of {@link #INDEX_SCAN} and {@link #TABLE_SCAN} operators that are applied to 
     * a base database object. Operators for which the {@link #getDatabaseObjects} method is empty 
     * are not considered.
     *
     * @return
     *     the list of referenced tables
     */
    public List<Operator> getDataAccessOperators()
    {
        List<Operator> operators = new ArrayList<Operator>();

        for (Operator op : toList())
            if (!op.getDatabaseObjects().isEmpty())
                operators.add(op);

        return operators;
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
        if (System.identityHashCode(entry.getElement()) != System.identityHashCode(op))
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
        if (System.identityHashCode(entry.getElement()) != System.identityHashCode(op))
            throw new NoSuchElementException("Can't find " + op);

        op.setAccumulatedCost(cost);

        elements.put(op, entry);
    }

    /**
     * Assigns the cost to the given operator.
     *
     * @param op
     *      operator whose database object is to be removed
     */
    public void removeColumnsFetched(Operator op)
    {
        // elements is a hash table and if we rename the operator we need to do it with care since 
        // by modifying the name of the operator we modify its hashCode. So what we do here is to
        // first remove the value from the hash
        Entry<Operator> entry = elements.get(op);

        if (entry == null)
            throw new NoSuchElementException("Can't find " + op);
        if (System.identityHashCode(entry.getElement()) != System.identityHashCode(op))
            throw new NoSuchElementException("Can't find " + op);

        op.removeColumnsFetched();

        elements.put(op, entry);
    }

    /**
     * Assigns the cost to the given operator.
     *
     * @param op
     *      operator whose database object is to be removed
     */
    public void removePredicates(Operator op)
    {
        // elements is a hash table and if we rename the operator we need to do it with care since 
        // by modifying the name of the operator we modify its hashCode. So what we do here is to
        // first remove the value from the hash
        Entry<Operator> entry = elements.get(op);

        if (entry == null)
            throw new NoSuchElementException("Can't find " + op);
        if (System.identityHashCode(entry.getElement()) != System.identityHashCode(op))
            throw new NoSuchElementException("Can't find " + op);

        op.removePredicates();

        elements.put(op, entry);
    }

    /**
     * Assigns the cost to the given operator.
     *
     * @param op
     *      operator whose database object is to be removed
     */
    public void removeDatabaseObject(Operator op)
    {
        // elements is a hash table and if we rename the operator we need to do it with care since 
        // by modifying the name of the operator we modify its hashCode. So what we do here is to
        // first remove the value from the hash
        Entry<Operator> entry = elements.get(op);

        if (entry == null)
            throw new NoSuchElementException("Can't find " + op);
        if (System.identityHashCode(entry.getElement()) != System.identityHashCode(op))
            throw new NoSuchElementException("Can't find " + op);

        op.removeDatabaseObject();

        elements.put(op, entry);
    }

    /**
     * Checks if the plan contains the given operator.
     *
     * @param operatorName
     *      name of operator being looked for
     * @return
     *      {@code true} if the operator identified by the given name is contained in this plan; 
     *      {@code false} otherwise
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
    public int hashCode()
    {
        int code = 1;

        code = 37 * code + super.hashCode();
        code = 37 * code + sql.hashCode();

        return code;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (!super.equals(obj))
            return false;

        if (this == obj)
            return true;
    
        if (!(obj instanceof SQLStatementPlan))
            return false;
    
        SQLStatementPlan o = (SQLStatementPlan) obj;
    
        if (sql == null && o.sql == null || (sql != null && o.sql != null && sql.equals(o.sql)))
            return true;

        return false;
    }
}
