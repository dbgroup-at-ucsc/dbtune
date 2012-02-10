package edu.ucsc.dbtune.optimizer.plan;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.util.Identifiable;

/**
 * Represents an operator of a SQL statement plan.
 *
 * @author Ivo Jimenez
 */
public class Operator implements Comparable<Operator>, Identifiable
{
    /** NLJ operator. **/
    public static final String NLJ = "NESTED.LOOP.JOIN";
    /** NLJ operator. **/
    public static final String NESTED_LOOP_JOIN = NLJ;

    /** Indexed NL join operator. **/
    public static final String INLJ = "INDEXED.NESTED.LOOP.JOIN";
    /** Indexed NL join operator. **/
    public static final String INDEXED_NESTED_LOOP_JOIN = INLJ;

    /** hash join operator. **/
    public static final String HJ = "HASH.JOIN";
    /** hash join operator. **/
    public static final String HASH_JOIN = HJ;

    /** merge sort join operator. **/
    public static final String MSJ = "SORT.MERGE.JOIN";
    /** merge sort join operator. **/
    public static final String SMJ = MSJ;
    /** merge sort join operator. **/
    public static final String SORT_MERGE_JOIN = MSJ;
    /** merge sort join operator. **/
    public static final String MERGE_SORT_JOIN = MSJ;

    /** table scan operator. **/
    public static final String TS = "TABLE.SCAN";
    /** table scan operator. **/
    public static final String TABLE_SCAN = TS;

    /** index scan operator. **/
    public static final String IS = "INDEX.SCAN";
    /** table scan operator. **/
    public static final String INDEX_SCAN = IS;

    /** row id scan operator. **/
    public static final String RID_SCAN = "RID_SCAN";

    /** fetch operator. **/
    public static final String FETCH = "FETCH";

    /** sort operator. **/
    public static final String SORT = "SORT";

    /** ID used to identify an operator within a plan. */
    protected int id;

    /** Name of operator. */
    protected String name;

    /** Accumulated cost of the plan up to this operator. */
    protected double accumulatedCost;

    /** Cost of the operator (not accumulated cost). */
    protected double cost;

    /** Number of tuples that the operator produces. */
    protected long cardinality;

    /** When the operator is applied to base objects. */
    protected List<DatabaseObject> objects;
    
    /** The predicates associated with the operator. */
    protected List<Predicate> predicates;

    /** columns fetched by the operator. */
    protected InterestingOrder columnsFetched;

    /**
     * creates an empty operator ({@code name="empty"}. This can be used to represent empty plans.
     */
    public Operator()
    {
        this("empty", 0.0, 0);
    }

    /**
     * creates an operator of the given name.
     *
     * @param name
     *     name of the operator
     * @param accumulatedCost
     *     cost of the plan up to this operator
     * @param cardinality
     *     number of rows produced by the operator
     */
    public Operator(String name, double accumulatedCost, long cardinality)
    {
        this.id              = 0;
        this.name            = name;
        this.cost            = 0.0;
        this.accumulatedCost = accumulatedCost;
        this.cardinality     = cardinality;
        this.objects         = new ArrayList<DatabaseObject>();
        this.predicates      = new ArrayList<Predicate>();
    }

    /**
     * Copies an operator.
     *
     * @param o
     *      other operator being copied
     */
    Operator(Operator o)
    {
        this.id = o.id;
        this.name = o.name;
        this.cost = o.cost;
        this.accumulatedCost = o.accumulatedCost;
        this.cardinality = o.cardinality;
        this.objects = o.objects;
        this.predicates = o.predicates;
        this.columnsFetched = o.columnsFetched;
    }

    /**
     * Adds a {@link DatabaseObject} to the list of objects that are touched by this operator. 
     * Usually this corresponds to base operators like sequential and index scans, as well as 
     * columns used in predicates.
     *
     * @param dbObject
     *     the object that this operator is processing
     */
    public void add(DatabaseObject dbObject)
    {
        objects.add(dbObject);
    }
    
    /**
     * @param columnsFetched
     *     the columns fetched by this operator
     */
    public void addColumnsFetched(InterestingOrder columnsFetched)
    {
        this.columnsFetched = columnsFetched;
    }

    /**
     * Returns the columns that are fetched by this operator.
     *
     * @return
     *      columns that are processed by this operator
     */
    public InterestingOrder getColumnsFetched()
    {
        return columnsFetched;
    }

    /**
     * Adds predicates to the list of predicates that are associated with the operator.
     *
     * @param predicates
     *      the predicates
     */
    public void add(List<Predicate> predicates)
    {
        this.predicates.addAll(predicates);
    }

    /**
     * Returns the list of objects that are touched by this operator.
     *
     * @return
     *     list of objects that are referenced by the operator
     */
    public List<DatabaseObject> getDatabaseObjects()
    {
        return new ArrayList<DatabaseObject>(objects);
    }

    /**
     * Returns the list of objects that are touched by this operator.
     *
     * @return
     *     list of objects that are referenced by the operator
     */
    public List<Predicate> getPredicates()
    {
        return new ArrayList<Predicate>(predicates);
    }

    /**
     * returns the operator id.
     *
     * @return
     *     operator id
     */
    @Override
    public int getId()
    {
        return id;
    }

    /**
     * assigns the value of the operator id.
     *
     * @param id
     *     operator id
     */
    public void setId(int id)
    {
        this.id = id;
    }

    /**
     * assigns the name of the operator.
     *
     * @param name
     *     operator id
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * returns the accumulated cost of the operator.
     *
     * @return
     *     the value corresponding to the accumulated cost of the operator
     */
    public double getAccumulatedCost()
    {
        return accumulatedCost;
    }

    /**
     * returns the cost of the operator.
     *
     * @param cost
     *     the value corresponding to the cost of the operator
     */
    public void setCost(double cost)
    {
        this.cost = cost;
    }

    /**
     * returns the cost of the operator.
     *
     * @return
     *     the value corresponding to the cost of the operator
     */
    public double getCost()
    {
        return cost;
    }

    /**
     * returns the number of rows produced by the operator.
     *
     * @return
     *     cardinality of the operator
     */
    public long getCardinality()
    {
        return cardinality;
    }

    /**
     * returns the operator name.
     *
     * @return
     *     name of operator
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return
     *      {@code true} if the operator is a join; {@code false} otherwise.
     */
    public boolean isJoin()
    {
        if (name.equals(NLJ) || name.equals(MSJ) || name.equals(HJ) || name.equals(INLJ))
            return true;

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Operator operator)
    {
        return new Integer(this.id).compareTo(operator.id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();

        str.append(id + ": " + name + "(cost=" + accumulatedCost + " rows=" + cardinality + ")\n");

        if (!objects.isEmpty())
            str.append("   Objects: \n");

        for (DatabaseObject obj : objects)
            str.append("       " + obj.getClass().getName() + ": " + obj + "\n");

        return str.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return new Integer(id).hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof Operator))
            return false;

        Operator op = (Operator) o;

        return getId() == op.getId();
    }
}
