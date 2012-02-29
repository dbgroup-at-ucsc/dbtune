package edu.ucsc.dbtune.optimizer.plan;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Double.doubleToLongBits;

import edu.ucsc.dbtune.metadata.DatabaseObject;

/**
 * Represents an operator of a SQL statement plan.
 *
 * @author Ivo Jimenez
 */
public class Operator
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
    /** index anding. **/
    public static final String INDEX_AND = "INDEX.AND";
    /** index oring. **/
    public static final String INDEX_OR = "INDEX.OR";

    /** row id scan operator. **/
    public static final String RID_SCAN = "RID.SCAN";

    /** fetch operator. **/
    public static final String FETCH = "FETCH";

    /** sort operator. **/
    public static final String SORT = "SORT";

    /** sort operator. **/
    public static final String SUBQUERY = "SUBQUERY";

    /** Name of operator. */
    protected String name;

    /** Accumulated cost of the plan up to this operator. */
    protected double accumulatedCost;

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
        this.name            = name;
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
        this.name = new String(o.name);
        this.accumulatedCost = o.accumulatedCost;
        this.cardinality = o.cardinality;
        this.objects = new ArrayList<DatabaseObject>(o.objects);
        this.predicates = new ArrayList<Predicate>(o.predicates);
        this.columnsFetched = o.columnsFetched;
    }

    /**
     * Duplicates this object.
     *
     * @return
     *      a duplicate of this operator
     */
    public Operator duplicate()
    {
        return new Operator(this);
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
     * assigns the name of the operator. <b>WARNING: </b> by using this method, a plan might get 
     * corrupted since the {@link edu.ucsc.dbtune.util.Tree} class maintains a hash list internally 
     * and modifying the name means that the hashCode of this object changes. Thus, don't use this 
     * method unless you are updating the internal map of the plan accordingly.
     *
     * @param name
     *     operator id
     * @see Plan#rename
     */
    void setName(String name)
    {
        this.name = name;
    }

    /**
     * assigns the cost of the operator. <b>WARNING: </b> by using this method, a plan might get 
     * corrupted since the {@link edu.ucsc.dbtune.util.Tree} class maintains a hash list internally 
     * and modifying the name means that the hashCode of this object changes. Thus, don't use this 
     * method unless you are updating the internal map of the plan accordingly.
     *
     * @param cost
     *     new cost
     * @see Plan#assignCost
     */
    void setAccumulatedCost(double cost)
    {
        this.accumulatedCost = cost;
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
     * removes the object associated to the operator.
     */
    public void removeDatabaseObject()
    {
        if (objects.size() != 1)
            throw new RuntimeException("Operator " + this + " doesn't have ONE database object");

        objects.clear();
    }

    /**
     * removes the predicates associated to the operator.
     */
    public void removePredicates()
    {
        predicates.clear();
    }

    /**
     * removes the predicates associated to the operator.
     */
    public void removeColumnsFetched()
    {
        columnsFetched = null;
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
    public String toString()
    {
        StringBuilder str = new StringBuilder();

        str.append(name)
            .append("(cost=")
            .append(accumulatedCost)
            .append(" rows=")
            .append(cardinality)
            .append(" object=")
            .append(objects.isEmpty() ? "NONE" : objects.get(0))
            .append(" fetch=")
            .append(columnsFetched == null ? "NONE" : columnsFetched)
            .append(")");

        return str.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        int code = 1;

        code = 37 * code + name.hashCode();
        code = 37 * code + (int) Double.doubleToLongBits(accumulatedCost);
        code = 37 * code + (int) (accumulatedCost * 100);
        code = 37 * code + (int) (cardinality ^ (cardinality >>> 32));
        code = 37 * code + objects.hashCode();
        code = 37 * code + predicates.hashCode();

        if (columnsFetched != null)
            code = 37 * code + columnsFetched.hashCode();

        return code;
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

        if (!name.equals(op.name) ||
                doubleToLongBits(accumulatedCost) != doubleToLongBits(op.accumulatedCost) ||
                cardinality != op.cardinality ||
                !predicates.equals(op.predicates) ||
                !objects.equals(op.objects))
            return false;

        if ((columnsFetched == null && op.columnsFetched == null) ||
            (columnsFetched != null && op.columnsFetched != null &&
                columnsFetched.equals(op.columnsFetched)))
            return true;

        return false;
    }
}
