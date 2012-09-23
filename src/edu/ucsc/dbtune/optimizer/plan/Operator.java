package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.util.Tree.Entry;

/**
 * Represents an operator of a SQL statement plan.
 *
 * @author Ivo Jimenez
 */
public class Operator
{
    /** delete operator. **/
    public static final String DELETE = "DELETE";

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
    /** insert operator. **/
    public static final String INSERT = "INSERT";


    /** row id scan operator. **/
    public static final String RID_SCAN = "RID.SCAN";

    /** fetch operator. **/
    public static final String FETCH = "FETCH";

    /** update operator. **/
    public static final String UPDATE = "UPDATE";

    /** sort operator. **/
    public static final String SORT = "SORT";

    /** sort operator. **/
    public static final String SUBQUERY = "SUBQUERY";

    /**
     * scan of a table that is generated by the DBMS, i.e. the scan of a table that is not a base 
     * table.
     */
    public static final String TEMPORARY_TABLE_SCAN = "TEMPORARY.TABLE.SCAN";

    /** Name of operator. */
    public String name;
    
    /** OUTER or INNER join. */
    public String joinInput;
    
    /**  */
    public int rows=-1;
    
    /**  */
    public int rowWidth=-1;
    
    /** Alias in explain table, for example: Q1 Q2 Q3 */
    public String aliasInExplainTables;
    
    /** Alias of a removed sub node, for example: Q1 Q2 Q3 */
    public String aliasInExplainTables2;

    /** If a fetch node is removed and merged with this node,
     * then this is the QID of the fetch node for example: Q1 Q2 Q3 */
    public String fetchAliasInExplainTables;
    
    /** Accumulated cost of the plan up to this operator. */
    public double accumulatedCost;
    
    /** Cost of table scan or index scan. */
    public double scanCost;
    
    /** The following variable are used for debugging purpose*/
    public int id;
    public double ioCost;
    public double cpuCost;
    public double first_row_cost;
    public double re_total_cost;
    public double re_io_cost;
    public double re_cpu_cost;
    public double buffers;
    public double internalCost;
    
    /** columns returned by database */
    public String rawColumnNames;
    
    /** predicate returned by database */
    public List<String> rawPredicateList;
    
    /** filter factor of the predicates */
    public List<Double> filterFactorList;
   
    /**
     * If this operator is inside the right child tree of
     * a Nest Loop Join operator, the coefficient should be
     * row count of that NLJ operator. If this operator is
     * inside the right child tree of multiple NLJ operators,
     * The coefficient should be the product of row count of
     * each NLJ operator.
     */
    public double coefficient=1;

    /** Number of tuples that the operator produces. */
    public double cardinality;
    
    /** Same as cardinality. Tuned for NLJ */
    public double cardinalityNLJ;

    /** When the operator is applied to base objects. */
    public List<DatabaseObject> objects;
    
    /** The predicates associated with the operator. */
    public List<Predicate> predicates;

    /** columns fetched by the operator. */
    public InterestingOrder columnsFetched;

    /** which slot this operator was instantiated */
    public TableAccessSlot slot;
    /**
     * creates an empty operator ({@code name="empty"}. This can be used to represent empty plans.
     */
    public Operator()
    {
        this("empty", 0.0, 0);
    }
    
    /**
     * save everything to a xml node
     * @param rx
     */
    public void save(Rx rx, Entry<Operator> self) {
        rx.setAttribute("name", name);
        rx.setAttribute("id", id);
        Rx params=rx.createChild("parameters");
        params.setAttribute("accumulatedCost", accumulatedCost);
        params.setAttribute("cardinality", cardinality);
        params.setAttribute("internalCost", internalCost);
        params.setAttribute("coefficient", coefficient);
        Rx extra=rx.createChild("extraParameters");
        extra.setAttribute("ioCost", ioCost);
        extra.setAttribute("cpuCost", cpuCost);
        extra.setAttribute("first_row_cost", first_row_cost);
        extra.setAttribute("re_total_cost", re_total_cost);
        extra.setAttribute("re_io_cost", re_io_cost);
        extra.setAttribute("re_cpu_cost", re_cpu_cost);
        extra.setAttribute("buffers", buffers);
        if (objects.size() > 0) {
            Rx dbo = rx.createChild("databaseObjects");
            for (DatabaseObject object : objects) {
                dbo.createChild("object", object.toString());
            }
        }
        if (predicates.size() > 0) {
            Rx pre = rx.createChild("predicates");
            for (Predicate predicate : predicates) {
                predicate.save(pre.createChild("predicate"));
            }
        }
        if (rawPredicateList.size() > 0) {
            Rx pre = rx.createChild("rawPredicates");
            for (String predicate : rawPredicateList) {
                pre.createChild("predicate",predicate);
            }
        }
        if (columnsFetched != null)
            rx.createChild("interestingOrder", columnsFetched.toString());
        if (rawColumnNames != null)
            rx.createChild("rawColumnNames", rawColumnNames);
        if (slot != null)
            rx.createChild("slot", slot.toString());
        if (self != null) {
            for (Entry<Operator> child : self.getChildren()) {
                child.getElement().save(rx.createChild("operator"), child);
            }
        }
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
     * @param objects
     *     list of database objects associated to the operator
     * @param predicates
     *     list of predicates associated to the operator
     * @param columnsFetched
     *     columnsFetched by the operator
     */
    public Operator(
            String name,
            double accumulatedCost,
            double cardinality,
            List<DatabaseObject> objects,
            List<Predicate> predicates,
            InterestingOrder columnsFetched)
    {
        this.name = new String(name);
        this.accumulatedCost = accumulatedCost;
        this.cardinality = cardinality;
        this.cardinalityNLJ = cardinality;
        this.objects = new ArrayList<DatabaseObject>(objects);
        this.predicates = new ArrayList<Predicate>(predicates);
        this.columnsFetched = columnsFetched;
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
    public Operator(String name, double accumulatedCost, double cardinality)
    {
        this.name            = name;
        this.accumulatedCost = accumulatedCost;
        this.cardinality     = cardinality;
        this.cardinalityNLJ     = cardinality;
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
        this(o.name, o.accumulatedCost, o.cardinality, o.objects, o.predicates, o.columnsFetched);
        this.id=o.id;
        this.ioCost=o.ioCost;
        this.cpuCost=o.cpuCost;;
        this.first_row_cost=o.first_row_cost;
        this.re_total_cost=o.re_total_cost;
        this.re_io_cost=o.re_io_cost;
        this.re_cpu_cost=o.re_cpu_cost;
        this.buffers=o.buffers;
        this.internalCost=o.internalCost;
        this.rawColumnNames=o.rawColumnNames;
        this.rawPredicateList=o.rawPredicateList;
        this.aliasInExplainTables=o.aliasInExplainTables;
        this.aliasInExplainTables2=o.aliasInExplainTables2;
        this.fetchAliasInExplainTables=o.fetchAliasInExplainTables;
        this.coefficient=o.coefficient;
        this.cardinalityNLJ=o.cardinalityNLJ;
        this.scanCost=o.scanCost;
        this.joinInput= o.joinInput;
    }

    /**
     * Duplicates this object.
     *
     * @return
     *      a duplicate of this operator
     */
    public Operator duplicate()
    {
    	Operator operator= new Operator(this);
    	return operator;
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
     * Get table associated with this operator
     * @return Table which associated with this operator
     * @throws SQLException
     */
    public Table getTable() throws SQLException {
        if (Operator.TEMPORARY_TABLE_SCAN.equals(getName()))
            return null;
        if (this instanceof TableAccessSlot) {
            TableAccessSlot op = (TableAccessSlot) this;
            return op.getTable();
        } else {
            List<DatabaseObject> objects = this.getDatabaseObjects();
            if (objects.size() == 0)
                return null;
            if (objects.size() != 1)
                throw new SQLException("Leaf should contain one object");
            DatabaseObject object = objects.get(0);
            if (object instanceof Table)
                return (Table) object;
            else if (object instanceof Index)
                return ((Index) object).getTable();
            else
                throw new Error();
        }
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
    public double getCardinality()
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
     * Whether or not the operator is one of the join operators defined.
     *
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
     * Whether or not the operator is a table access one, i.e. its type is {@link INDEX_SCAN}, 
     * {@link FETCH} or {@link TABLE_SCAN}, and the {@link #getDatabaseObjects} method returns a 
     * non-empty list.
     *
     * @return
     *      {@code true} if the operator is accessing a base table; {@code false} otherwise.
     */
    public boolean isDataAccess()
    {
        if (getDatabaseObjects().isEmpty())
            return false;

        if (name.equals(TABLE_SCAN) || name.equals(FETCH) || name.equals(INDEX_SCAN))
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
            .append("(")
            .append(this.aliasInExplainTables==null?"":this.aliasInExplainTables+" ")
            .append("id=")
            .append(id);
        if ( joinInput!=null)
            str.append(" ").append(joinInput);
        str.append(" cost=")
            .append(String.format("%.2f", accumulatedCost));
        if (rows>=0)
            str.append(" rows=")
            .append(String.format("%d", rows));
        if (rowWidth>=0)
            str.append(" rowWidth=")
            .append(String.format("%d", rowWidth));
        str.append(" card=")
            .append(String.format("%.2f", cardinality));
        if (Math.abs(cardinality-cardinalityNLJ)>0.1)
            str.append(String.format("/%.2f", cardinalityNLJ));
        str.append(" coeff=")
            .append(String.format("%.1f", coefficient));
        str.append(" object=")
            .append(objects.isEmpty() ? "NONE" : objects.get(0))
//            .append(" predicate=")
//            .append(this.predicates == null ? "NONE" : predicates)
            .append(" rawColumns=")
            .append(this.predicates == null ? "NONE" : rawColumnNames)
            .append(" rawPredicate=")
            .append(this.predicates == null ? "NONE" : rawPredicateList)
            .append(" fetch=")
            .append(columnsFetched == null ? "NONE" : columnsFetched)
//            .append(" io=")
//            .append(ioCost)
//            .append(" cpu=")
//            .append(cpuCost)
            .append(" internal=")
            .append(String.format("%.2f", internalCost))
//            .append(" fr=")
//            .append(first_row_cost)
//            .append(" rcost=")
//            .append(re_total_cost)
//            .append(" rio=")
//            .append(re_io_cost)
//            .append(" rcpu=")
//            .append(re_cpu_cost)
//            .append(" buffers=")
//            .append(buffers)
            .append(")");

        return str.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        //id is unique for each operator
        if (true)
            return id;
        int code = 1;

        code = 37 * code + name.hashCode();
        code = 37 * code + (int) Double.doubleToLongBits(accumulatedCost);
        long c=(long)cardinality;
        code = 37 * code + (int) (c ^ (c >>> 32));

        int listCode = 0;
        for (DatabaseObject dbo : objects)
            listCode += dbo.hashCode();
        code = 37 * code + listCode;

        listCode = 0;
        for (Predicate p : predicates)
            listCode += p.hashCode();
        code = 37 * code + listCode;

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
        
      //id is unique for each operator
        if (true)
        	return op.id==this.id;

        if (!name.equals(op.name) ||
                Double.compare(accumulatedCost, op.accumulatedCost) != 0 ||
                cardinality != op.cardinality ||
                !predicates.containsAll(op.predicates) ||
                !objects.containsAll(op.objects))
            return false;

        if ((columnsFetched == null && op.columnsFetched == null) ||
            (columnsFetched != null && op.columnsFetched != null &&
                columnsFetched.isCoveredBy(op.columnsFetched) &&
                op.columnsFetched.isCoveredBy(columnsFetched)))
            return true;

        return false;
    }
}