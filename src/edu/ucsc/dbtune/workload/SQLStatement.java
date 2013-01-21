package edu.ucsc.dbtune.workload;

/**
 * Represents a SQL statement. Each {@code SQLStatement} object is tied to a {@code String} object
 * that contains the actual literal contents of the SQL statement.
 *
 * @author Ivo Jimenez
 */
public class SQLStatement
{
    /** category of statement. */
    private SQLCategory category;

    /** workload this statement corresponds to. */
    private String workload;

    /** literal contents of the statement. */
    private String sql;

    /** The frequency of the statement in the workload. */
    private double fq;

    /** The position of the statement in the workload. */
    private int position;

    /**
     * @param workload
     *      workload this statement corresponds to.
     * @param sql
     *      a sql statement.
     * @param position
     *      position of the statement in the workload
     */
    public SQLStatement(String workload, String sql, int position)
    {
        this(workload, sql, SQLCategory.from(sql), position);
    }

    /**
     * Constructs a {@code SQLStatement}. The constructor tries to infer the category of the
     * statement using the {@link SQLCategory#from} method.
     *
     * @param sql
     *      a sql statement.
     * @see SQLCategory#from
     */
    public SQLStatement(String sql)
    {
        this(null, sql, SQLCategory.from(sql), 0);
    }

    /**
     * Constructs a {@code SQLStatement} given the name of the workload it belongs to, the SQL 
     * category, the literal contents of the statement and the position of the statement in relation 
     * to others in the workload.
     *
     * @param workload
     *      workload this statement corresponds to.
     * @param category
     *      the corresponding {@link SQLCategory} representing the category of statement.
     * @param sql
     *      a sql statement.
     * @param position
     *      position of the statement in the workload
     */
    public SQLStatement(String workload, String sql, SQLCategory category, int position)
    {
        this.workload = workload;
        this.category = category;
        this.sql      = sql;
        this.position = position;

        // the default weight of the statement is 1.0
        fq = 1.0;
    }

    /**
     * Returns the category of statement.
     *
     * @return
     *     a sql category.
     * @see SQLCategory
     */
    public SQLCategory getSQLCategory()
    {
        return category;
    }

    /**
     * Returns the actual SQL statement.
     *
     * @return
     *     a string containing the SQL statement that was optimized.
     */
    public String getSQL()
    {
        return sql;
    }


    /**
     * Retrieves the weight of the statement.
     *
     * @return
     *      The weight of this statement
     */
    public double getStatementWeight()
    {
        return fq;
    }

    /**
     * Set the weight for the statement.
     *
     * @param fq
     *      The weight
     */
    public void setStatementWeight(double fq)
    {
        this.fq = fq;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return sql.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof SQLStatement))
            return false;

        SQLStatement o = (SQLStatement) obj;

        if (category.isSame(o.category) && sql.equals(o.sql))
            return true;

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "[ category=" + category +
               " text=\"" + sql + "\"]";
    }

    /**
     * Gets the workload for this instance.
     *
     * @return The workload.
     */
    public String getWorkload()
    {
        return this.workload;
    }

    /**
     * Gets the position for this instance.
     *
     * @return The position.
     */
    public int getPosition()
    {
        return this.position;
    }
}
