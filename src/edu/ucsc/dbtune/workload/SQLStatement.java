package edu.ucsc.dbtune.workload;

import java.io.Serializable;

/**
 * Represents a SQL statement. Each {@code SQLStatement} object is tied to a {@code String} object 
 * that contains the actual literal contents of the SQL statement.
 *
 * @author Ivo Jimenez
 */
public class SQLStatement implements Serializable
{
    /**
     *  Default 
     */
    private static final long serialVersionUID = 1L;

    /** category of statement. */
    private SQLCategory category;

    /** literal contents of the statement. */
    private String sql;
    
    /** The frequency of the statement in the workload */
    private int fq;
    
    
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
        this(sql, SQLCategory.from(sql));
    }

    /**
     * Constructs a {@code SQLStatement} given its category and the literal contents.
     *
     * @param category
     *      the corresponding {@link SQLCategory} representing the category of statement.
     * @param sql
     *      a sql statement.
     */
    public SQLStatement(String sql, SQLCategory category)
    {
        this.category = category;
        this.sql      = sql;
        
        // the default weight of the statement is 1
        fq = 1;
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
     * Retrieves the weight of the statement
     * 
     * @return
     *      The weight of this statement
     */
    public int getStatementWeight()
    {
        return fq;
    }
    
    /**
     * Set the weight for the statement
     * 
     * @param fq
     *      The weight
     */
    public void setStatementWeight(int fq)
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
}
