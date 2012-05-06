package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.HashMap;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

/**
 * Used to represent the full table scan access in an INUM template plan. There's only one single 
 * object of this type per table, i.e. this can be viewed as a singleton class but instead of 
 * creating one object throughout the JVM, this class creates one for every distinct table contained 
 * in a {@link Schema}.
 *
 * @author Trung Tran
 * @author Ivo Jimenez
 * @see edu.ucsc.dbtune.optimizer.plan.InumPlan
 * @see edu.ucsc.dbtune.optimizer.plan.TableAccessSlot
 */
public final class FullTableScanIndex extends Index
{
    private static Map<Table, Index> instances = new HashMap<Table, Index>();
    
    protected Table table;

    /**
     * Creates an object corresponding to the given table. There's only one object per table.
     *
     * @param table
     *      the table this FTS corresponds to
     * @throws SQLException
     *      if the schema of the table is null or can't be retrieved
     */
    private FullTableScanIndex(Table table) throws SQLException
    {
        super(table.getName() + "_full_table_scan", table.getSchema());

        this.table = table;
    }

    /**
     * Returns the single instance of this class corresponding to the given table.
     *
     * @param table
     *      table for which the corresponding instance is being retrieved
     * @return
     *      the full table scan corresponding to the given table
     * @throws SQLException
     *      if the instance can't be instantiated
     */
    public static FullTableScanIndex getFullTableScanIndexInstance(Table table) throws SQLException
    {
        FullTableScanIndex ftsIndex = (FullTableScanIndex) instances.get(table);

        if (ftsIndex == null) {
            ftsIndex = new FullTableScanIndex(table);
            instances.put(table, ftsIndex);
        }

        return ftsIndex;
    }

    /**
     * A given index covers the FTS, only if the index is covering all the columns in the table and
     * the clustering of the table corresponds to the sorting of the given index.
     * 
     * @param other
     *      the index that is checked
     * @return
     *      {@code true} if the index covers the FTS; {@code false} otherwise
     */
    @Override
    public boolean isCoveredBy(Index other)
    {
        if (other.size() == table.size())
            throw new RuntimeException("Not handled yet");

        if ((other instanceof FullTableScanIndex) &&
                (((FullTableScanIndex) other)).getTable().equals(getTable()))
            return true;

        
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table getTable()
    {
        return table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return super.getFullyQualifiedName().hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof FullTableScanIndex))
            return false;

        FullTableScanIndex fts = (FullTableScanIndex) other;

        return table.equals(fts.table);
    }
}
