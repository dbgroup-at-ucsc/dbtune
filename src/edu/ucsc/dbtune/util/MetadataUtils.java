package edu.ucsc.dbtune.util;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.collect.Iterables;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.ColumnOrdering;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

import static edu.ucsc.dbtune.metadata.ColumnOrdering.ASC;

/**
 * @author Ivo Jimenez
 */
public final class MetadataUtils
{
    /**
     * Utility class.
     */
    private MetadataUtils()
    {
    }

    /**
     * Returns the set of schemas referenced by the given collection of tables.
     *
     * @param tables
     *      a collection of tables
     * @return
     *      the set of schemas corresponding to one or more tables in the set
     */
    public static Set<Schema> getReferencedSchemas(Collection<Table> tables)
    {
        Set<Schema> schemas = new HashSet<Schema>();

        for (Table t : tables)
            schemas.add(t.getSchema());

        return schemas;
    }

    /**
     * Partitions a set of indexes based on the table they refer.
     *
     * @param orderings
     *      a collection of orderings
     * @return
     *      the set of tables corresponding to one or more indexes in the set
     */
    public static Map<Table, Set<ColumnOrdering>> getOrderingsPerTable(
            Collection<ColumnOrdering> orderings)
    {
        Map<Table, Set<ColumnOrdering>> indexesPerTable = new HashMap<Table, Set<ColumnOrdering>>();
        Set<ColumnOrdering> indexesForTable;

        for (ColumnOrdering i : orderings) {

            indexesForTable = indexesPerTable.get(i.getTable());

            if (indexesForTable == null) {
                indexesForTable = new HashSet<ColumnOrdering>();
                indexesPerTable.put(i.getTable(), indexesForTable);
            }

            indexesForTable.add(i);
        }

        return indexesPerTable;
    }

    /**
     * Partitions a set of indexes based on the table they refer.
     *
     * @param indexes
     *      a collection of indexes
     * @return
     *      the set of tables corresponding to one or more indexes in the set
     */
    public static Map<Table, Set<Index>> getIndexesPerTable(Collection<Index> indexes)
    {
        Map<Table, Set<Index>> indexesPerTable = new HashMap<Table, Set<Index>>();
        Set<Index> indexesForTable;

        for (Index i : indexes) {

            indexesForTable = indexesPerTable.get(i.getTable());

            if (indexesForTable == null) {
                indexesForTable = new HashSet<Index>();
                indexesPerTable.put(i.getTable(), indexesForTable);
            }

            indexesForTable.add(i);
        }

        return indexesPerTable;
    }

    /**
     * Returns the set of indexes that reference one of the tables contained in {@code tables}.
     *
     * @param indexes
     *      a collection of indexes
     * @param tables
     *      a collection of tables
     * @return
     *      the set of indexes, where each references one table in {@code tables}
     */
    public static Set<Index> getIndexesReferencingTables(
            Collection<Index> indexes, Collection<Table> tables)
    {
        Set<Index> indexesReferencingTables = new HashSet<Index>();

        for (Index i : indexes)
            if (tables.contains(i.getTable()))
                indexesReferencingTables.add(i);

        return indexesReferencingTables;
    }

    /**
     * Returns the set of indexes that reference one the given table.
     *
     * @param indexes
     *      a collection of indexes
     * @param table
     *      a table
     * @return
     *      the set of indexes that reference {@code table}
     */
    public static Set<Index> getIndexesReferencingTable(
            Collection<Index> indexes, Table table)
    {
        Set<Index> indexesReferencingTable = new HashSet<Index>();

        for (Index i : indexes)
            if (table.equals(i.getTable()))
                indexesReferencingTable.add(i);

        return indexesReferencingTable;
    }

    /**
     * Returns the set of tables referenced by the given collection of indexes.
     *
     * @param indexes
     *      a collection of indexes
     * @return
     *      the set of tables corresponding to one or more indexes in the set
     */
    public static Set<Table> getReferencedTables(Collection<Index> indexes)
    {
        Set<Table> tables = new HashSet<Table>();

        for (Index i : indexes)
            tables.add(i.getTable());

        return tables;
    }

    /**
     * Finds an index by id in a set of indexes and throws an exception if it's not found.
     *
     * @param indexes
     *      set of indexes where one with the given id is being looked for
     * @param id
     *      id of the index being looked for
     * @return
     *      the index with the given id; {@code null} if not found
     * @throws NoSuchElementException
     *      if it's not found
     */
    public static Index findOrThrow(Set<Index> indexes, int id)
        throws NoSuchElementException
    {
        for (Index i : indexes)
            if (i.getId() == id)
                return i;

        throw new NoSuchElementException(
                "Can't find index with ID " + id + " in " + getDisplayList(indexes, "   "));
    }

    /**
     * Finds an index by id in a set of indexes.
     *
     * @param indexes
     *      set of indexes where one with the given id is being looked for
     * @param id
     *      id of the index being looked for
     * @return
     *      the index with the given id; {@code null} if not found
     */
    public static Index find(Set<Index> indexes, int id)
    {
        for (Index i : indexes)
            if (i.getId() == id)
                return i;

        return null;
    }

    /**
     * Finds an existing index with the given ordering. If none is found then the
     * 
     * @param ordering
     *      orders that are converted
     * @return
     *      the equivalent index or a new one with the given ordering
     * @throws SQLException
     *      if an error occurs while instantiating the index
     */
    public static Index findEquivalentOrCreateNew(ColumnOrdering ordering)
        throws SQLException
    {
        Schema sch = ordering.getColumns().get(0).getTable().getSchema();

        Index existing = sch.findIndex(ordering);

        if (existing != null)
            return existing;

        return new Index(ordering);
    }

    /**
     * Converts column orders to indexes.
     * 
     * @param orderings
     *      orders that are converted
     * @return
     *      set of indexes
     * @throws SQLException
     *      if an error occurs while instantiating indexes
     */
    public static Set<Index> convert(Set<ColumnOrdering> orderings)
        throws SQLException
    {
        Set<Index> indexes = new HashSet<Index>();

        if (orderings.isEmpty())
            return indexes;

        for (ColumnOrdering co : orderings)
            indexes.add(findEquivalentOrCreateNew(co));

        return indexes;
    }

    /**
     * Returns the set of indexes that are not shared by the sets. The comparison is done using 
     * {@link Index#equalsContent}.
     *
     * @param set1
     *      set of indexes
     * @param set2
     *      set of indexes
     * @return
     *      the number of indexes that are not in, both, set1 AND set2
     * @throws SQLException
     *      if the index can't be removed
     */
    public static int getNumberOfDistinctIndexes(Set<Index> set1, Set<Index> set2)
        throws SQLException
    {
        return
            Sets.difference(set1, set2).size() +
            Sets.difference(set2, set1).size();
    }

    /**
     * Checks two configurations to see if they're equivalent, with respect to the covering of each 
     * other. In this definition, a configuration {@code a} is equivalent to another one {@code b} 
     * if an index in {@code a} is covered or covers at least one index in {@code b}.
     *
     * @param a
     *      set of indexes
     * @param b
     *      set of indexes
     * @return
     *      {@code true} if {@code a} covers {@code b}
     */
    public static boolean equalsBasedOnCovering(Set<Index> a, Set<Index> b)
    {
        for (Index iA : a) {

            boolean isCoveredOrCovers = false;

            for (Index iB : b) {
                if (iB.isCoveredBy(iA) || iA.isCoveredBy(iB)) {
                    isCoveredOrCovers = true;
                    break;
                }
            }

            if (!isCoveredOrCovers)
                return false;
        }

        return true;
    }

    /**
     * Checks to configurations to see if one covers the other. A configuration {@code a} covers 
     * another one {@code b} if all the indexes of a are covering indexes of the indexes in {@code 
     * b}.
     *
     * @param a
     *      set of indexes
     * @param b
     *      set of indexes
     * @return
     *      {@code true} if {@code a} covers {@code b}
     */
    public static boolean covers(Set<Index> a, Set<Index> b)
    {
        for (Index iA : a) {

            boolean isCovered = false;

            for (Index iB : b) {
                if (iB.isCoveredBy(iA)) {
                    isCovered = true;
                    break;
                }
            }

            if (!isCovered)
                return false;
        }

        return true;
    }

    /**
     * Finds an index by name in a set of indexes. This looks only at the name of the of the index 
     * and not to the whole fully qualified one.
     *
     * @param indexes
     *      set of indexes where one with the given name is being looked for
     * @param name
     *      name of the index being looked for
     * @return
     *      the index with the given name; {@code null} if not found
     */
    public static Index find(Set<Index> indexes, String name)
    {
        String[] pathElements = name.split("\\.");
        String schemaName = null;
        String indexName;

        if (pathElements.length == 0)
            throw new RuntimeException("String is empty");

        if (pathElements.length == 1) {
            indexName = name;
        } else if (pathElements.length == 2) {
            schemaName = pathElements[0];
            indexName = pathElements[1];
        } else {
            throw new RuntimeException("Can't look for an index with 3 path elements");
        }

        if (schemaName != null)
            // search by schemaName.indexname first
            for (Index i : indexes)
                if (i.getSchema().getName().equals(schemaName) && i.getName().equals(indexName))
                    return i;

        // search by the name only
        for (Index i : indexes)
            if (i.getName().equals(indexName))
                return i;

        return null;
    }

    /**
     * Returns a string containing the set of CREATE statements required to create the given catalog 
     * on a DBMS.
     *
     * @param catalog
     *      catalog being dumped to a SQL CREATE statement
     * @return
     *      the string for the CREATE statements
     */
    public static String getCreateStatement(Catalog catalog)
    {
        StringBuilder create = new StringBuilder();

        for (Schema sch : catalog) {
            create.append("CREATE SCHEMA ").append(sch.getName()).append(";\n");
            for (Table tbl : sch.tables()) {
                create.append("CREATE TABLE ").append(tbl.getFullyQualifiedName()).append("(\n");
                for (Column col : tbl)
                    create.append("  ").append(col.getName()).append(" INT,\n");
                create.delete(create.length() - 2, create.length() - 0);
                create.append("\n);\n\n");
            }

            for (Index i : sch.indexes())
                create.append(getCreateStatement(i));
        }

        return create.toString();
    }

    /**
     * Generates the SQL statement that simulates the creation of an index. That is, this is the way 
     * the DBMS would scan the data, expressed as a SQL query.
     *
     * @param ordering
     *      the ordering for which the statement is being generated
     * @return
     *      the SQL statement that corresponds to the work that the DBMS needs to do in order to 
     *      create an index
     */
    public static String getMaterializationStatement(ColumnOrdering ordering)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        for (Column c : ordering.getColumns())
            sb.append(c.getName()).append(", ");

        sb.delete(sb.length() - 2, sb.length() - 1);
        sb.append("   FROM ").append(ordering.getTable().getFullyQualifiedName());
        sb.append("   ORDER BY ");

        for (Column c : ordering.getColumns())
            sb.append(c.getName()).append(ordering.getOrdering(c) == ASC ? " ASC, " : " DESC, ");

        sb.delete(sb.length() - 2, sb.length() - 1);

        return sb.toString();
    }

    /**
     * Generates the SQL statement that simulates the creation of an index. That is, this is the way 
     * the DBMS would scan the data, expressed as a SQL query.
     *
     * @param index
     *      the index for which the statement is being generated
     * @return
     *      the SQL statement that corresponds to the work that the DBMS needs to do in order to 
     *      create an index
     */
    public static String getMaterializationStatement(Index index)
    {
        try {
            return getMaterializationStatement(new ColumnOrdering(index));
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Generates the SQL create statement (standard) for the given index.
     *
     * @param index
     *      the index for which the statement is being generated
     * @return
     *      the create statement
     */
    public static String getCreateStatement(Index index)
    {
        StringBuilder create = new StringBuilder();

        create.append("CREATE INDEX ").append(index.getFullyQualifiedName());
        create.append(" ON ").append(index.getTable().getFullyQualifiedName()).append("(");
        create.append(getColumnListString(index));
        create.append(")");

        if (index.getScanOption() == Index.REVERSIBLE)
            create.append(" ALLOW REVERSE SCANS");

        create.append("\n");

        return create.toString();
    }

    /**
     * Obtains the transition cost for a pair of recommendations. The transition cost is defined in 
     * terms of the underlying DBMS' optimizer and it's just the sum of the {@link 
     * Index#getCreationCost creation cost} for all the indexes that are in {@code y} and not in 
     * {@code x}, i.e. for those indexes the set-difference.
     * <p>
     * Cost of dropping an index is assumed to be zero.
     *
     * @param x
     *      previous recommendation
     * @param y
     *      next recommendation
     * @return
     *      the cost of transitioning from x to y
     */
    public static double transitionCost(Set<Index> x, Set<Index> y)
    {
        double transition = 0;

        for (Index index : Sets.difference(y, x))
            transition += index.getCreationCost();

        return transition;
    }

    /**
     * Returns a string of the form {@code column1 ASC, column2 DESC, ...} for the given index.
     *
     * @param indexes
     *      set to display
     * @param offset
     *      to use on each entry of the list
     * @return
     *      the string
     */
    public static String getDisplayList(Set<Index> indexes, String offset)
    {
        if (indexes.isEmpty())
            return "";

        StringBuilder indexList = new StringBuilder();


        for (Index index : indexes) {
            indexList
                .append("\n").append(offset)
                .append(index.getId()).append(":")
                .append("INDEX ").append(index.getName())
                .append(Iterables.get(indexes, 0).getTable())
                .append(" (")
                .append(getColumnListString(index));
            indexList.delete(indexList.length() - 2, indexList.length() - 0);
            indexList.append(")");
        }

        return indexList.toString();
    }

    /**
     * Returns a string of the form {@code column1 ASC, column2 DESC, ...} for the given index.
     *
     * @param index
     *      an index
     * @return
     *      the string
     */
    public static String getColumnListString(Index index)
    {
        StringBuilder columnsString = new StringBuilder();

        for (Column col : index)
            columnsString
                .append(col.getName())
                .append(index.isAscending(col) ? " ASC" : " DESC")
                .append(", ");

        columnsString.delete(columnsString.length() - 2, columnsString.length() - 0);

        return columnsString.toString();
    }

    /**
     * Returns a string containing the name of an index based on the contents of the index.
     *
     * @param index list of indexes
     * @return a string containing the PG-dependent string representation of the given list, as the
     *         EXPLAIN INDEXES statement expects it
     */
    public static String getName(Index index)
    {
        StringBuilder str = new StringBuilder();
        boolean first = true;

        for (Column col : index) {
            if (first) { first = false; } else { str.append("_"); }

            str.append(col.getName());
        }
        str.append("_index");
        return str.toString();
    }
}
