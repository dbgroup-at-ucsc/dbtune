package edu.ucsc.dbtune.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.ByContentIndex;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

import static com.google.common.collect.Sets.difference;

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
     * @param indexes
     *      a collection of indexes
     * @return
     *      the set of tables corresponding to one or more indexes in the set
     */
    public static Map<Table, Set<Index>> getIndexesPerTable(Collection<? extends Index> indexes)
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
            Collection<? extends Index> indexes, Collection<Table> tables)
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
            Collection<? extends Index> indexes, Table table)
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
    public static Set<Table> getReferencedTables(Collection<? extends Index> indexes)
    {
        Set<Table> tables = new HashSet<Table>();

        for (Index i : indexes)
            tables.add(i.getTable());

        return tables;
    }

    /**
     * Finds an index by id in a set of indexes.
     *
     * @param indexes
     *      set of indexes where one with the given name is being looked for
     * @param id
     *      id of the index being looked for
     * @return
     *      the index with the given id; {@code null} if not found
     */
    public static Index find(Set<? extends Index> indexes, int id)
    {
        for (Index i : indexes)
            if (i.getId() == id)
                return i;

        return null;
    }

    /**
     * Makes a set of {@link Index} to {@link ByContentIndex}.
     *
     * @param indexes
     *      set of indexes where each will be copied to a ByContentIndex instance
     * @return
     *      the index with the given id; {@code null} if not found
     */
    public static Set<ByContentIndex> toByContent(Set<Index> indexes)
    {
        Set<ByContentIndex> byContentSet = new HashSet<ByContentIndex>();

        for (Index i : indexes)
            byContentSet.add(new ByContentIndex(i));

        return byContentSet;
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
     */
    public static int getNumberOfDistinctIndexesByContent(Set<Index> set1, Set<Index> set2)
    {
        Set<ByContentIndex> byContentSet1 = toByContent(set1);
        Set<ByContentIndex> byContentSet2 = toByContent(set2);

        return
            difference(byContentSet1, byContentSet2).size() +
            difference(byContentSet2, byContentSet1).size();
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

            for (Index idx : sch.indexes()) {
                create.append("CREATE INDEX ").append(idx.getFullyQualifiedName());
                create.append(" ON ").append(idx.getTable().getFullyQualifiedName()).append("(");

                for (Column col : idx)
                    create.append("").append(col.getName()).append(", ");

                create.delete(create.length() - 2, create.length() - 0);

                create.append(");\n");
            }

        }

        return create.toString();
    }
}
