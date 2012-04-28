package edu.ucsc.dbtune.metadata.extraction;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

/**
 * Extractor that uses JDBC's {@link DatabaseMetaData} class to obtain basic metadata information.
 * <p>
 * The class tries to extract, through the use of JDBC's {@link DatabaseMetaData}, name, data type, 
 * size and indexes (primary and secondary). When some information is not available through it, the 
 * corresponding metadata class (from the {@link edu.ucsc.dbtune.metadata} package) is empty. In the 
 * worst case, the returned {@link Catalog} object is empty. Also, a DBMS-specific implementation 
 * should find more efficient ways of retrieving metadata, which regularly is achieved by querying 
 * the system tables.
 *
 * @author Ivo Jimenez
 * @see DatabaseMetaData
 */
public abstract class GenericJDBCExtractor implements MetadataExtractor
{
    /** whether or not an extractor has swapped the catalog and schema terms. */
    protected boolean swappedTerms;

    /**
     * Given a database connection, it extracts metadata information. The information is comprised 
     * of all the {@link edu.ucsc.dbtune.metadata.DatabaseObject}s that exist in the database 
     * associated with the given {@link Connection} object and its context. With context we mean 
     * whatever is the connection's scope, i.e. the objects that are visible to it, as it is passed 
     * to this method. This varies depending on how the connection was configured, on the 
     * DBMS/driver being used (for a default configuration), amon other things.
     * <p>
     * This method tries to extract, through the use of JDBC's {@link DatabaseMetaData}, names, data 
     * types and indexes (primary and secondary). When some information is not available through it, 
     * the corresponding metadata class (from the {@link edu.ucsc.dbtune.metadata} package) is 
     * empty. In the worst case, the returned {@link Catalog} object is empty.
     *
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     if the driver corresponding to <code>connection</code> returns <code>null</code> when 
     *     calling the {@link java.sql.Connection#getMetaData} method; when an error occurs when 
     *     communicating with the underlying DBMS
     * @return
     *     the extracted catalog
     * @see DatabaseMetaData#getCatalogTerm
     * @see DatabaseMetaData#getCatalogs
     */
    @Override
    public Catalog extract(Connection connection) throws SQLException
    {
        Catalog catalog = new Catalog(connection.getCatalog());

        extractCatalog(catalog, connection);
        extractSchemas(catalog, connection);
        extractTables(catalog, connection);
        extractColumns(catalog, connection);
        extractIndexes(catalog, connection);
        extractObjectIDs(catalog, connection);
        extractBytes(catalog, connection);
        extractPages(catalog, connection);
        extractCardinality(catalog, connection);

        return catalog;
    }

    /**
     * Extracts the information corresponding to the {@link Catalog}.
     *
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     if the driver corresponding to <code>connection</code> returns <code>null</code> when 
     *     calling the {@link java.sql.Connection#getMetaData} method; when an error occurs when 
     *     communicating with the underlying DBMS
     * @see DatabaseMetaData#getCatalogTerm
     * @see DatabaseMetaData#getCatalogs
     */
    protected abstract void extractCatalog(Catalog catalog, Connection connection)
        throws SQLException;

    /**
     * Extracts the {@link Schema} objects contained in the catalog (a.k.a. database). The 
     * information is comprised of all the schemas that exist in the database as well as their 
     * names.
     * 
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     if the driver corresponding to <code>connection</code> returns <code>null</code> when 
     *     calling the {@link java.sql.Connection#getMetaData} method; when an error occurs when 
     *     communicating with the underlying DBMS
     * @see DatabaseMetaData#getSchemas
     */
    protected abstract void extractSchemas(Catalog catalog, Connection connection)
        throws SQLException;
    
    /**
     * Extracts the {@link Table} objects contained in each schema. The information is comprised of 
     * all the tables that exist in the database as well as their names.
     * 
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     if the driver corresponding to <code>connection</code> returns <code>null</code> when 
     *     calling the {@link java.sql.Connection#getMetaData} method; when an error occurs when 
     *     communicating with the underlying DBMS
     * @see DatabaseMetaData#getTables
     */
    protected void extractTables(Catalog catalog, Connection connection) throws SQLException
    {
        DatabaseMetaData meta;
        ResultSet        rs;

        meta = connection.getMetaData();

        if (meta == null)
            throw new SQLException("Connection " + connection + " doesn't handle JDBC metadata");

        String[] tableTypes = {"TABLE"};

        for (Schema sch : catalog) {

            if (!swappedTerms)
                rs = meta.getTables(null, sch.getName(), null, tableTypes);
            else
                rs = meta.getTables(sch.getName(), null, null, tableTypes);

            while (rs.next())
                new Table(sch, rs.getString("table_name"));

            rs.close();
        }
    }

    /**
     * Extracts the {@link Column} objects contained in each table. The information is comprised of 
     * all the columns that exist in the database as well as their names and data types.
     * 
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     if the driver corresponding to <code>connection</code> returns <code>null</code> when 
     *     calling the {@link java.sql.Connection#getMetaData} method; when an error occurs when 
     *     communicating with the underlying DBMS
     * @see DatabaseMetaData#getColumns
     */
    protected void extractColumns(Catalog catalog, Connection connection) throws SQLException
    {
        DatabaseMetaData meta;
        ResultSet rs;
        String columnName;
        int type;

        meta = connection.getMetaData();

        if (meta == null)
            throw new SQLException("Connection " + connection + " doesn't handle JDBC metadata");

        for (Schema sch : catalog.schemas()) {

            for (Table table : sch.tables()) {

                if (!swappedTerms)
                    rs = meta.getColumns(null, sch.getName(), table.getName(), "%");
                else
                    rs = meta.getColumns(sch.getName(), null, table.getName(), "%");

                while (rs.next()) {
                    columnName = rs.getString("column_name");
                    type = rs.getInt("data_type");

                    new Column(table, columnName, type);
                }

                rs.close();
            }
        }
    }

    /**
     * Extracts the {@link Index} objects contained in each schema. The information is comprised of 
     * all the indexes, their names, the columns contained in them and their constraints.
     * 
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     if the driver corresponding to <code>connection</code> returns <code>null</code> when 
     *     calling the {@link java.sql.Connection#getMetaData} method; when an error occurs when 
     *     communicating with the underlying DBMS
     * @see DatabaseMetaData#getIndexInfo
     */
    protected void extractIndexes(Catalog catalog, Connection connection) throws SQLException
    {
        Map<Integer, Column> indexToColumns;

        DatabaseMetaData meta;
        ResultSet        rs;
        Column           column;
        Index            index;
        String           columnName;
        String           indexName;
        int              type;
        boolean          isUnique;
        boolean          isClustered;
        boolean          isPrimary = false;

        meta = connection.getMetaData();

        if (meta == null)
            throw new SQLException("Connection " + connection + " doesn't handle JDBC metadata");

        for (Schema sch : catalog.schemas()) {
            for (Table table : sch.tables()) {

                indexToColumns = new HashMap<Integer, Column>();
                indexName      = "";
                index          = null;

                if (!swappedTerms)
                    rs = meta.getIndexInfo(null, sch.getName(), table.getName(), false, true);
                else
                    rs = meta.getIndexInfo(sch.getName(), null, table.getName(), false, true);

                while (rs.next()) {
                    type = rs.getShort("TYPE");

                    if (type == DatabaseMetaData.tableIndexStatistic) {
                        table.setPages(rs.getInt("pages"));
                        table.setCardinality(rs.getInt("cardinality"));
                    } else {

                        if (!indexName.equals(rs.getString("index_name"))) {

                            if (index != null)
                                for (int i = 0; i < indexToColumns.size(); i++)
                                    index.add(indexToColumns.get(i + 1));

                            type = rs.getShort("type");

                            if (type == DatabaseMetaData.tableIndexClustered)
                                isClustered = true;
                            else
                                isClustered = false;

                            isUnique  = !rs.getBoolean("non_unique");
                            indexName = rs.getString("index_name");
                            index     = new Index(sch, indexName, isPrimary, isClustered, isUnique);

                            indexToColumns = new HashMap<Integer, Column>();

                            index.setMaterialized(true);
                        }

                        columnName = rs.getString("column_name");
                        column     = table.findColumn(columnName);

                        if (column == null)
                            throw new SQLException(
                                    "Column " + columnName + " not in " + table.getName());

                        indexToColumns.put(rs.getInt("ordinal_position"), column);
                    }
                }

                // add the columns of the last index
                if (index != null)
                    for (int i = 0; i < indexToColumns.size(); i++)
                        index.add(indexToColumns.get(i + 1));

                rs.close();
            }
        }
    }

    /**
     * Extracts the object ID of each {@link edu.ucsc.dbtune.metadata.DatabaseObject} contained in 
     * the database. The JDBC specification doesn't provide a way of obtaining the internal object 
     * identifiers, so this has to necessarily be a DBMS-dependent implementation.     
     *
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     when an error occurs when communicating with the underlying DBMS
     * @see edu.ucsc.dbtune.metadata.DatabaseObject#getInternalID
     */
    protected abstract void extractObjectIDs(Catalog catalog, Connection connection)
        throws SQLException;

    /**
     * Extracts the size in bytes of each {@link edu.ucsc.dbtune.metadata.DatabaseObject} contained 
     * in the database. The JDBC specification doesn't provide a way of obtaining the amount of disk 
     * space that a database object occupies, so this has to necessarily be a DBMS-dependent 
     * implementation.     
     *
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     when an error occurs when communicating with the underlying DBMS
     * @see edu.ucsc.dbtune.metadata.DatabaseObject#getBytes
     */
    protected abstract void extractBytes(Catalog catalog, Connection connection)
        throws SQLException;

    /**
     * Extracts the number of pages that each {@link edu.ucsc.dbtune.metadata.DatabaseObject} has 
     * assigned to it. The JDBC specification doesn't provide a way of obtaining the amount of disk 
     * space that a database object occupies, so this has to necessarily be a DBMS-dependent 
     * implementation.
     *
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     when an error occurs when communicating with the underlying DBMS
     * @see edu.ucsc.dbtune.metadata.DatabaseObject#getPages
     */
    protected abstract void extractPages(Catalog catalog, Connection connection)
        throws SQLException;

    /**
     * Extracts the cardinality of each {@link edu.ucsc.dbtune.metadata.DatabaseObject} contained in 
     * the database.
     *
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     when an error occurs when communicating with the underlying DBMS
     * @see edu.ucsc.dbtune.metadata.DatabaseObject#getCardinality
     */
    protected void extractCardinality(Catalog catalog, Connection connection)
        throws SQLException
    {
        //extractTableCardinality(catalog, connection);
        //extractColumnCardinality(catalog, connection);
        //extractIndexCardinality(catalog, connection);
    }

    /**
     * Extracts the cardinality of each {@link Table} object contained in the database. This base 
     * implementation doesn't extract the cardinality of tables from the system catalog (i.e. it 
     * doesn't use the {@link DatabaseMetaData} implementation, instead, it executes the SQL 
     * statement:
     * <code>
     * SELECT
     *     count(*)
     * FROM
     *     schema_name.table_name;
     * </code>
     * This may be prohibitive for big tables so a DBMS-specific implementation will be required in 
     * order to have an efficient method.
     *
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     when an error occurs when communicating with the underlying DBMS
     * @see edu.ucsc.dbtune.metadata.DatabaseObject#getCardinality
     */
    protected void extractTableCardinality(Catalog catalog, Connection connection)
        throws SQLException
    {
        Statement stm;
        ResultSet rs;
        String    cmd;

        for (Schema sch : catalog.schemas()) {
            for (Table tbl : sch.tables()) {
                stm = connection.createStatement();
                cmd =
                    " SELECT " +
                    "   count(*) " +
                    " FROM " +
                        sch.getName() + "." + tbl.getName();

                rs = stm.executeQuery(cmd);

                while (rs.next())
                    tbl.setCardinality(rs.getLong(1));

                stm.close();
            }
        }
    }

    /**
     * Extracts the cardinality of each {@link Column} object contained in the database. This base 
     * implementation doesn't extract the cardinality of tables from the system catalog (i.e. it 
     * doesn't use the {@link DatabaseMetaData} implementation, instead, it executes the SQL 
     * statement:
     * <code>
     * SELECT
     *     count(DISTINCT column_name)
     * FROM
     *     schema_name.table_name;
     * </code>
     * This may be prohibitive for big tables so a DBMS-specific implementation will be required in 
     * order to have an efficient method.
     *
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     when an error occurs when communicating with the underlying DBMS
     * @see edu.ucsc.dbtune.metadata.DatabaseObject#getCardinality
     */
    protected void extractColumnCardinality(Catalog catalog, Connection connection)
        throws SQLException
    {
        Statement stm;
        ResultSet rs;
        String    cmd;

        for (Schema sch : catalog.schemas()) {
            for (Table tbl : sch.tables()) {
                for (Column col : tbl) {
                    stm = connection.createStatement();
                    cmd =
                        " SELECT " +
                        "   count(DISTINCT " + col.getName() + ") " +
                        " FROM " +
                            sch.getName() + "." + tbl.getName();

                    rs = stm.executeQuery(cmd);

                    while (rs.next())
                        col.setCardinality(rs.getLong(1));

                    stm.close();
                }
            }
        }
    }

    /**
     * Extracts the cardinality of each {@link Index} object contained in the database. This base 
     * implementation doesn't extract the cardinality of tables from the system catalog (i.e. it 
     * doesn't use the {@link DatabaseMetaData} implementation, instead, it executes the SQL 
     * statement:
     * <code>
     * SELECT
     *     count(*)
     *     column1,
     *     column2,
     *     ....
     * FROM
     *     schema_name.table_name;
     * GROUP BY
     *     column1,
     *     column2,
     *     ...
     * </code>
     * This may be prohibitive for big tables so a DBMS-specific implementation will be required in 
     * order to have an efficient method.
     *
     * @param catalog
     *     catalog being populated
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     when an error occurs when communicating with the underlying DBMS
     * @see edu.ucsc.dbtune.metadata.DatabaseObject#getCardinality
     */
    protected void extractIndexCardinality(Catalog catalog, Connection connection)
        throws SQLException
    {
        Statement stm;
        ResultSet rs;
        String    cmd;
        Table     table;

        for (Schema sch : catalog.schemas()) {

            for (Index  idx : sch.indexes()) {

                if (idx.size() == 0)
                    throw new SQLException("no columns in index " + idx.getName());

                if (idx.size() == 1) {
                    idx.setCardinality(idx.at(0).getCardinality());
                    continue;
                }

                cmd =
                    " SELECT " +
                    "   count(*),";

                table = null;

                for (Column col : idx.columns()) {
                    if (table == null)
                        table = col.getTable();

                    cmd += col.getName() + ", ";
                }

                cmd = cmd.substring(0, cmd.length() - 2);

                cmd +=
                    " FROM " +
                        table.getFullyQualifiedName() +
                    " GROUP BY ";

                for (Column col : idx.columns())
                    cmd += col.getName() + ", ";

                cmd = cmd.substring(0, cmd.length() - 2);
                stm = connection.createStatement();
                rs  = stm.executeQuery(cmd);

                while (rs.next())
                    idx.setCardinality(rs.getLong(1));

                stm.close();
            }
        }
    }
}
