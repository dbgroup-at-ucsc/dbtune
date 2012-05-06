package edu.ucsc.dbtune.metadata.extraction;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

/**
 * Metadata extractor for MySQL.
 * <p>
 * This class assumes a MySQL system version 5.5 or greater is on the backend and that JDBC 
 * connections are using the MySQL's JDBC driver (type 4) version 5.1 or greater.
 * <p>
 * The user associated to the {@link Connection} object needs 'read' privileges on the {@code 
 * information_schema} database (schema in DBTune's terminology; database in MySQL lingo).
 *
 * @author Ivo Jimenez
 */
public class MySQLExtractor extends GenericJDBCExtractor
{
    private int idCounter = 1;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractCatalog(Catalog catalog, Connection connection) throws SQLException
    {
        catalog.setName("mysql");
        catalog.setInternalID(idCounter++);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractSchemas(Catalog catalog, Connection connection) throws SQLException
    {
        DatabaseMetaData meta;
        ResultSet        rs;
        String           schName;

        meta = connection.getMetaData();

        if (meta == null)
            throw new SQLException("Connection " + connection + " doesn't handle JDBC metadata");

        rs = meta.getCatalogs();

        while (rs.next()) {

            schName = rs.getString("table_cat");

            if (schName.equals("information_schema") || schName.equals("mysql"))
                continue;

            Schema sch = new Schema(catalog, schName);

            sch.setInternalID(idCounter++);
        }

        rs.close();

        swappedTerms = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractIndexes(Catalog catalog, Connection connection) throws SQLException
    {
        Map<Integer, Column> ordinalToColumns;
        Map<Column, Boolean> ascendingValues;

        DatabaseMetaData meta;
        ResultSet        rs;
        Column           column;
        Index            index;
        String           columnName;
        String           currentIndexName;
        String           nextIndexName;
        int              type;
        boolean          isUnique;
        boolean          isClustered;
        boolean          isPrimary;

        meta = connection.getMetaData();

        for (Schema sch : catalog) {
            for (Table table : sch.tables()) {

                table.setInternalID(idCounter++);

                ordinalToColumns = new TreeMap<Integer, Column>();
                ascendingValues = new HashMap<Column, Boolean>();
                currentIndexName = "";
                index            = null;

                rs = meta.getIndexInfo(sch.getName(), null, table.getName(), false, true);

                while (rs.next()) {
                    type          = rs.getShort("type");
                    nextIndexName = rs.getString("index_name");

                    if (nextIndexName.equals("PRIMARY"))
                        nextIndexName = table + "_pkey";

                    if (!currentIndexName.equals(nextIndexName)) {
                        
                        if (!ordinalToColumns.isEmpty()) {

                            type = rs.getShort("type");

                            if (type == DatabaseMetaData.tableIndexClustered)
                                isClustered = true;
                            else
                                isClustered = false;

                            isUnique         = !rs.getBoolean("non_unique");
                            currentIndexName = rs.getString("index_name");

                            if (currentIndexName.equals("PRIMARY")) {
                                currentIndexName = table + "_pkey";
                                isPrimary        = true;
                            } else {
                                isPrimary = false;
                            }

                            index =
                                new Index(
                                        currentIndexName,
                                        new ArrayList<Column>(ordinalToColumns.values()),
                                        ascendingValues,
                                        isPrimary,
                                        isClustered,
                                        isUnique);

                            index.setMaterialized(true);
                            index.setInternalID(idCounter++);

                            ordinalToColumns.clear();
                            ascendingValues.clear();
                        }
                    }

                    columnName = rs.getString("column_name");
                    column     = table.findColumn(columnName);

                    if (column == null)
                        throw new SQLException(
                                "Column " + columnName + " not in " + table.getName());

                    ordinalToColumns.put(rs.getInt("ordinal_position"), column);
                    ascendingValues.put(
                            column, rs.getString("asc_or_desc").equals("A") ? true : false);
                }

                // add the columns of the last index
                if (index != null) {
                    type = rs.getShort("type");

                    if (type == DatabaseMetaData.tableIndexClustered)
                        isClustered = true;
                    else
                        isClustered = false;

                    isUnique         = !rs.getBoolean("non_unique");
                    currentIndexName = rs.getString("index_name");

                    if (currentIndexName.equals("PRIMARY")) {
                        currentIndexName = table + "_pkey";
                        isPrimary        = true;
                    } else {
                        isPrimary = false;
                    }

                    index =
                        new Index(
                                currentIndexName,
                                new ArrayList<Column>(ordinalToColumns.values()),
                                ascendingValues,
                                isPrimary,
                                isClustered,
                                isUnique);

                    index.setMaterialized(true);
                    index.setInternalID(idCounter++);
                }

                rs.close();

                for (Column col : table)
                    col.setInternalID(idCounter++);
            }
        }
    }

    @Override
    protected void extractObjectIDs(Catalog catalog, Connection connection) throws SQLException
    {
        // MySQL doesn't assign internal IDS; the IDS are generated incrementally in #extractIndexes
    }

    @Override
    protected void extractBytes(Catalog catalog, Connection connection) throws SQLException
    {
        // nothing yet
    }
    @Override
    protected void extractPages(Catalog catalog, Connection connection) throws SQLException
    {
        // nothing yet
    }
}
