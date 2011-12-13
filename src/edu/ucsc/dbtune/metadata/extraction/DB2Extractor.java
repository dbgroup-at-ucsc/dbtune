package edu.ucsc.dbtune.metadata.extraction;

import edu.ucsc.dbtune.metadata.Catalog;

import java.sql.SQLException;
import java.sql.Connection;

/**
 * Metadata extractor for DB2.
 * <p>
 * This class assumes a DB2 system version ?? or greater is on the backend and connections
 * created using the postgres' JDBC driver (type 4) version ?? or greater.
 *
 * @author Ivo Jimenez
 */
public class DB2Extractor extends GenericJDBCExtractor
{
    @Override
    protected void extractCatalog(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
    @Override
    protected void extractSchemas(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
    @Override
    protected void extractObjectIDs(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
    @Override
    protected void extractBytes(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
    @Override
    protected void extractPages(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
    @Override
    protected void extractCardinality(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
}
