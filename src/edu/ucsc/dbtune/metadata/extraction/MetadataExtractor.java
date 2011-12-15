package edu.ucsc.dbtune.metadata.extraction;

import edu.ucsc.dbtune.metadata.Catalog;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for the main class of the metadata extraction package
 *
 * @author Ivo Jimenez
 */
public interface MetadataExtractor
{
    /**
     * Given a database connection, it extracts metadata information. The information is comprised 
     * of all the database objects defined in the database that is associated with the connection.
     * 
     * @param connection
     *     object used to obtain metadata for its associated database
     * @see edu.ucsc.dbtune.metadata
     */
    public Catalog extract(Connection connection) throws SQLException;
}
