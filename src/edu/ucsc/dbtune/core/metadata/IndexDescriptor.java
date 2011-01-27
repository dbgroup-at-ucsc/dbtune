package edu.ucsc.dbtune.core.metadata;

import edu.ucsc.dbtune.core.DatabaseIndexSchema;

/**
 * An immutable type that describes {@code indexes}.
 * The implementation of this type depends on the supported DBMS. i.e., one implementation
 * per supported DBMS.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface IndexDescriptor {
    /**
     * @return an index's internal id.
     */
    int getInternalId();

    /**
     * @return the username (whoever is using the database).
     */
    String getOwner();

    /**
     * convenience method that provides a shortcut to {@code #getSchema().hashCode()}
     * for caching purposes.
     * @return the cached hashcode of {@link #getSchema()}.
     */
    int getCachedHashcode();

    /**
     * @return the cost that it will take to create this index.
     */
    double getCreationCost();

    /**
     * @return the index's creation text.
     */
    String getCreationStatement();

    /**
     * @return a DBMS-specific {@link DatabaseIndexSchema} object.
     */
    DatabaseIndexSchema getSchema();

    /**
     * @return the number of columns part of this index.
     */
    boolean getColumnsCount();

    /**
     * @return the size of the index (in megabytes).
     */
    double getSize();
}
