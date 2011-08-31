package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.inum.InumInterestingOrdersExtractor.ColumnInformation;
import java.util.Properties;
import java.util.Set;

/**
 * It calls the dbms to obtain the necessary column information
 * to help the extraction of interesting orders.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface ColumnProperty {
  /**
   * Obtain {@link ColumnInformation column information} of a given
   * {@code reloid}.
   * @param reloid
   *    relation oid.
   * @return a set of column information objects matching a {@code reloid}.
   */
  Set<ColumnInformation> getColumnInformation(int reloid);

  /**
   * @return a live {@link DatabaseConnection db connection}.
   * @throws IllegalStateException if the returning database connection is closed.
   */
  DatabaseConnection getDatabaseConnection();

  /**
   * Obtain a property value for a given key.
   * @param key
   *    property's assigned key.
   * @return
   *    property's value.
   */
  String getProperty(String key);

  /**
   * @return all stored properties.
   */
  Properties getProperties();

  /**
   * refresh the property values held by this {@link ColumnProperty object}.
   */
  void refresh();
}
