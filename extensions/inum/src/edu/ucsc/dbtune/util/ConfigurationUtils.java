package edu.ucsc.dbtune.util;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.metadata.Index;

/**
 * Utility class for dealing with configurations.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class ConfigurationUtils {
  private ConfigurationUtils() {
  }

  public static Set<String> findUsedTables(Set<Index> configuration) {
    final Set<String> tables = Sets.newHashSet();
    for (Index each : configuration) {
      final String name = each.getTable().getName();
      if (!tables.contains(name)) {
        tables.add(name);
      }
    }
    return ImmutableSet.copyOf(tables);
  }
}
