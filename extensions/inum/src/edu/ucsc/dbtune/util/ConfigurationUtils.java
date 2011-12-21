package edu.ucsc.dbtune.util;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import java.util.Set;

/**
 * Utility class for dealing with configurations.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class ConfigurationUtils {
  private ConfigurationUtils() {
  }

  public static Set<String> findUsedTables(Configuration configuration) {
    final Set<String> tables = Sets.newHashSet();
    for (Index each : configuration.toList()) {
      final String name = each.getTable().getName();
      if (!tables.contains(name)) {
        tables.add(name);
      }
    }
    return ImmutableSet.copyOf(tables);
  }
}
