package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.util.Pair;
import java.io.File;
import java.util.Map;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface PersistedWorkloadVisitor {
  Pair<String, File> visit(WorkloadFileNode workload);
  Map<String, File> visit(WorkloadDirectoryNode directory);
}
