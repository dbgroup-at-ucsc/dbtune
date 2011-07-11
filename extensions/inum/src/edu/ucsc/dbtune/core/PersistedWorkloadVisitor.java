package edu.ucsc.dbtune.core;

import java.util.Set;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface PersistedWorkloadVisitor {
  String visit(WorkloadFileNode workload);
  Set<String> visit(WorkloadDirectoryNode directory);
}
