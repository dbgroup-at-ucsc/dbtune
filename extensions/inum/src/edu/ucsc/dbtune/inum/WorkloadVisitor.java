package edu.ucsc.dbtune.inum;

import java.util.Set;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface WorkloadVisitor
{
  String visit(WorkloadFileNode fileNode);
  Set<String> visit(WorkloadDirectoryNode directoryNode);
}
