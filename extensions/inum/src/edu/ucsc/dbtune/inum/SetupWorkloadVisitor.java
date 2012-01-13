package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.Strings;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * Visitor that loads all workload files that will be used during {@link Inum}'s {@link
 * Precomputation setup} phase.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class SetupWorkloadVisitor implements WorkloadVisitor {
  private static final String NOTHING = "";

  @Override public String visit(WorkloadFileNode fileNode) {
    try {
      return Strings.wholeContentAsSingleLine(fileNode.getFile());
    } catch (IOException e) {
      Console.streaming().error("unable to read file", e);
      return NOTHING;
    }
  }

  @Override public Set<String> visit(WorkloadDirectoryNode directoryNode) {
    final Set<String> workload = Sets.newHashSet();
    for (WorkloadNode<?> each : directoryNode.getChildren()) {
      if (isWorkloadFile(each)) {
        final String content = ((WorkloadFileNode) each).accept(this);
        if (Strings.isEmpty(content)) { continue; }
        if (!workload.contains(content)) {
          workload.add(content);
        }
      }
    }
    return workload;
  }

  private boolean isWorkloadFile(WorkloadNode<?> workload) {
    return (!Strings.contains(workload.toString(), "children"));
  }
}
