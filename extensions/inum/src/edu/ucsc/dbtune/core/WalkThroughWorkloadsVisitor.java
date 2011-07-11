package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Strings;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class WalkThroughWorkloadsVisitor implements PersistedWorkloadVisitor {
  private static final String EMPTY_STRING = "";
  @Override public String visit(WorkloadFileNode workload) {
    try {
      return Strings.wholeContentAsSingleLine(workload.getFile());
    } catch (IOException e) {
      Console.streaming().error("unable to read file", e);
      return EMPTY_STRING;
    }
  }

  @Override public Set<String> visit(WorkloadDirectoryNode directory) {
    final Set<String> allWorkloadContent = new HashSet<String>();
    for(PersistedWorkloadNode<?> each : directory.getChildren()){
      if(isSingleWorkload(each)){
        allWorkloadContent.add(((WorkloadFileNode)each).accept(this));
      }
    }
    return allWorkloadContent;
  }

  private boolean isSingleWorkload(PersistedWorkloadNode workload){
    return (!Strings.contains(workload.toString(), "children"));
  }
}
