package edu.ucsc.dbtune.core;

import com.google.common.collect.Maps;
import edu.ucsc.dbtune.inum.commons.Pair;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Strings;
import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class LoadingWorkloadsVisitor implements PersistedWorkloadVisitor {
  private static final Pair<String, File> EMPTY_PAIR = Pair.empty();
  @Override public Pair<String, File> visit(WorkloadFileNode workload) {
    try {
      final Pair<String, File> pair;
      pair = Pair.of(Strings.wholeContentAsSingleLine(workload.getFile()), workload.getFile());
      return pair;
    } catch (IOException e) {
      Console.streaming().error("unable to read file", e);
      return EMPTY_PAIR;
    }
  }

  @Override public Map<String, File> visit(WorkloadDirectoryNode directory) {
    final Map<String, File> allWorkloadContent = Maps.newHashMap();
    for(PersistedWorkloadNode<?> each : directory.getChildren()){
      if(isSingleWorkload(each)){
        final Pair<String, File> eachWorkload = ((WorkloadFileNode)each).accept(this);
        allWorkloadContent.put(eachWorkload.getLeft(), eachWorkload.getRight());
      }
    }
    return allWorkloadContent;
  }

  private boolean isSingleWorkload(PersistedWorkloadNode workload){
    return (!Strings.contains(workload.toString(), "children"));
  }
}
