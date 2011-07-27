package edu.ucsc.dbtune.core;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class WorkloadDirectoryNode implements PersistedWorkloadNode<Map<String, File>> {
  private final File directory;

  public WorkloadDirectoryNode(File directory){
    this.directory = directory;
  }

  @Override public Map<String, File> accept(PersistedWorkloadVisitor visitor) {
    return visitor.visit(this);
  }

  public File getDirectory(){
    return directory;
  }

  public List<PersistedWorkloadNode> getChildren(){
    final File[] files = getDirectory().listFiles();
    final List<PersistedWorkloadNode> workloads = new ArrayList<PersistedWorkloadNode>(files.length);
    for(File each : files){
      if(each.isDirectory()){
        workloads.add(new WorkloadDirectoryNode(each));
      } else {
        workloads.add(new WorkloadFileNode(each));
      }
    }

    return workloads;
  }

  @Override public String toString() {
    return String.format(
        "%s (children=%d)",
        getDirectory().getName(),
        getChildren().size());
  }
}
