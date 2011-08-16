package edu.ucsc.dbtune.inum;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.spi.Environment;
import java.io.File;
import java.io.FileFilter;
import java.util.Set;

/**
 * Represents a workload directory (a directory where some workload files reside).
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class WorkloadDirectoryNode implements WorkloadNode <Set<String>> {
  private final File directory;
  static final String INUM_WORKLOAD_PATH = Environment.getInstance().getScriptAtWorkloadsFolder(
      "inum/");

  public WorkloadDirectoryNode(File directory){
    this.directory = directory;
  }

  public WorkloadDirectoryNode(){
   this(new File(INUM_WORKLOAD_PATH));
  }

  @Override public Set<String> accept(WorkloadVisitor visitor) {
    return visitor.visit(this);
  }

  public Set<WorkloadNode<?>> getChildren(){
    final File[] files = getDirectory().listFiles(new OnlyDirectoriesOrSqlFiles());
    final Set<WorkloadNode<?>> workload = Sets.newHashSet();
    for(File each : files){
      if(each.isDirectory()){
        final WorkloadDirectoryNode directory = new WorkloadDirectoryNode(each);
        if(!workload.contains(directory)){
          workload.add(directory);
        }
      } else {
        final WorkloadFileNode node = new WorkloadFileNode(each);
        if(!workload.contains(node)){
          workload.add(node);
        }
      }
    }

    return workload;
  }

  public File getDirectory(){
    return directory;
  }

  @Override public String toString() {
    return String.format(
        "%s (children=%d)",
        getDirectory().getName(),
        getChildren().size());
  }

  private static class OnlyDirectoriesOrSqlFiles implements FileFilter {
    @Override public boolean accept(File file) {
      return file.isDirectory() || file.getName().endsWith(".sql");
    }
  }
}
