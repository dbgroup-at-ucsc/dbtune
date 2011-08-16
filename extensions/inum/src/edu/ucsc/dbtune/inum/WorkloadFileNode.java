package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.util.Strings;
import java.io.File;

/**
 * Represent an individual workload file.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class WorkloadFileNode implements WorkloadNode<String> {
  private final File file;
  public WorkloadFileNode(File file){
    this.file = file;
  }

  @Override public String accept(WorkloadVisitor visitor) {
    return visitor.visit(this);
  }

  public File getFile() {
    return file;
  }

  @Override public String toString() {
    return String.format("%s", getFile().getName());
  }
}
