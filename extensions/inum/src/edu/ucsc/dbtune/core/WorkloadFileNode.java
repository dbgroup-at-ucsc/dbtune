package edu.ucsc.dbtune.core;

import java.io.File;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class WorkloadFileNode implements PersistedWorkloadNode<String> {
  private final File file;

  public WorkloadFileNode(File file){
    this.file = file;
  }

  @Override public String accept(PersistedWorkloadVisitor visitor) {
    return visitor.visit(this);
  }

  public File getFile(){
    return file;
  }

  @Override public String toString() {
    return String.format("%s", getFile().getName());
  }
}
