package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.inum.commons.Pair;
import java.io.File;
import java.util.Map;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class WorkloadFileNode implements PersistedWorkloadNode<Pair<String, File>> {
  private final File file;

  public WorkloadFileNode(File file){
    this.file = file;
  }

  @Override public Pair<String, File> accept(PersistedWorkloadVisitor visitor) {
    return visitor.visit(this);
  }

  public File getFile(){
    return file;
  }

  @Override public String toString() {
    return String.format("%s", getFile().getName());
  }
}
