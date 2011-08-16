package edu.ucsc.dbtune.inum;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface WorkloadNode <T> {
  T accept(WorkloadVisitor visitor);
}
