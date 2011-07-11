package edu.ucsc.dbtune.core;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface PersistedWorkloadNode<T> {
  T accept(PersistedWorkloadVisitor visitor);
}
