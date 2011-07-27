package edu.ucsc.dbtune.inum.commons;

import edu.ucsc.dbtune.util.Objects;

/**
 * @author ddash
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Pair<L,R> {
  private final L left;
  private final R right;

  private Pair(L left, R right) {
    this.left  = left;
    this.right = right;
  }

  public static <L, R> Pair<L, R> of(L left, R right) {
    return new Pair<L, R>(left, right);
  }

  public static <L, R> Pair<L, R> empty(){
    return new Pair<L, R>(null, null);
  }

  public static <L, R> Pair<L, R> copyOf(Pair<L, R> pair) {
    return new Pair<L, R>(pair.getLeft(), pair.getRight());
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof Pair) && Objects.equals(this, o);
  }

  public L getLeft() {
    return left;
  }

  public R getRight() {
    return right;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getLeft(), getRight());
  }

  @Override
  public String toString() {
    return String.format("Pair: (%s, %s)", getLeft(), getRight());
  }
}
