package edu.ucsc.dbtune.inum;

/**
 * Thrown if we are trying to execute INUM without starting it first.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
@SuppressWarnings("serial")
public class InumExecutionException extends RuntimeException
{
  public InumExecutionException(String s)
  {
    super(s);
  }
}
