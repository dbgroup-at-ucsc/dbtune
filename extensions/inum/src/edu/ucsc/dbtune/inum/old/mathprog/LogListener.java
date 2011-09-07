package edu.ucsc.dbtune.inum.old.mathprog;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface LogListener {
  String COPHY = "COPHY";

  void onLogEvent(String target, String message);
}
