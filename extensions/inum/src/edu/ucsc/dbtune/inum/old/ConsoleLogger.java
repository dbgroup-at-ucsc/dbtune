package edu.ucsc.dbtune.inum.old;

import edu.ucsc.dbtune.inum.old.mathprog.LogListener;
import edu.ucsc.dbtune.spi.core.Console;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class ConsoleLogger implements LogListener {
  private static final Console CONSOLE = Console.streaming();
  @Override public void onLogEvent(String target, String message) {
    CONSOLE.info(target + ": " + message);
  }
}
