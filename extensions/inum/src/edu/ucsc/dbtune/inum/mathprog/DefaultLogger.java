package edu.ucsc.dbtune.inum.mathprog;

import java.util.logging.Logger;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class DefaultLogger implements LogListener {
  private static final Logger LOGGER = Logger.getLogger("DefaultLogger");
  @Override public void onLogEvent(String target, String message) {
    LOGGER.info(target + " " + message);
  }
}
