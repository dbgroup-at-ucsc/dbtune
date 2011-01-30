package edu.ucsc.dbtune.spi.core;

/**
 * It emits what a commands has spitted out.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Printer {
    void verbose(String s);
    void info(String s);
    void info(String s, Throwable exception);
    void warn(String s);
}
