package edu.ucsc.dbtune.spi;

/**
 * It emits what commands have spitted out.
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Printer
{
    /**
     * appends '.' on the screen.
     */
    void dot();

    /**
     * prints on the screen verbose messages.
     * @param message
     *      message to be printed on screen.
     */
    void verbose(String message);

    /**
     * prints a message on the screen.
     * @param message
     *      text to be displayed on screen
     */
    void info(String message);

    /**
     * prints a message on the screen, including an exception that was
     * thrown during the execution of a block of code.
     * @param message
     *     text to be displayed on screen.
     * @param exception
     *      exception that was fired.
     */
    void error(String message, Throwable exception);

    /**
     * prints a warning message on the screen.
     * @param message
     *      text to be displayed on screen
     */
    void warn(String message);

    /**
     * prints a message on the screen.
     * @param message
     *    text to be displayed on screen
     */
    void log(String message);

    /**
     * skips a message and then prints a new line on the screen.
     */
    void skip();
}
