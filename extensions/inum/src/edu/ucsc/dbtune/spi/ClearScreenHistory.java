package edu.ucsc.dbtune.spi;

import java.io.PrintStream;

/**
 * Clear screen, on console, upto some line.
 */
public class ClearScreenHistory
{
    private final PrintStream    out;
    private final Trail          trail          = new Trail();
    private final StringBuilder  trailContent   = new StringBuilder();

    public ClearScreenHistory(PrintStream out)
    {
        this.out = out;
    }

    public void println(String text)
    {
        trail.mark = captureClearingRange(text + "\n");
    }

    private int captureClearingRange(String text)
    {
        int cursor = 0;
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '\n') {
                cursor++;
                trailContent.delete(0, trailContent.length());
            } else {
                trailContent.append(text.charAt(i));
            }
        }

        out.print(text);
        out.flush();
        return cursor;
    }

    public void print(String text)
    {
        trail.mark = captureClearingRange(text);
    }

    public Mark mark()
    {
        return new Mark();
    }

    private static class Trail
    {
       private int mark = 0;
    }

    public class Mark
    {
        private final int    markRow        = trail.mark;
        private final String markRowContent = trailContent.toString();

        private Mark()
        {}

        public void reset()
        {
            /*
             * ANSI escapes
             * http://en.wikipedia.org/wiki/ANSI_escape_code
             *
             *  \u001b[K   clear the rest of the current line
             *  \u001b[nA  move the cursor up n lines
             *  \u001b[nB  move the cursor down n lines
             *  \u001b[nC  move the cursor right n lines
             *  \u001b[nD  move the cursor left n columns
             */

            for (int r = trail.mark; r > markRow; r--) {
                // clear the line, up a line
                System.out.print("\u001b[0G\u001b[K\u001b[1A");
            }

            // clear the line, reprint the line
            out.print("\u001b[0G\u001b[K");
            out.print(markRowContent);
            trailContent.delete(0, trailContent.length());
            trailContent.append(markRowContent);
            trail.mark = markRow;
        }
    }
}
