package edu.ucsc.satuning.util;

import edu.ucsc.satuning.db.DBIndex;

import java.io.IOException;
import java.io.ObjectInputStream;

public class DBUtilities {
	@SuppressWarnings("unchecked")
	public static <I extends DBIndex<I>> I readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		return (I) in.readObject();
	}
	
	/**
	 * performs some rudimentary trimming of some text that is assumed
	 * to contain an sql statement
	 * 
	 * Given an input string, let L be the text before the first \n or \r,
	 * with any whitespace removed from the beginning or end.
	 * If L starts with "--", the empty string is returned. 
	 * Otherwise, L is returned.
	 * 
	 * Less formally, return the empty string if the first line is empty or
	 * only contains a comment. Otherwise, return the first line, trimmed,
	 * with no terminating semicolon.
     * @param sql
     *      sql query to be trimmed.
     * @return
     *      a trimmed sql query.
     */
	public static String trimSqlStatement(String sql) {
		String firstLine;
		{
			int crPos = sql.indexOf('\r');
			int lfPos = sql.indexOf('\n');
			int firstLineLength = 
			    (crPos >= 0)
			    ? ((lfPos >= 0) ? Math.min(crPos, lfPos) : crPos)
			    : ((lfPos >= 0) ? lfPos : sql.length());
			while (sql.charAt(firstLineLength-1) == ';')
				--firstLineLength;
			firstLine = sql.substring(0, firstLineLength);
		}
		
		if (firstLine.charAt(0) == '-' && firstLine.charAt(1) == '-')
			return "";
		else
			return firstLine;
	}

    /**
     * performs some rudimentary trimming of some text that is assumed
     * to contain an sql statement
     *
     * Given an input string, let L be the text before the first \n or \r,
     * with any whitespace removed from the beginning or end.
     * If L starts with "--", the empty string is returned.
     * Otherwise, L is returned.
     *
     * Less formally, return the empty string if the first line is empty or
     * only contains a comment. Otherwise, return the first line, trimmed,
     * with no terminating semicolon.
     * @param sbuf
     *       string buuilder.
     * @param data
     *       data to be parsed.
     * @param separator
     *       string separator
     */
     public static void implode(StringBuilder sbuf, Object[] data, String separator) {
         for (int i = 0; i < data.length; i++) {
             if (i > 0)
                 sbuf.append(separator);
             sbuf.append(data[i]);
         }
     }

    public static void formatIdentifier(String str, StringBuilder sbuf) {
         int strlen = str.length();
         boolean simple;

         if (!Character.isLetter(str.charAt(0)))
             simple = false;
         else {
             simple = true;
             for (int i = 0; i < strlen; i++) {
                 char c = str.charAt(i);
                 if (c != '_' && !Character.isLetterOrDigit(c)) {
                     simple = false;
                     break;
                 }
             }
         }

         if (simple)
             sbuf.append(str);
         else {
             sbuf.append('"');
             for (int i = 0; i < strlen; i++) {
                 char c = str.charAt(i);
                 sbuf.append(c);
                 if (c == '"')
                     sbuf.append('"');
             }
             sbuf.append('"');
         }
     }

    public static void formatStringLiteral(String str, StringBuilder sbuf) {
         int strlen = str.length();

         sbuf.append('\'');
         for (int i = 0; i < strlen; i++) {
             char c = str.charAt(i);
             if (c == '\'')
                 sbuf.append("''");
             else
                 sbuf.append(c);
         }
         sbuf.append('\'');
     }
}
