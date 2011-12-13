package edu.ucsc.dbtune.util;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class Strings
{
    /** utility class */
    private Strings(){}

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
     public static void implode(StringBuilder sbuf, Object[] data, String separator)
     {
         for (int i = 0; i < data.length; i++) {
             if (i > 0)
                 sbuf.append(separator);
             sbuf.append(data[i]);
         }
     }

     /**
      * performs some rudimentary trimming of some text that is assumed
      * to contain an sql statement.
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
     public static String trimSqlStatement(String sql)
     {
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
     * Treats each element from {@code valArray} as a {@link Double} and returns an array containing 
     * them.
     *
     * @param valArray
     *     array of values containing a string representation of an integer each.
     * @return
     *     an array containing integers in the order they appear in the given array. That is, {@code 
     *     valArray[0]} corresponds to the first integer in the array, {@code valArray[1]} to the 
     *     second and so on.
     * @throws NumberFormatException
     *     if one string does not contain a parsable integer.
     */
    public static int[] toIntegerArray(String[] valArray)
    {
        int[] intArray = new int[valArray.length];

        for(int i = 0; i < valArray.length; i++)
            intArray[i] = Integer.parseInt(valArray[i]);

        return intArray;
    }

    /**
     * Treats each element from {@code valArray} as a {@link Boolean} and returns an array 
     * containing them. The valid literal values are "Y" and "N".
     *
     * @param valArray
     *     array of values containing a string representation of an integer each.
     * @return
     *     an array containing integers in the order they appear in the given array. That is, {@code 
     *     valArray[0]} corresponds to the first integer in the array, {@code valArray[1]} to the 
     *     second and so on.
     * @throws NumberFormatException
     *     if one string does not contain a parsable integer.
     */
    public static Boolean[] toBooleanArray(String[] valArray)
    {
        return (Boolean[])toBooleanList(valArray).toArray();
    }

    /**
     * Treats each element from {@code valArray} as a {@link Boolean} and returns a list containing 
     * them. The valid literal values are "Y" and "N".
     *
     * @param valArray
     *     array of values containing a string representation of an integer each.
     * @return
     *     a list containing integers in the order they appear in the given array. That is, {@code 
     *     valArray[0]} corresponds to the first integer in the array, {@code valArray[1]} to the 
     *     second and so on.
     * @throws NumberFormatException
     *     if one string does not contain a parsable integer.
     */
    public static List<Boolean> toBooleanList(String[] valArray)
    {
        List<Boolean> booleanList = new ArrayList<Boolean>();

        for(int i = 0; i < valArray.length; i++)
            booleanList.add(Boolean.parseBoolean(valArray[i]));

        return booleanList;
    }

    /**
     * Treats each element from {@code valArray} as an indexed {@link Double} element in the 
     * format:
     * <code>
     * 2<sp>1.08
     * 0<sp>102.38
     * 3<sp>.183
     * 1<sp>-90.8564
     * 7<sp>5433.332
     * ...
     * </code>
     * the {@code separator} string is used to separate the index from the value on each element 
     * contained in {@code valArray}. The elements can be in any order as long as the number of ALL 
     * of them don't have an index bigger than {@code valArray.length}. For example:
     *
     * @param valArray
     *     array of strings where each is of the form {@code idx<separator>double}.
     * @param separator
     *     separator that is used to separate the index from the value
     * @throws NumberFormatException
     *     if one string does not contain a parsable integer.
     * @throws ArrayIndexOutOfBoundsException
     *     if one of the indexes (numbers before the separator) is greater than {@code 
     *     valArray.length}
     */
    public static double[] toDoubleArrayFromIndexed(String[] valArray, String separator)
    {
        String[] splitVal;
        double[] dblArray;
        double   dblValue;

        dblArray = new double[valArray.length];

        for(int i = 0, position; i < valArray.length; i++)
        {
            splitVal = valArray[i].split("=");

            if(splitVal.length != 2)
                throw new RuntimeException("Getting length " + splitVal.length + " expecting 2"); 

            position = Integer.valueOf(splitVal[0]);
            dblValue = Double.valueOf(splitVal[1]);

            dblArray[position] = dblValue;
        }

        return dblArray;
    }

    /**
     * Compares two strings representing software versions.
     *
     * @param v1
     *     string representing a version
     * @param v2
     *     string representing a version
     * @return
     *     0 if equal; negative if {@code v1} less than {@code v2}; positive otherwise.
     */
    public static int compareVersion(String v1, String v2) {
        String s1 = normalizeVersion(v1,".",6);
        String s2 = normalizeVersion(v2,".",6);

        return s1.compareTo(s2);
    }

    /**
     * Normalizes a string representing the version of a software in such a way that the {link 
     * String#compareTo} method can be applied to two normalized strings.
     *
     * @param version
     *     string representing the version of a software, e.g. 7.74.23b or 0.1a
     * @param separator
     *     string used to separate revision numbers, usually "-" or ".".
     * @param maxWidth
     *     the maximum width of the string
     * @return
     *     the normalized version of
     */
    public static String normalizeVersion(String version, String separator, int maxWidth)
    {
        StringBuilder sb    = new StringBuilder();
        String[]      split = Pattern.compile(separator, Pattern.LITERAL).split(version);

        for (String s : split)
            sb.append(String.format("%" + maxWidth + 's', s));

        return sb.toString();
    }

    /**
     * <p>Checks if String contains a search String irrespective of case,
     * handling <code>null</code>. Case-insensitivity is defined as by
     * {@link String#equalsIgnoreCase(String)}.
     *
     * @param str  the String to check, may be null
     * @param searchStr  the String to find, may be null
     * @return true if the String contains the search String irrespective of
     * case or false if not or <code>null</code> string input
     */
    public static boolean contains(String str, String searchStr)
    {
        if (str == null || searchStr == null)
            return false;

        int len = searchStr.length();
        int max = str.length() - len;
        for (int i = 0; i <= max; i++)
            if (str.regionMatches(true, i, searchStr, 0, len))
                return true;

        return false;
    }

    /**
     * <p>Checks if String contains many search String irrespective of case,
     * handling <code>null</code>. Case-insensitivity is defined as by
     * {@link String#equalsIgnoreCase(String)}.
     *
     * @param str  the String to check, may be null
     * @param searchStrs  the many String to find, may be null
     * @return true if the String contains the many search String irrespective of
     * case or false if not or <code>null</code> string input
     */
    public static boolean containsAny(String str, String... searchStrs)
    {
        boolean result = false;

        for(String each : searchStrs)
            result |= contains(str, each);

        return result;
    }

    /**
     * checks if a String is empty ("") or null.
     * @param str
     *      string to be checked.
     * @return true if the string is empty; false otherwise.
     */
    public static boolean isEmpty(String str)
    {
      return (null == str || str.length() == 0);
    }

    /**
     * check if two strings are the same.
     * @param left string
     * @param right string
     * @return {@code true} if they are the same. {@code false} otherwise.
     */
    public static boolean same(String left, String right)
    {
        return left.equals(right) || left.equalsIgnoreCase(right);
    }

    /**
     * Obtain the string representation of an object.
     *
     * @param value object of interest.
     * @param <T> type of the object of interest.
     * @return a string representation of the object of interest.
     */
    public static <T> String str(T value) {
        return value == null ? "" : value.toString();
    }

    /**
     * Returns a string containing the name of an index based on the contents of the index.
     *
     * @param indexes
     *     list of indexes
     * @return
     *     a string containing the PG-dependent string representation of the given list, as the 
     *     EXPLAIN INDEXES statement expects it
     */
    public static String getName(Index index)
    {
        StringBuilder str = new StringBuilder();
        boolean first = true;

        for (Column col : index) {
            if (first)
                first = false;
            else
                str.append("_");

            str.append(col.getName());
        }
        str.append("_index");
        return str.toString();
    }

    public static String join(String delimiter, Object... objects)
    {
        return join(Arrays.asList(objects), delimiter);
    }

    public static String join(Iterable<?> objects, String delimiter)
    {
        Iterator<?> i = objects.iterator();

        if (!i.hasNext())
            return "";

        StringBuilder result = new StringBuilder();
        result.append(i.next());

        while(i.hasNext())
            result.append(delimiter).append(i.next());

        return result.toString();
    }

    public static String[] objectsToStrings(Object[] objects)
    {
        String[] result = new String[objects.length];
        int i = 0;

        for (Object o : objects)
            result[i++] = o.toString();

        return result;
    }

    public static String[] objectsToStrings(Collection<?> objects)
    {
        return objectsToStrings(objects.toArray());
    }

    // does not accept nulls
    public static String[] splits(String text, char separator)
    {
        if(isEmpty(text)) return new String[0];
        final List<String> words = new LinkedList<String>();
        int     idx         = 0;
        int     start       = 0;
        boolean foundMatch  = false;
        while(idx < text.length()){
            if(text.charAt(idx) == separator){
                if(foundMatch){
                    // once the word is added, reset the value of foundMatch
                    foundMatch = !(words.add(text.substring(start, idx)));
                }

                start = ++idx;
                continue;
            }

            foundMatch = true;
            idx++;
        }

        if(foundMatch) words.add(text.substring(start, idx));
        return words.toArray(new String[words.size()]);
    }

    // Taken from http://weblogs.java.net/blog/pat/archive/2004/10/stupid_scanner_1.html
    public static String wholeContentAsSingleLine(File f) throws IOException {
        return new Scanner(f).useDelimiter("\\A").next().replaceAll("\\s+", " ");
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
