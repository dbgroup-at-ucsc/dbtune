package edu.ucsc.dbtune.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public final class Strings
{
    /**
     * utility class.
     */
    private Strings()
    {
    }

    /**
     * performs some rudimentary trimming of some text that is assumed to contain an sql statement
     *
     * Given an input string, let L be the text before the first \n or \r, with any whitespace
     * removed from the beginning or end. If L starts with "--", the empty string is returned.
     * Otherwise, L is returned.
     *
     * Less formally, return the empty string if the first line is empty or only contains a
     * comment. Otherwise, return the first line, trimmed, with no terminating semicolon.
     *
     * @param sbuf string buuilder.
     * @param data data to be parsed.
     * @param separator string separator
     */
    public static void implode(StringBuilder sbuf, Object[] data, String separator)
    {
        for (int i = 0; i < data.length; i++) {
            if (i > 0) { sbuf.append(separator); }
            sbuf.append(data[i]);
        }
    }

    /**
     * check if two strings are the same.
     *
     * @param left string
     * @param right string
     * @return {@code true} if they are the same. {@code false} otherwise.
     */
    public static boolean same(String left, String right)
    {
        return left.equals(right) || left.equalsIgnoreCase(right);
    }

    /**
     * performs some rudimentary trimming of some text that is assumed to contain an sql
     * statement.
     *
     * Given an input string, let L be the text before the first \n or \r, with any whitespace
     * removed from the beginning or end. If L starts with "--", the empty string is returned.
     * Otherwise, L is returned.
     *
     * Less formally, return the empty string if the first line is empty or only contains a
     * comment. Otherwise, return the first line, trimmed, with no terminating semicolon.
     *
     * @param sql sql query to be trimmed.
     * @return a trimmed sql query.
     */
    public static String trimSqlStatement(String sql)
    {
        String firstLine;
        int crPos = sql.indexOf('\r');
        int lfPos = sql.indexOf('\n');
        int firstLineLength =
            (crPos >= 0) ? ((lfPos >= 0) ? Math.min(crPos, lfPos) : crPos) : ((lfPos >= 
                        0) ? lfPos : sql.length());
        while (sql.charAt(firstLineLength - 1) == ';') { --firstLineLength; }
        firstLine = sql.substring(0, firstLineLength);

        if (firstLine.charAt(0) == '-' && firstLine.charAt(1) == '-') { return ""; } else {
            return firstLine;
        }
    }

    /**
     * Treats each element from {@code valArray} as a {@link Double} and returns an array containing
     * them.
     *
     * @param valArray array of values containing a string representation of an integer each.
     * @return an array containing integers in the order they appear in the given array. That is,
     *         {@code valArray[0]} corresponds to the first integer in the array, {@code
     *         valArray[1]} to the second and so on.
     * @throws NumberFormatException if one string does not contain a parsable integer.
     */
    public static int[] toIntegerArray(String[] valArray)
    {
        int[] intArray = new int[valArray.length];

        for (int i = 0; i < valArray.length; i++) { intArray[i] = Integer.parseInt(valArray[i]); }

        return intArray;
    }

    /**
     * Converts from a string to a boolean value. The valid literal values are "Y" or "N", "yes" or 
     * "no", "on" or "off", "true" or "false". The comparison is case insensitive.
     *
     * @param value
     *      a string containing a literal boolean value
     * @return
     *      {@code true} if {@code "y"}, {@code "on"} or {@code "true"}; {@code false} if {@code 
     *      "n"}, {@code "off"} or {@code "false"}.
     */
    public static boolean toBoolean(String value)
    {
        String lv = value.toLowerCase();

        if (lv.equals("y") || lv.equals("true") || lv.equals("on") || lv.equals("yes"))
            return true;
        else if (lv.equals("n") || lv.equals("false") || lv.equals("off") || lv.equals("no"))
            return false;

        throw new RuntimeException("Unknown value " + value);
    }

    /**
     * Treats each element from {@code valArray} as a {@link Boolean} and returns an array
     * containing them. The valid literal values are "Y" and "N".
     *
     * @param valArray array of values containing a string representation of an integer each.
     * @return an array containing integers in the order they appear in the given array. That is,
     *         {@code valArray[0]} corresponds to the first integer in the array, {@code
     *         valArray[1]} to the second and so on.
     * @throws NumberFormatException if one string does not contain a parsable integer.
     */
    public static Boolean[] toBooleanArray(String[] valArray)
    {
        return (Boolean[]) toBooleanList(valArray).toArray();
    }

    /**
     * Treats each element from {@code valArray} as a {@link Boolean} and returns a list containing
     * them. The valid literal values are "Y" and "N".
     *
     * @param valArray array of values containing a string representation of an integer each.
     * @return a list containing integers in the order they appear in the given array. That is,
     *         {@code valArray[0]} corresponds to the first integer in the array, {@code
     *         valArray[1]} to the second and so on.
     * @throws NumberFormatException if one string does not contain a parsable integer.
     */
    public static List<Boolean> toBooleanList(String[] valArray)
    {
        List<Boolean> booleanList = new ArrayList<Boolean>();

        for (String eachVal : valArray) {
            booleanList.add(Boolean.parseBoolean(eachVal));
        }

        return booleanList;
    }

    /**
     * Treats each element from {@code valArray} as an indexed {@link Double} element in the format:
     * <code> 2<sp>1.08 0<sp>102.38 3<sp>.183 1<sp>-90.8564 7<sp>5433.332 ... </code> the {@code
     * separator} string is used to separate the index from the value on each element contained in
     * {@code valArray}. The elements can be in any order as long as the number of ALL of them don't
     * have an index bigger than {@code valArray.length}. For example:
     *
     * @param valArray array of strings where each is of the form {@code idx<separator>double}.
     * @param separator separator that is used to separate the index from the value
     * @throws NumberFormatException if one string does not contain a parsable integer.
     * @throws ArrayIndexOutOfBoundsException if one of the indexes (numbers before the separator)
     * is greater than {@code valArray.length}
     * @return
     *    an array of "double" values.
     */
    public static double[] toDoubleArrayFromIndexed(String[] valArray, String separator)
    {
        String[] splitVal;
        double[] dblArray;
        double dblValue;
        int position;

        dblArray = new double[valArray.length];

        for (int i = 0; i < valArray.length; i++) {
            splitVal = valArray[i].split("=");

            if (splitVal.length != 2) {
                throw new RuntimeException("Getting length " + splitVal.length + " expecting 2");
            }

            position = Integer.valueOf(splitVal[0]);
            dblValue = Double.valueOf(splitVal[1]);

            dblArray[position] = dblValue;
        }

        return dblArray;
    }

    /**
     * Compares two strings representing software versions.
     *
     * @param v1 string representing a version
     * @param v2 string representing a version
     * @return 0 if equal; negative if {@code v1} less than {@code v2}; positive otherwise.
     */
    public static int compareVersion(String v1, String v2)
    {
        String s1 = normalizeVersion(v1, ".", 6);
        String s2 = normalizeVersion(v2, ".", 6);

        return s1.compareTo(s2);
    }

    /**
     * Normalizes a string representing the version of a software in such a way that the {link
     * String#compareTo} method can be applied to two normalized strings.
     *
     * @param version string representing the version of a software, e.g. 7.74.23b or 0.1a
     * @param separator string used to separate revision numbers, usually "-" or ".".
     * @param maxWidth the maximum width of the string
     * @return the normalized version of
     */
    public static String normalizeVersion(String version, String separator, int maxWidth)
    {
        StringBuilder sb = new StringBuilder();
        String[] split = Pattern.compile(separator, Pattern.LITERAL).split(version);

        for (String s : split) { sb.append(String.format("%" + maxWidth + 's', s)); }

        return sb.toString();
    }

    /**
     * <p>Checks if String contains a search String irrespective of case, handling
     * <code>null</code>. Case-insensitivity is defined as by {@link 
     * String#equalsIgnoreCase(String)}.
     *
     * @param str the String to check, may be null
     * @param searchStr the String to find, may be null
     * @return true if the String contains the search String irrespective of case or false if not or
     *         <code>null</code> string input
     */
    public static boolean contains(String str, String searchStr)
    {
        if (str == null || searchStr == null) { return false; }

        int len = searchStr.length();
        int max = str.length() - len;
        for (int i = 0; i <= max; i++) {
            if (str.regionMatches(true, i, searchStr, 0, len)) { return true; }
        }

        return false;
    }

    /**
     * <p>Checks if String contains many search String irrespective of case, handling
     * <code>null</code>. Case-insensitivity is defined as by {@link 
     * String#equalsIgnoreCase(String)}.
     *
     * @param str the String to check, may be null
     * @param searchStrs the many String to find, may be null
     * @return true if the String contains the many search String irrespective of case or false if
     *         not or <code>null</code> string input
     */
    public static boolean containsAny(String str, String... searchStrs)
    {
        boolean result = false;

        for (String each : searchStrs) { result |= contains(str, each); }

        return result;
    }

    /**
     * Returns a string containing the name of an index based on the contents of the index.
     *
     * @param index list of indexes
     * @return a string containing the PG-dependent string representation of the given list, as the
     *         EXPLAIN INDEXES statement expects it
     */
    public static String getName(Index index)
    {
        StringBuilder str = new StringBuilder();
        boolean first = true;

        for (Column col : index) {
            if (first) { first = false; } else { str.append("_"); }

            str.append(col.getName());
        }
        str.append("_index");
        return str.toString();
    }

    /**
     * Returns a string containing the create statement of the given index.
     *
     * @param index
     *      index for which a SQL create statement is created
     * @return
     *      string containing the create statement
     */
    public static String getCreateStatement(Index index)
    {
        StringBuilder sql = new StringBuilder();

        sql.append("CREATE INDEX " + index.getFullyQualifiedName());
        sql.append(" ON " + index.getTable().getFullyQualifiedName() + " (");

        for (Column col : index)
            sql.append(col.getName()).
                append(index.isAscending(col) ? " ASC" : " DESC").
                append(",");

        sql.delete(sql.length() - 1, sql.length());

        sql.append(")");

        return sql.toString();
    }

    /**
     * Concatenate a list of input strings using the given connector to return one output string.
     * Eg: join("+", (("a"), ("bc")) = "a + bc"
     * 
     * @param connector
     *      The input connector to link elements (e.g., "+", "-")
     * @param listElement
     *      The input list of elements to connect     
     * @return
     *      The output string
     */ 
    public static String concatenate(String connector, List<String> listElement)
    {   
        StringBuilder result = new StringBuilder();
        boolean isFirst = true;

        for (String var : listElement) {
            if (!isFirst) {
                result.append(connector); 
            }
            result.append(var);
            isFirst = false;
        }

        return result.toString();
    }
}
