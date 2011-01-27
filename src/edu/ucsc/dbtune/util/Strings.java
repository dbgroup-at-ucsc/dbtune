/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
package edu.ucsc.dbtune.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Strings {
    private Strings(){}

    public static String readStream(IterableFileReader reader) throws IOException {
        StringBuilder result = new StringBuilder();
        final IterableFileReader linesInFile = Checks.checkNotNull(reader);
        for(String each : linesInFile){
            result.append(each);
            result.append('\n');
        }
        return result.toString();
    }

    public static String readFile(File f) throws IOException {
        return readStream(new IterableFileReader(f));
    }

    public static List<String> readFileLines(File f) throws IOException {
        List<String> list = new ArrayList<String>();
        final IterableFileReader linesInFile = new IterableFileReader(f);
        for(String each : linesInFile){
            list.add(each);
        }
        return list;
    }


    public static String join(String delimiter, Object... objects) {
        return join(Arrays.asList(objects), delimiter);
    }

    public static String join(Iterable<?> objects, String delimiter) {
        Iterator<?> i = objects.iterator();
        if (!i.hasNext()) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        result.append(i.next());
        while(i.hasNext()) {
            result.append(delimiter).append(i.next());
        }
        return result.toString();
    }

    public static String[] objectsToStrings(Object[] objects) {
        String[] result = new String[objects.length];
        int i = 0;
        for (Object o : objects) {
            result[i++] = o.toString();
        }
        return result;
    }

    public static String[] objectsToStrings(Collection<?> objects) {
        return objectsToStrings(objects.toArray());
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
    public static boolean contains(String str, String searchStr) {
        if (str == null || searchStr == null) {
            return false;
        }
        int len = searchStr.length();
        int max = str.length() - len;
        for (int i = 0; i <= max; i++) {
            if (str.regionMatches(true, i, searchStr, 0, len)) {
                return true;
            }
        }
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
    public static boolean containsAny(String str, String... searchStrs){
        boolean result = false;
        for(String each : searchStrs){
            result |= contains(str, each);
        }
        return result;
    }

    /**
     * Obtain the string representation of an object.
     * @param value
     *      object of interest.
     * @param <T>
     *      type of the object of interest.
     * @return a string representation of the object of interest.
     */
    public static <T> String str(T value){
        return value.toString();
    }

    // does not accept nulls
    public static String[] splits(String text, char separator){
        final String string     = Checks.checkNotNull(text);
        final char   separ      = Checks.checkNotNull(separator);
        if(isEmpty(string)) return new String[0];
        final List<String> words = Instances.newLinkedList();
        int     idx         = 0;
        int     start       = 0;
        boolean foundMatch  = false;
        while(idx < string.length()){
            if(string.charAt(idx) == separ){
              if(foundMatch){
                  // once the word is added, reset the value of foundMatch
                  foundMatch = !(words.add(string.substring(start, idx)));
              }

              start = ++idx;
              continue;
            }

            foundMatch = true;
            idx++;
        }

        if(foundMatch) words.add(string.substring(start, idx));
        return words.toArray(new String[words.size()]);
    }

    /**
     * checks if a String is empty ("") or null.
     * @param str
     *      string to be checked.
     * @return true if the string is empty; false otherwise.
     */
    public static boolean isEmpty(String str) {
      return (null == str || str.length() == 0);
    }

    /**
     * check if two strings are the same.
     * @param left string
     * @param right string
     * @return {@code true} if they are the same. {@code false} otherwise.
     */
    public static boolean same(String left, String right){
        return left.equals(right) || left.equalsIgnoreCase(right);
    }


    /**
     * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
     * @see <p>http://www.gotobject.com</p>
     */
    public static class IterableFileReader implements Iterable<String> {
        private BufferedReader reader;
        public IterableFileReader(InputStream is, String encoding) {
            reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(is, encoding));
            } catch (UnsupportedEncodingException e) {
                unableToOpenFile(e);
            }
        }

        public IterableFileReader(File file) throws IOException {
            this(new FileInputStream(file), "UTF-8");
        }


        private static void unableToOpenFile(Throwable cause){
            throw new RuntimeException("unable to open the given file", cause);
        }

        public Iterator<String> iterator() {
            return new Iterator<String>(){
                private String line;
                public boolean hasNext() {
                    try {
                        line = reader.readLine();
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                    return line != null;
                }

                public String next() {
                    return line;
                }

                public void remove() {
                    throw new UnsupportedOperationException("remove operation is not supported");
                }
            };
        }
    }
}
