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
import java.io.Reader;
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
        final IterableFileReader linesInFile = PreConditions.checkNotNull(reader);
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
     * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
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
                    throw new UnsupportedOperationException("remove is not supported. sorry dude!");
                }
            };
        }
    }
}
