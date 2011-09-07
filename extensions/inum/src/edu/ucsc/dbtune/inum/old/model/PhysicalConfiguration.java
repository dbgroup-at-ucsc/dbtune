package edu.ucsc.dbtune.inum.old.model;

import com.google.common.collect.Multimap;
import edu.ucsc.dbtune.inum.old.CollectionEnumerator;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Dec 26, 2007
 * Time: 8:46:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class PhysicalConfiguration {
    PerTableSet indexes = new PerTableSet();
    List<MatView> matVeiws = new ArrayList();

   
    public PhysicalConfiguration(Index... indexes) {
        for (int i = 0; i < indexes.length; i++) {
            Index index = indexes[i];
            addIndex(index);
        }
    }

    public PhysicalConfiguration(Multimap<String, String> map) {
        if (map != null) {
            for (Iterator<String> iterator = map.keySet().iterator(); iterator.hasNext();) {
                String table = iterator.next();
                addIndex(new Index(table, new LinkedHashSet(map.get(table))));
            }
        }
    }

    public void addIndex(Index idx) {
        indexes.put(idx.getTableName(), idx);
    }

    public Set<String> getIndexedTableNames() {
        return indexes.keySet();
    }

    public void addMatView(MatView view) {
        matVeiws.add(view);
    }

    public Set<Index> getIndexesForTable(String tableName) {
        return indexes.get(tableName);
    }

    public Index getFirstIndexForTable(String tableName) {
        Set set = indexes.get(tableName.toLowerCase());
        if (set == null || set.isEmpty()) {
            return null;
        } else {
            return (Index) set.iterator().next();
        }
    }

    public Iterator<Index> indexes() {
        return indexes.valueIterator();
    }

    public void addConfiguration(PhysicalConfiguration conf) {
        Iterator<Index> iter = conf.indexes();
        while (iter.hasNext()) {
            Index index = iter.next();
            this.addIndex(index);
        }
    }

    public static PhysicalConfiguration loadConfigFromString(String str) {
        Pattern pat = Pattern.compile("(\\w+)=\\[(\\[.*?\\])\\]");
        Pattern idxPat = Pattern.compile("\\[(.*?)\\]");
        Matcher matcher = pat.matcher(str);
        int start = 0;
        PhysicalConfiguration config = new PhysicalConfiguration();
        while (matcher.find(start)) {
            String table = matcher.group(1);
            String indexes = matcher.group(2);

            Matcher indexMatcher = idxPat.matcher(indexes);
            int idxStart = 0;
            while (indexMatcher.find(idxStart)) {
                String fieldStr = indexMatcher.group(1);
                String fields[] = fieldStr.split(", ");

                config.addIndex(new Index(table, new LinkedHashSet(Arrays.asList(fields))));
                idxStart = indexMatcher.end(1);
            }

            start = matcher.end(2);
        }

        return config;
    }


    public static List<Index> loadIndexesFromFile(String file) throws IOException {
        List<Index> configs = new ArrayList<Index>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("[=, ]+");
            String tableName = fields[0];
            LinkedHashSet set = new LinkedHashSet();
            for (int i = 1; i < fields.length; i++) {
                String field = fields[i];
                if(field.startsWith("["))
                    field = field.substring(1);
                if(field.endsWith("]"))
                    field = field.substring(0,field.length()-1);

                set.add(field);
            }

            Index idx = new Index(tableName, set);
            configs.add(idx);
        }

        reader.close();
        return configs;
    }

    public List getMaterializedViews() {
        return matVeiws;
    }

    public boolean isEmpty() {
        return matVeiws.isEmpty() && this.getIndexedTableNames().isEmpty();
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        for (Iterator indexIterator = indexes(); indexIterator.hasNext();) {
            Index index = (Index) indexIterator.next();
            buf.append(index).append("\n");
        }
        for (Iterator<MatView> matViewIterator = matVeiws.iterator(); matViewIterator.hasNext();) {
            MatView matView = matViewIterator.next();
            buf.append(matView.toString()).append("\n");
        }
        return buf.toString();
    }

    public boolean isAtomic() {
        Set tables = indexes.keySet();
        for (Iterator iterator = tables.iterator(); iterator.hasNext();) {
            String tableName = (String) iterator.next();
            if(indexes.get(tableName).size() > 1) {
                return false;
            }
        }

        return true;
    }

    public Iterator atomicConfigurationIterator() {
        ArrayList input = new ArrayList();
        for (Iterator iterator = indexes.keySet().iterator(); iterator.hasNext();) {
            String tableName = (String) iterator.next();

            input.add(indexes.get(tableName));
        }

        CollectionEnumerator enumerator = new CollectionEnumerator(input);
        List state = null;
        List configs = new ArrayList();
        while ((state = enumerator.next()) != null) {
            PhysicalConfiguration config = new PhysicalConfiguration();
            for (int i = 0; i < state.size(); i++) {
                Index idx = (Index) state.get(i);
                config.addIndex(idx);
            }
            configs.add(config);
        }

        return configs.iterator();
    }

    public static class PerTableSet {
        Map<String, Set> map = new HashMap();

        public PerTableSet() {
        }

        public void put(String tableName, Object o) {
            LinkedHashSet set = (LinkedHashSet) map.get(tableName);
            if (set != null) {
                set.add(o);
            } else {
                map.put(tableName, new LinkedHashSet(Arrays.asList(o)));
            }
        }

        public Set keySet() {
            return map.keySet();
        }

        public Set get(String tableName) {
            return map.get(tableName);
        }

        public Iterator valueIterator() {
            return new Iterator() {
                Iterator entries = map.entrySet().iterator();
                Iterator values = null;
                Map.Entry entry;

                public void remove() {
                    throw new UnsupportedOperationException();
                }

                public boolean hasNext() {
                    return entries.hasNext() || (values != null && values.hasNext());
                }

                public Object next() {
                    while (true) {
                        if (entry == null) {
                            entry = (Map.Entry) entries.next();
                            values = ((Set) entry.getValue()).iterator();
                        }
                        if (values.hasNext()) {
                            Object value = values.next();
                            return value;
                        } else {
                            entry = null;
                        }
                    }
                }
            };
        }
    }

    public static Index loadIndexFromString(String str) {
//        Pattern idxPat = Pattern.compile("\\[\\[(.*)\\]\\]");
        Pattern idxPat = Pattern.compile("\\[(.*)\\]");
        Matcher matcher = idxPat.matcher(str);

        while (matcher.find()) {
            String fieldStr = matcher.group(1);
            String fields[] = fieldStr.split(", ");
            System.out.println("fieldStr = " + fieldStr);

            return new Index(ColumnsGatherer.getTableName(fields[0]), new LinkedHashSet(Arrays.asList(fields)));
        }

        return null;
    }
}