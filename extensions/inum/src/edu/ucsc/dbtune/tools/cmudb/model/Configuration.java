package edu.ucsc.dbtune.tools.cmudb.model;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Configuration implements Cloneable, Serializable {

    private static final long serialVersionUID = 6938521568769439982L;
    public LinkedHashMap map;
    public transient Map implementedIndexes;
    public String name;
    public HashMap indexMap;

    private transient static final Map keyMap = new IdentityHashMap();
    private transient ConfigurationKey _key;

    public static int _totalAddColumnSets = 0, _newColumnSets = 0, _totalConfigKey = 0, _newConfigKey = 0;

    public Configuration() {
        map = new LinkedHashMap();
    }

    public Configuration(List<Configuration> configs) {
        this();
        for (int i = 0; i < configs.size(); i++) {
            Configuration configuration = configs.get(i);
            this.addConfig(configuration);
        }
    }

    /*
    public void buildIndexMap() {

    for (Iterator mi = indexes.entrySet().iterator(); mi.hasNext();) {
        Map.Entry E = (Map.Entry) mi.next();
        for (Iterator ci = (LinkedHashSet)E.getValue().iterator();ci.hasNext();) {
        LinkedHashSet columnSet = (LinkedHashSet) ci.next();

        }
    }
    }
    */


    public void addColumn(String table_name, String column) {
        LinkedHashSet tableSet = (LinkedHashSet) map.get(table_name);
        if (tableSet == null) {
            tableSet = new LinkedHashSet();
            map.put(table_name, tableSet);
        }
        LinkedHashSet columnSet = new LinkedHashSet();
        columnSet.add(column);
        tableSet.add(columnSet);
    }

    public void addColumnSet(String table_name, LinkedHashSet columnSet) {
        _totalAddColumnSets++;
        LinkedHashSet tableSet = (LinkedHashSet) map.get(table_name);
        if (tableSet == null) {
            tableSet = new LinkedHashSet();
            map.put(table_name, tableSet);
        }

        if (columnSet instanceof ColumnSet) {
            tableSet.add(columnSet);
        } else {
            _newColumnSets++;
            tableSet.add(new ColumnSet(columnSet));
        }
    }

    public void addConfig(Configuration config) {
        for (Map.Entry entry : (Set<Map.Entry>) config.map.entrySet()) {
            LinkedHashSet set = (LinkedHashSet) map.get(entry.getKey());
            if (set == null) {
                map.put(entry.getKey(), new LinkedHashSet((LinkedHashSet) entry.getValue()));
            } else {
                set.addAll((Collection) entry.getValue());
            }
        }
    }


    public boolean containsConfig(Configuration C) {
        for (Map.Entry<String, LinkedHashSet> entry : (Set<Map.Entry<String, LinkedHashSet>>) C.map.entrySet()) {
            String tableName = entry.getKey();

            LinkedHashSet theCTableSet = (LinkedHashSet) entry.getValue();
            LinkedHashSet theTableSet = (LinkedHashSet) map.get(tableName);
            if (theTableSet == null)
                return false;

            for (LinkedHashSet<String> theCColumnSet : (Set<LinkedHashSet<String>>) theCTableSet) {
                boolean found = false;
                for (LinkedHashSet<String> theColumnSet : (Set<LinkedHashSet<String>>) theTableSet) {
                    if (theCColumnSet.toString().compareTo(theColumnSet.toString()) == 0) {
                        found = true;
                        break;
                    }
                }
                if (found == false)
                    return false;
            }
        }
        return true;
    }

    public void configTraverser(IndexCallback callback) {
        for (Map.Entry<String, LinkedHashSet> entry : (Set<Map.Entry<String, LinkedHashSet>>) map.entrySet()) {
            String tableName = entry.getKey();
            for (LinkedHashSet<String> columns : (Set<LinkedHashSet<String>>) entry.getValue()) {
                callback.onIndex(tableName, columns);
            }
        }
    }


    //same as below, useful when there is only a single index
    public LinkedHashSet getSingleTableSet() {
        LinkedHashSet[] tableSets = getTableSets();
        return tableSets[0];
    }


    public LinkedHashSet[] getTableSets() {
        return (LinkedHashSet[]) (map.values().toArray(new LinkedHashSet[map.values().size()]));
    }

    public LinkedHashSet getSingleTableSet(String table_name) {
        LinkedHashSet tableSet = (LinkedHashSet) map.get(table_name);
        if (tableSet == null)
            return null;

        if (tableSet.size() == 1) {
            return (LinkedHashSet) tableSet.iterator().next();
        }

        LinkedHashSet result = new LinkedHashSet();
        for (Iterator i = tableSet.iterator(); i.hasNext();) {
            LinkedHashSet columnSet = (LinkedHashSet) i.next();
            result.addAll(columnSet);
        }

        return result;
    }

    public LinkedHashSet getTableSet(String table_name) {
        return (LinkedHashSet) map.get(table_name);
    }

    public Set<String> getTableNames() {
        return map.keySet();
    }

    public String getSingleTableName() {
        return (String) map.keySet().iterator().next();

    }


    public String toString() {
        return (name != null ? name + " - " : "") + map.toString();
    }

    public void setColumnSet(String tableName, LinkedHashSet set) {
        map.put(tableName, set);
    }

    public Configuration copy() {
        Configuration config = new Configuration();
        //config.indexes = (LinkedHashMap) this.indexes.clone();
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            String tableName = (String) entry.getKey();
            LinkedHashSet setOfSets = (LinkedHashSet) entry.getValue();

            for (Iterator iter1 = setOfSets.iterator(); iter1.hasNext();) {
                LinkedHashSet set = (LinkedHashSet) iter1.next();

                config.addColumnSet(tableName, set);
            }
        }
        return config;
    }

    public LinkedHashSet getAllColumns() {
        LinkedHashSet columnSetSets[] = getTableSets();
        LinkedHashSet returnSet = new LinkedHashSet(columnSetSets.length);
        for (int i = 0; i < columnSetSets.length; i++) {
            LinkedHashSet set = columnSetSets[i];
            for (Iterator iterator = set.iterator(); iterator.hasNext();) {
                LinkedHashSet hashSet = (LinkedHashSet) iterator.next();
                returnSet.addAll(hashSet);
            }
        }

        return returnSet;
    }

    public static String getIndexKey(LinkedHashSet configColSet) {
        return configColSet.toString();
    }

    public String getIndexName(String tableString) {
        for (Iterator iter = implementedIndexes.keySet().iterator(); iter.hasNext();) {
            String s = (String) iter.next();
            String tableName = s.substring(0, s.indexOf('.'));
            if (tableName.compareTo(tableString) == 0) {
                return s;
            }
        }

        return null;
    }

    public Configuration getIndexDefinition(String indexName) {
        if (implementedIndexes == null) return null;
        for (Iterator iter = implementedIndexes.keySet().iterator(); iter.hasNext();) {
            String s = (String) iter.next();
            if (s.endsWith(indexName)) {
                Configuration config = new Configuration();
                String tableName = s.substring(0, s.indexOf('.'));
                config.addColumnSet(tableName, (LinkedHashSet) implementedIndexes.get(s));
                return config;
            }
        }

        return null;
    }

    public boolean equals(Configuration config) {
        if (!config.map.keySet().equals(map.keySet())) {
            System.out.println("Config Mismatch: The configurations don't have the same tables");
            System.out.println("Config Mismatch: " + map.keySet() + " -- " + config.map.keySet());
            return false;
        }

        for (Iterator iterator = config.map.keySet().iterator(); iterator.hasNext();) {
            String tableName = (String) iterator.next();
            LinkedHashSet<LinkedHashSet> nsets = (LinkedHashSet) config.map.get(tableName);
            List nlist = new ArrayList();
            for (LinkedHashSet set : nsets) {
                nlist.add(set.toString());
            }
            Collections.sort(nlist);

            LinkedHashSet<LinkedHashSet> osets = (LinkedHashSet) map.get(tableName);
            List olist = new ArrayList();
            for (LinkedHashSet set : osets) {
                olist.add(set.toString());
            }
            Collections.sort(olist);

            if (!nlist.toString().equals(olist.toString())) {
                System.out.println("Config Mismatch: different indexes for " + tableName);
                System.out.println("Config Mismatch: config " + nlist);
                System.out.println("Config Mismatch: this " + olist);
                return false;
            }
        }

        return true;
    }

    public String getUniqueKey() {
        return map.toString();
    }

    public static List<Configuration> loadFromFile(String filename) throws IOException {
        List<Configuration> configs = new ArrayList<Configuration>();
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("[=,]");
            String tableName = fields[0];
            LinkedHashSet set = new LinkedHashSet();
            for (int i = 1; i < fields.length; i++) {
                String field = fields[i];
                set.add(field);
            }

            Configuration config = new Configuration();
            config.addColumnSet(tableName, Index.sortColumnSet(set));
            configs.add(config);
        }

        reader.close();
        return configs;
    }

    public static Configuration loadConfigFromString(String str) {
        Pattern pat = Pattern.compile("(\\w+)=\\[(\\[.*?\\])\\]");
        Pattern idxPat = Pattern.compile("\\[(.*)\\]");
        Matcher matcher = pat.matcher(str);
        int start = 0;
        Configuration config = new Configuration();
        while (matcher.find(start)) {
            String table = matcher.group(1);
            String indexes = matcher.group(2);

            Matcher indexMatcher = idxPat.matcher(indexes);
            int idxStart = 0;
            while (indexMatcher.find(idxStart)) {
                String fieldStr = indexMatcher.group(1);
                String fields[] = fieldStr.split(", ");
                config.addColumnSet(table, new LinkedHashSet(Arrays.asList(fields)));
                idxStart = indexMatcher.regionEnd();
            }

            start = matcher.regionEnd();
        }

        return config;
    }

    public static void saveConfigsToFile(String filename, List<Configuration> configs) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(filename));
        for (int i = 0; i < configs.size(); i++) {
            Configuration config = configs.get(i);

            writeConfig(config, writer);
        }

        writer.close();
    }

    public static void writeConfig(Configuration config, PrintWriter writer) {
        List keys = new ArrayList(config.map.keySet());
        Collections.sort(keys);

        for (Iterator iter = keys.iterator(); iter.hasNext();) {
            String tableName = (String) iter.next();
            LinkedHashSet sets = (LinkedHashSet) config.map.get(tableName);

            for (LinkedHashSet columns : (Set<LinkedHashSet>) sets) {
                StringBuffer buffer = new StringBuffer();
                buffer.append(tableName + "=");
                boolean start = true;
                for (String column : (Set<String>) columns) {
                    if (!start) {
                        buffer.append(",");
                    } else {
                        start = false;

                    }
                    buffer.append(column);
                }

                writer.println(buffer);
            }
        }
    }

    public ConfigurationKey getConfigKey() {
        _totalConfigKey++;
        if (_key == null) {
            final List<Integer> ids = new ArrayList();
            this.configTraverser(new IndexCallback() {
                public void onIndex(String tableName, LinkedHashSet<String> columnNames) {
                    ids.add(System.identityHashCode(columnNames));
                }
            });

            int[] idxIds = new int[ids.size()];
            for (int i = 0; i < ids.size(); i++) {
                Integer j = ids.get(i);
                idxIds[i] = j;
            }
            _newConfigKey++;
            _key = new ConfigurationKey(idxIds);
        }

        return _key;
    }

    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.addColumnSet("dash", new LinkedHashSet(Arrays.asList(new String[]{"l_a", "l_b", "l_c"})));
        LinkedHashSet set = config.getSingleTableSet("dash");
        Configuration config1 = new Configuration();
        config1.addColumnSet("dash", new LinkedHashSet(Arrays.asList(new String[]{"l_a", "l_b", "l_c"})));

        System.out.println("config1.getConfigKey().hashCode() = " + config1.getConfigKey().hashCode());
        System.out.println("config.getConfigKey().hashCode() = " + config.getConfigKey().hashCode());
        System.out.println("equals = " + (config.getConfigKey().equals(config1.getConfigKey())));
    }
}
