package edu.ucsc.dbtune.tools.cmudb.model;

import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: Debabrata Dash
 * Date: Jun 8, 2006
 * Time: 5:51:18 PM
 * The file is a copyright of CMU, all rights reserved.
 */
public class ConfigurationKey {
    int [] indexKeys = null;
    int hashCode = 0;

    public ConfigurationKey(int[] indexKeys) {
        Arrays.sort(indexKeys);
        this.indexKeys = indexKeys;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ConfigurationKey that = (ConfigurationKey) o;

        if (!Arrays.equals(indexKeys, that.indexKeys)) return false;

        return true;
    }

    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(indexKeys);
        }

        return hashCode;
    }
}