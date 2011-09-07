package edu.ucsc.dbtune.inum.old.model;

import java.util.LinkedHashSet;

/**
 * Created by IntelliJ IDEA.
 * User: Debabrata Dash
 * Date: May 24, 2006
 * Time: 12:02:01 PM
 * The file is a copyright of CMU, all rights reserved.
 */
public interface IndexCallback {
    public void onIndex(String tableName, LinkedHashSet<String> columnNames);
}
