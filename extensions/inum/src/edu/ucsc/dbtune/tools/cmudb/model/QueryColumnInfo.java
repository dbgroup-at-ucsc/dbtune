package edu.ucsc.dbtune.tools.cmudb.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: dash
 * Date: Apr 15, 2009
 * Time: 10:51:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueryColumnInfo {
    public List tables = new ArrayList();
    public Set joinColumns = new HashSet();
    public Set conditionColumns = new HashSet();
    public Set orderColumns = new HashSet();
    public Set allColumns = new HashSet();

    @Override
    public String toString() {
        return "QueryColumnInfo{" +
                "\ttables=" + tables +
                ",\n\tjoinColumns=" + joinColumns +
                ",\n\tconditionColumns=" + conditionColumns +
                ",\n\torderColumns=" + orderColumns +
                ",\n\tallColumns=" + allColumns +
                '}';
    }
}