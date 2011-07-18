package edu.ucsc.dbtune.tools.cmudb.model;

import Zql.ZFromItem;
import Zql.ZQuery;
import edu.ucsc.dbtune.tools.cmudb.commons.ZqlUtils;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Vector;



/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Dec 10, 2007
 * Time: 8:01:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class MatView {
    private ZQuery query;
    private String implementedName;
    private String key;
    private LinkedHashSet columns;

    public MatView(ZQuery query) {
        this.query = query;
        if(query.getSelect().isEmpty()) {
            throw new IllegalArgumentException();
        }
        LinkedHashSet cols = new LinkedHashSet(ZqlUtils.getSelectColumns(query));
        columns = new LinkedHashSet(cols.size());
        for (Iterator iterator = cols.iterator(); iterator.hasNext();) {
            String column = (String) iterator.next();
            columns.add(column.toUpperCase());
        }
    }

    public String getImplementedName() {
        return implementedName;
    }

    public void setImplementedName(String implementedName) {
        this.implementedName = implementedName;
    }

    public boolean isImplemented() {
        return implementedName != null;
    }

    public LinkedHashSet<String> getColumns() {
        return columns;
    }

    public int hashCode() {
        return query.hashCode();
    }

    public boolean equals(Object o) {
        if(o == null) return false;
        final MatView view = (MatView) o;

        return query.toString().equals(view.query.toString());
    }

    public String getKey() {
        if (key == null) {
            key = query.toString();
        }
        return key;        
    }

    public String getFirstColumn() {
        return (String) columns.iterator().next();
    }

    public ZQuery getQuery() {
        return query;
    }

    public String toString() {
        return getKey();
    }

    public Set getTables() {
        Vector v = query.getFrom();
        Set set = new HashSet();
        for (int i = 0; i < v.size(); i++) {
            ZFromItem item = (ZFromItem) v.elementAt(i);
            set.add(item.getTable().toLowerCase());
        }

        return set;
    }
}