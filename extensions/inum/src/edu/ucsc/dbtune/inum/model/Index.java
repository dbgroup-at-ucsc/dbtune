package edu.ucsc.dbtune.inum.model;

import edu.ucsc.dbtune.inum.commons.Utils;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Dec 10, 2007
 * Time: 8:01:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class Index {
    private LinkedHashSet columns;
    private String tableName;
    private String implementedName;
    private int hashCode;
    private String key;
    private int theSize;
    public Index(String tableName, LinkedHashSet columns) {
        this.tableName = tableName;
        this.columns = sortColumnSet(columns);
        hashCode = 31 * columns.hashCode() + tableName.hashCode(); //nu stiu exact ce e asta
        theSize = 0;
    }

    public Index(String tableName, String... columns) {
        this(tableName, Utils.makeColumns(columns));
    }

    //get the index size
    public int getTheSize()
    {
    return theSize;
    }
    //set the size
    public void setTheSize(int siz)
    {
    theSize = siz;
    }

    public static LinkedHashSet sortColumnSet(LinkedHashSet configColSet) {
        if (configColSet.size() <= 1) {
            return configColSet;
        } else {
            String[] cols = (String[]) configColSet.toArray(new String[configColSet.size()]);
            Arrays.sort(cols, 1, cols.length);
            return new LinkedHashSet(Arrays.asList(cols));
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
    
    public String getTableName() {
        return tableName;
    }

    public LinkedHashSet getColumns() {
        return columns;
    }

    public int hashCode() {
        return hashCode;
    }

    public boolean equals(Object o) {
        if(o == null || !(o instanceof Index)) return false;
        final Index index = (Index) o;

        return tableName.equals(index.tableName) && index.getKey().equals(getKey());
    }

    public String getKey() {
        if(key == null) {
            if(columns.isEmpty()) {
                key = tableName + "EMPTY_INDEX";
            } else {
                key = columns.toString();
            }
        }

        return key;
    }

    public String getFirstColumn() {
        if(columns.isEmpty())
            return null;
        else
            return (String) columns.iterator().next();
    }

    public String toString() {
        return getKey() + (isImplemented() ? "-(" + implementedName + ")" : "");
    }

    public String getColumnByIndex(int column) {
        for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
            String columnName = (String) iterator.next();
            if(column == 0)
                return columnName;

            column --;
        }

        throw new ArrayIndexOutOfBoundsException(column);
    }
}