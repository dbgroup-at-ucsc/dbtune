package edu.ucsc.dbtune.tools.cmudb.model;

/**
 * Created by IntelliJ IDEA.
 * User: dash
 * Date: Apr 17, 2009
 * Time: 5:52:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class ColumnInfo {
    public String tableName;
    public String aliasName;
    public String columnName;

    public ColumnInfo(String tableName, String aliasName, String columnName) {
        this.tableName = tableName;
        this.aliasName = aliasName;
        this.columnName = columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnInfo that = (ColumnInfo) o;

        if (aliasName != null ? !aliasName.equals(that.aliasName) : that.aliasName != null) return false;
        if (columnName != null ? !columnName.equals(that.columnName) : that.columnName != null) return false;
        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (aliasName != null ? aliasName.hashCode() : 0);
        result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return columnName;
    }
}