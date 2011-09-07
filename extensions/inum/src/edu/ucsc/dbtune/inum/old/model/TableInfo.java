package edu.ucsc.dbtune.inum.old.model;

/**
 * Created by IntelliJ IDEA.
 * User: dash
 * Date: Apr 16, 2009
 * Time: 5:27:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableInfo {
    private String tableName;
    private String aliasName;

    public TableInfo(String tableName, String aliasName) {
        this.tableName = tableName;
        this.aliasName = aliasName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "tableName='" + tableName + '\'' +
                ", aliasName='" + aliasName + '\'' +
                '}';
    }
}
