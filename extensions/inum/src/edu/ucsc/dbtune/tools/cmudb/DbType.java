package edu.ucsc.dbtune.tools.cmudb;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Dec 10, 2007
 * Time: 5:54:13 PM
 * To change this template use File | Settings | File Templates.
 */
public enum DbType {
    DB2,
    MS,
    ORCL,
    PGSQL;

    public String toString() {
       return super.toString().toLowerCase();
    }
}