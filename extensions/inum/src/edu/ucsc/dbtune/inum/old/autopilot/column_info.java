package edu.ucsc.dbtune.inum.old.autopilot;
//Required database table information

public class column_info {
    public String col_name;
    public String col_type;
    public int col_size;
    public int sysindexes_id;
    public int sysindexes_indid;
    public boolean is_pk;
    public int precision;
    public int scale;
    public boolean nullable;

    //  public binary_array statblob ??


    public column_info() {
        sysindexes_id = 0;
        sysindexes_indid = 0;
        col_size = 0;
        is_pk = false;
        precision = 0;
        scale = 0;


    }

    public String toString() {
        return "\n " + col_name + " " + col_type + " " + col_size + " " + sysindexes_id + " " + sysindexes_indid + " " + is_pk;


    }


};
