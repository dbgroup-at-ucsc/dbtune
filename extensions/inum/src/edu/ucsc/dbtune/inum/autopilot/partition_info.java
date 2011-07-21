package edu.ucsc.dbtune.inum.autopilot;

import java.util.ArrayList;

public class partition_info {

    public String table_name;
    public String base_name;
    public ArrayList columns;

    public ArrayList clustered_columns;
    public boolean cluster_pk;


    public partition_info(String table_name, String base_name, ArrayList columns) {
        this.table_name = new String(table_name);
        this.base_name = new String(base_name);
        this.columns = new ArrayList(columns);

        //Default values
        clustered_columns = new ArrayList(); //empty
        cluster_pk = true; //Always cluster on the primary key
    }

    public partition_info(partition_info p) {
        table_name = new String(p.table_name);
        base_name = new String(p.base_name);
        columns = new ArrayList();
        for (int i = 0; i < p.columns.size(); i++) {
            columns.add(p.columns.get(i));
        }

        clustered_columns = new ArrayList(p.clustered_columns);
        cluster_pk = p.cluster_pk;


    }


    public partition_info() {

        //Do nothing
    }


    public String toString() {


        String result = table_name.toUpperCase() + "\n";
        for (int i = 0; i < columns.size(); i++)
            result += ((String) columns.get(i)).toUpperCase() + "\n";
        result += " " + "\n";
        return result;

    }

};

