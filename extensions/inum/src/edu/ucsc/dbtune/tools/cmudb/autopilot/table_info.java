package edu.ucsc.dbtune.tools.cmudb.autopilot;

import java.util.ArrayList;
import java.util.HashMap;


public class table_info {

    public String table_name;
    public int row_count;
    public HashMap col_info;
    public ArrayList pkeys;
    public int dpages;
};
