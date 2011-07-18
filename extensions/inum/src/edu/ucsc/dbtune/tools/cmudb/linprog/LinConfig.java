package edu.ucsc.dbtune.tools.cmudb.linprog;

import edu.ucsc.dbtune.tools.cmudb.model.PhysicalConfiguration;
import java.util.ArrayList;
import java.util.Arrays;


public class LinConfig { 

    int ID; //config ID
    ArrayList indexes; //IDs of indexes in that config. 
    PhysicalConfiguration config; //Link to the actual configuration object. ...
    int queryID; //a config variable in the linar program corresponds to a specific query unfortunately ... 
    float cost; //the benefit if a given query uses the particular index ... 
    float max_benefit; //the maximum benefit attainable for the query using this config.

    public String toString() { 
	return new String(indexes + " Q:" + queryID);
	
    }

    public String signature() { 
	

	Integer[] IDArray = (Integer[]) indexes.toArray(new Integer[indexes.size()]);
	Arrays.sort(IDArray);
	String result = new String(); 
	for (int i=0; i<indexes.size(); i++) { 
	    result+=IDArray[i].toString() + " "; 
	}
	    
	return result;

    }


};
