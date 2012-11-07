package edu.ucsc.dbtune.bip;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Rt;

public class CompressQueryPlanDesc extends DIVPaper 
{
    protected static List<QueryPlanDesc> descs;
    
    @Test
    public void main() throws Exception
    {  
        // get database instances, candidate indexes
        getEnvironmentParameters();
        setParameters();
        
        readPlans();
        
        for (QueryPlanDesc desc : descs) {
            if (desc.getNumberOfTemplatePlans() >= 2) {
                Rt.p("L30, number of template plans: " + desc.getNumberOfTemplatePlans());
                compressPlans (desc);
            }
        }
    }
    
    
    protected static void compressPlans(QueryPlanDesc desc)
    {   
        List<Double> minCost = new ArrayList<Double>();
        List<Double> maxCost = new ArrayList<Double>();
        double min, max;
        List<Double> accessCosts;
        boolean isRequiredSorted;
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++){
            
            min = desc.getInternalPlanCost(k);
            max = desc.getInternalPlanCost(k);
            
            // for each slot
            for (int i = 0; i < desc.getNumberOfSlots(k); i++){
                accessCosts = new ArrayList<Double>();
                
                isRequiredSorted = true;
                
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    accessCosts.add(desc.getAccessCost(k, i, index));
                    
                    if (index instanceof FullTableScanIndex)
                        isRequiredSorted  = false;
                }
                
                // worst case: NOT INDEX CAN BE USED
                if (isRequiredSorted)
                    accessCosts.add(Math.pow(10, 10));
                
                Collections.sort(accessCosts);
                
                min += accessCosts.get(0);
                max += accessCosts.get(accessCosts.size() - 1);
            }
            
            minCost.add(min);
            maxCost.add(max);
        }
        
        Collections.sort(maxCost);
        int count=0;
        for (int k=0; k < minCost.size(); k++){
            if (minCost.get(k) >= maxCost.get(0)){
                count++;
            }
        }
        Rt.p(" REMOVE " + count + " PLANS");
        
        Rt.p(" min cost " + minCost);
        Rt.p(" max cost " + maxCost);
        
    }
    protected static void readPlans() throws Exception
    {
        String fileName; 
        
        descs = new ArrayList<QueryPlanDesc>();        
        fileName = en.getWorkloadsFoldername() + "/query-plan-desc.bin";
        
        ObjectInputStream in;
        
        try {
            FileInputStream fileIn = new FileInputStream(fileName);
            in = new ObjectInputStream(fileIn);
            descs = (List<QueryPlanDesc>) in.readObject();
        
            in.close();
            fileIn.close();
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
    }
}
