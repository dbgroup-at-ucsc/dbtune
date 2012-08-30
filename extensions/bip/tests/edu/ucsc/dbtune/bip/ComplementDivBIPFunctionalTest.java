package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.metadata.Index;


public class ComplementDivBIPFunctionalTest extends DivTestSetting 
{
    private static List<Set<Index>> distinctIndexesDiv;
    private static List<Set<Index>> distinctIndexesUnif;
    
    @Test
    public void main() throws Exception
    {
        /*
        // 1. Read common parameter
        getEnvironmentParameters();
        
        // 2. set parameter for DivBIP()
        setParameters();
        
        if (!(io instanceof InumOptimizer))
            return;
  
        candidates = readCandidateIndexes();
        
        // divergent
        testDivergent();
        
        // uniform
        // 3. Call divergent design
        distinctIndexesUnif = new ArrayList<Set<Index>>();
        
        for (double B1 : listBudgets) {
            
            B = B1;            
            System.out.println(" Space:  " + B + "============\n");
            DivBIPFunctionalTest.testUniformOneSpaceBudget(B);
        
            Set<Index> conf = new HashSet<Index>();
            
            for (int r = 0; r < 1; r++)
                conf.addAll(divConf.indexesAtReplica(r));
            
            distinctIndexesUnif.add(conf);
            
        }
        
        // compute the intersection
        for (int i = 0; i < listBudgets.size(); i++) 
            showDifference(i, distinctIndexesDiv.get(i), distinctIndexesUnif.get(i));
        
        */
    }
    
    private static void testDivergent() throws Exception
    {
        /*
        distinctIndexesDiv = new ArrayList<Set<Index>>();
        
        // 3. Call divergent design
        for (double B1 : listBudgets) {
            
            B = B1;            
            System.out.println(" Space:  " + B + "============\n");
            
            for (int i = 0; i < listNumberReplicas.size(); i++) {           
                
                nReplicas = listNumberReplicas.get(i);
                loadfactor = (int) Math.ceil( (double) nReplicas / 2);
                    
                System.out.println("----------------------------------------");
                System.out.println(" DIV-BIP, # replicas = " + nReplicas
                                    + ", load factor = " + loadfactor
                                    + ", space = " + B);
                
                DivBIPFunctionalTest.testDiv();
                
                System.out.println("----------------------------------------");
            
                Set<Index> conf = new HashSet<Index>();
                
                for (int r = 0; r < nReplicas; r++)
                    conf.addAll(divConf.indexesAtReplica(r));
                
                distinctIndexesDiv.add(conf);
    
            }
        }
        */
    }
  
    
    public static void showDifference(int i, Set<Index> div, Set<Index> unif)
    {
        Set<Integer> divID = new HashSet<Integer>();
        Set<Integer> unifID = new HashSet<Integer>();
        
        for (Index index : div)
            divID.add(index.getId());
        
        for (Index index : unif)
            unifID.add(index.getId());
        
        Set<Integer> intersect = new HashSet<Integer> (divID);
        intersect.retainAll(unifID);
        
        System.out.println(" Order =" + i + " div = " + div.size()
                 + " unif = " + unif.size()
        		 + "number of intersect = " + intersect.size());
    }
}
