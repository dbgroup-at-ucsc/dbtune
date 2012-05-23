package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


import org.junit.Test;


import java.util.concurrent.atomic.AtomicInteger;

import edu.ucsc.dbtune.bip.core.InumQueryPlanDesc;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;

import edu.ucsc.dbtune.workload.SQLStatement;

public class SerializedQueryPlanDescFunctionalTest extends DivTestSetting 
{
    /**
     * 
     * @throws Exception
     */
    @Test
    public void testSerialization() throws Exception
    {
        // 1. Read common parameter
        getEnvironmentParameters();
        
        // 2. set parameter for DivBIP()
        setParameters();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        candidates = readCandidateIndexes();
        System.out.println(" space budget: " + B
                        + " number of candidate indexes: " + candidates.size());
        
        // reset the counter in Index class
        int maxID = -1;
        for (Index index : candidates)
            if (index.getId() > maxID)
                maxID = index.getId();
        
        Index.IN_MEMORY_ID = new AtomicInteger(maxID + 1);
        // ----------------------------------------------------
        
        // read QueryPlanDesc
        List<QueryPlanDesc> serializations = readQueryPlanDescsFromFile();
        
        // compare against the one read from 
        for (int i = 0; i < workload.size(); i++)
            testCompability(serializations.get(i), workload.get(i));
                        
    }
    
    /**
     * todo
     * @param serialization
     * @param stmt
     * @throws Exception
     */
    protected static void testCompability(QueryPlanDesc serialization, SQLStatement stmt)
                throws Exception
    {   
        Optimizer io = db.getOptimizer();
        QueryPlanDesc desc = InumQueryPlanDesc.getQueryPlanDescInstance(stmt);
        
        // Populate the INUM space 
        desc.generateQueryPlanDesc((InumOptimizer)io, candidates);
        
        System.out.println(" SERIALIZATION " + serialization + "\n"
                + " GENERATE DIRECTLY: " + desc);
        // implement equal operators in QueryPlanDesc
        assertThat(serialization.equals(desc), is(true));
    }
    
    @SuppressWarnings("unchecked")
    protected static List<QueryPlanDesc> readQueryPlanDescsFromFile() 
            throws SQLException
    {
        ObjectInputStream in;
        List<QueryPlanDesc> queryPlanDescs = new ArrayList<QueryPlanDesc>();
        
        try {
            FileInputStream fileIn = new FileInputStream(folder + "/query-plan-desc.bin");
            in = new ObjectInputStream(fileIn);
            queryPlanDescs = (List<QueryPlanDesc>) in.readObject();
            
            // reassign the statement with the corresponding weight
            for (int i = 0; i < queryPlanDescs.size(); i++)
                queryPlanDescs.get(i).setStatement(workload.get(i));
        
            in.close();
            fileIn.close();
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        
        return queryPlanDescs;
    }
}
