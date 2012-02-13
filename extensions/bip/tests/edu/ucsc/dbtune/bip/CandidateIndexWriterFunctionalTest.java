package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OneColumnCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetCandidateGenerator;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

/**
 * 
 * @author Quoc Trung Tran
 *
 */
public class CandidateIndexWriterFunctionalTest 
{
    private static DatabaseSystem db;
    private static Environment    en;
    
    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp() throws Exception
    {
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
    }
    
    @Test
    public void generateAndWriteIndexesToFile() throws Exception
    {
        String folder = en.getWorkloadsFoldername() + "/tpch-small";
        Workload workload = workload(folder);
        
        // 1. optimal indexes
        /*
        CandidateGenerator candGen =
          //  new OneColumnCandidateGenerator(
                       new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        */
        
        CandidateGenerator candGen = new PowerSetCandidateGenerator
                                         (db.getCatalog(), 2, true);
        
        Set<Index> candidates = candGen.generate(workload);
        System.out.println(" Number of indexes: " + candidates.size());
        
        try {
            writeIndexesToFile(candidates, folder  + "/candidate-powerset");
        } catch (Exception e) {
            throw e;
        }
    }
    
    
    /**
     * Write the set of indexes into the specified file in the format of:
     * {@code id|table_name|col_name|ASC}
     * 
     * @param indexes
     *      A set of indexes to be serialized into binary file
     * @param folder
     *      The folder of the file to be written
     * @param name
     *      The name of the file
     * 
     * @throws IOException
     *      when there is an error in creating files.
     */
    private void writeIndexesToFile(Set<Index> indexes, String name) 
                throws IOException
    {
        StringBuilder sb;
        PrintWriter   out;
          
        out = new PrintWriter(new FileWriter(name), false);
        
        for (Index idx : indexes) {
            sb = new StringBuilder();
            sb.append(idx.getId()).append("|")
              .append(idx.getTable().getName()).append("|");
            
            for (Column c : idx.columns())
                sb.append(c.getName()).append("|")
                        .append(idx.isAscending(c) ? "ASC" : "DESC")
                        .append("|");
            
            sb.delete(sb.length() - 1, sb.length() );
            out.println(sb.toString());
        }
        
        out.close();
    }
}
