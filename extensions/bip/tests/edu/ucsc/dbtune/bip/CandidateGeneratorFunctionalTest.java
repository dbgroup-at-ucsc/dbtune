package edu.ucsc.dbtune.bip;


import static edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER;
import static edu.ucsc.dbtune.util.EnvironmentProperties.POWERSET;

import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;



import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;


import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


import java.io.IOException;


import java.sql.SQLException;
import java.util.ArrayList;


import java.util.HashSet;
import java.util.List;


import java.util.Set;


import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;


/**
 * This class generates candidate indexes and write into the corresponding file
 * in the same folder containing the workload.
 * 
 * @author Quoc Trung Tran
 *
 */
public class CandidateGeneratorFunctionalTest extends DivTestSetting 
{  
    /**
     * Read index from the corresponding file or create and store into the file 
     * (if the candidates have not been generated before)
     *  
     * @return
     *      A set of candidate indexes
     * @throws Exception
     */
    public static Set<Index> readCandidateIndexes() throws Exception
    {        
        // test candidate generation
        String fileName = "";
        
        for (String generatorOption : en.getCandidateGenerator()) {
            
            if (generatorOption.equals(OPTIMIZER)) 
                fileName = folder  + "/candidate-optimizer.bin";
            else if (generatorOption.equals(POWERSET))
                fileName = folder  + "/candidate-powerset.bin";
            
            break;
        }
        
        File file = new File(fileName);
        
        if (!file.exists()) {
            
            // Generate candidate indexes
            generateAndWriteToFileOptimizerCandidates(fileName);
            return candidates;
        }
        
        return readIndexes(fileName);
    }
    
    
    /**
     * Generate candidate indexes recommended by the optimizer
     */
    protected static void generateAndWriteToFileOptimizerCandidates(String fileName) throws Exception
    {   
        CandidateGenerator candGen =
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        
        // temporary get only SELECT statements
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(SELECT))
                sqls.add(sql);
        
        Workload wlCandidate = new Workload(sqls);
        candidates = candGen.generate(wlCandidate);
        
        // Calculate the total size (for solely information)
        totalIndexSize = 0;
        for (Index index : candidates) 
            totalIndexSize += index.getBytes();
        
        System.out.println(" OPTIMAL, number of statements to generate candidate: " 
                            + wlCandidate.size() + "\n"
                            + "Number of candidate: " + candidates.size() + "\n" 
                            + "Total size: " + totalIndexSize);
        
        // write to text file
        writeIndexesToFile(candidates, fileName);
    }
    
    
    /**
     * Generate powerset candidate indexes
     */
    protected static void generateAndWriteToFilePowersetCandidates(String fileName) throws Exception
    {
        CandidateGenerator candGen = 
                        new PowerSetOptimalCandidateGenerator(db.getOptimizer(),
                                new OptimizerCandidateGenerator
                                    (getBaseOptimizer(db.getOptimizer())), 3);
        
        // temporary get only SELECT statements
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(SELECT))
                sqls.add(sql);
     
        Workload wlCandidate = new Workload(sqls);
        candidates = candGen.generate(wlCandidate);
    
        // Calculate the total size (for solely information)
        totalIndexSize = 0;
        
        for (Index index : candidates)
            totalIndexSize += index.getBytes();
        
        System.out.println("POWERSET, Number of statements: " + workload.size() + "\n"
                            + "Number of candidate: " + candidates.size() + "\n" 
                            + "Total size: " + totalIndexSize);
        
        // write to text file
        writeIndexesToFile(candidates, fileName);
    }
    
    
    /**
     * Write the set of indexes into the specified file in the format of:
     * {@code id|table_name|col_name|ASC}
     * 
     * @param indexes
     *            A set of indexes to be serialized into binary file
     * @param folder
     *            The folder of the file to be written
     * @param name
     *            The name of the file
     * 
     * @throws IOException
     *             when there is an error in creating files.
     */
    /*
    protected static void writeIndexesToFile(Set<Index> indexes, String name) 
                        throws IOException
    {
        StringBuilder sb;
        PrintWriter   out;
          
        out = new PrintWriter(new FileWriter(name), false);
        
        for (Index idx : indexes) {
            sb = new StringBuilder();
            
            for (Column c : idx.columns()) 
                sb.append(c.getFullyQualifiedName()).append("|")
                        .append(idx.isAscending(c) ? "ASC" : "DESC")
                        .append("|");
                
            //sb.delete(sb.length() - 1, sb.length() );
            sb.append(idx.getBytes());
            sb.append("|").append(idx.getCreationCost());
            out.println(sb.toString());
            
        }
        
        out.close();
    }
    */
    
    protected static void writeIndexesToFile(Set<Index> indexes, String name) 
                          throws Exception
    {
        ObjectOutputStream write;
        
        try {
            write = new ObjectOutputStream(new FileOutputStream(name));
            write.writeObject(indexes);
        } catch(IOException e) {
            throw new SQLException(e);
        }
        
        write.close();
    }
    
    
    /**
     * Read indexes stored from a file
     * @param fileName
     * @return
     * @throws Exception
     */
    /*
    protected static Set<Index> readIndexes(String fileName) throws Exception
    {
        /// reset candidate ID?
        Index.IN_MEMORY_ID = new AtomicInteger(START_INDEX_ID);
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line = null;      
        
        List<Column>  cols;
        Map<Column, Boolean> ascending;
        Set<Index> candidates;
        
        candidates = new HashSet<Index>();
        boolean asc;
        Column col;
        
        while((line = reader.readLine()) != null) {     
            
            String[] token = line.split("\\|");
            
            // {id, table name, column, asc}
            cols = new ArrayList<Column>();
            ascending = new HashMap<Column, Boolean>();
            
            for (int i = 0; i < token.length - 2; i+= 2) {
                col = db.getCatalog().<Column>findByName(token[i]);
                cols.add(col);
                
                asc = true;
                if (token[i + 1].equals("DESC"))
                    asc = false;
                
                ascending.put(col, asc);
            }
            
            Index index = new Index(cols, ascending);
            index.setBytes(Long.parseLong(token[token.length - 2]));
            index.setCreationCost(Double.parseDouble(token[token.length - 1]));
            candidates.add(index);
            //System.out.println("INdex " + index.getId() + " name: " + index.getFullyQualifiedName());
             
        }
        
        System.out.println(" Number of candidates: " + candidates.size());
        return candidates;
    }
    */
    
    @SuppressWarnings("unchecked")
    private static Set<Index> readIndexes(String fileName) throws Exception
    {
        ObjectInputStream in;
        Set<Index> candidates = new HashSet<Index>();
        
        try {
            in = new ObjectInputStream(new FileInputStream(fileName));
            candidates = (Set<Index>) in.readObject();
            
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        
        in.close();
        return candidates;
       
    }
    
}
