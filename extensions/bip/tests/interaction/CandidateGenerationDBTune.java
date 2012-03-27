package interaction;

import interaction.cand.Generation;
import interaction.db.DB2Index;
import interaction.db.DB2IndexSet;
import interaction.util.Files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.util.Environment;

public class CandidateGenerationDBTune 
{    
    private static Environment    en;
    
    public static void readIndexesFromDBTune(Generation.Strategy strategy, String dbName,
                                             String tableOwner, String subfolder)
                throws Exception
    {
        en = Environment.getInstance();
        //String folder = en.getWorkloadsFoldername() + "/tpcds-small";
        String folder = en.getWorkloadsFoldername() + subfolder;
        System.out.println("L29, subfolder: " + folder);
        String fileName;
        
        if (strategy.equals(Generation.Strategy.OPTIMAL_1C))
            fileName = folder  + "/candidate-1C.txt";
        else if (strategy.equals(Generation.Strategy.POWER_SET))
            fileName = folder  + "/candidate-powerset.txt";
        else
            fileName = folder  + "/candidate-optimal.txt";
                
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line = null;
        
        int    id;
        String tableName;        
        
        List<String>  colNames;
        List<Boolean> descending;
        DB2IndexSet candidateSet = new DB2IndexSet();
        
        while((line = reader.readLine()) != null) {         
            String[] token = line.split("\\|");
            
            // {id, table name, column, asc}
            colNames  = new ArrayList<String>();
            descending = new ArrayList<Boolean>();
            id        = Integer.parseInt(token[0]);
            tableName = token[1];
            
            for (int i = 2; i < token.length; i+= 2) {
                
                colNames.add(token[i]);
                if (token[i + 1].equals("DESC"))
                    descending.add(true);
                else if (token[i + 1].equals("ASC"))
                    descending.add(false);
            }
            
            candidateSet.add(DB2Index.constructIndexFromDBTune(dbName, tableName, tableOwner, 
                                    colNames, descending, id));
        }
        
        System.out.println("L78, candidate set: " + candidateSet.size());
        File optimalCandidateFile = Configuration.candidateFile(strategy);     
        writeCandidates(optimalCandidateFile, candidateSet);
    }
    
    private static void writeCandidates(File outputFile, DB2IndexSet indexes) throws IOException 
    {
        ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(outputFile));
        try {
            out.writeObject(indexes);
        } finally {
            out.close(); // closes underlying stream
        }
    }
}