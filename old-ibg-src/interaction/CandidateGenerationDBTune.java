package interaction;

import static interaction.cand.Generation.Strategy.UNION_OPTIMAL;
import static interaction.cand.Generation.Strategy.POWER_SET;

import interaction.cand.UnionOptimal;
import interaction.db.DB2Index;
import interaction.db.DB2IndexSet;
import interaction.util.Files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CandidateGenerationDBTune {

    
    public static void main(String[] args) throws IOException, SQLException 
    {
        String fileName = Configuration.candidateTextFile(POWER_SET);
        
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
            
            candidateSet.add(DB2Index.constructIndexFromDBTune("ONEGB", tableName, 
                                    colNames, descending, id));
        }
        
        System.out.println("L54, candidate set: " + candidateSet);
        File optimalCandidateFile = Configuration.candidateFile(POWER_SET);     
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
