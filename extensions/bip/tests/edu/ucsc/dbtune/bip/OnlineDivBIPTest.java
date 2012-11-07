package edu.ucsc.dbtune.bip;

import java.io.File;

import org.junit.Test;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.util.Rt;

import static edu.ucsc.dbtune.bip.util.LogListener.EVENT_SOLVING_BIP;
import static edu.ucsc.dbtune.bip.util.LogListener.EVENT_FORMULATING_BIP;
import static edu.ucsc.dbtune.bip.util.LogListener.EVENT_POPULATING_INUM;

public class OnlineDivBIPTest extends AdaptiveDivBIPTest 
{   
    /**
    *
    * Generate paper results
    */
   @Test
   public void main() throws Exception
   {   
       // 1. initialize file locations & necessary 
       // data structures
       initialize();
       
       // get database instances, candidate indexes
       getEnvironmentParameters();
       setParameters();
       
       // set control knobs
       controlKnobs();
       
       // prepare workload
       prepareWorkload();
       
       getInitialConfiguration(0, -1);
       initializeOnlineObject();
       runOnline();
   }
   
   /**
    * Run online experiments
    * @throws Exception
    */
   protected static void runOnline() throws Exception
   {   
       long startTime = System.currentTimeMillis();
       
       onlineFile = new File(rawDataDir, wlName + "_" + ONLINE_FILE
                           + "_windows_" + windowDuration);
       onlineFile.delete();
       onlineFile = new File(rawDataDir, wlName + "_" + ONLINE_FILE
               + "_windows_" + windowDuration);
       onlineFile.createNewFile();
    
       onlineEntry = new OnlinePaperEntry();
       
       // initial divergent design: the first phase
       getInitialConfiguration(0, 199);
       onlineDiv.setInitialConfiguration(initialConf);
       onlineDiv.setDivBIP(div);
       
       // populate INUM space
       LogListener logger = LogListener.getInstance();
       onlineDiv.setLogListenter(logger);
       
       double timeBIP, timeInum;
       timeBIP = 0.0;
       timeInum = 0.0;
       Rt.p("Number of statements = " + numStatementsOnline);
       onlineEntry.setWindowDuration(windowDuration);
       
       for (int i = 0; i < numStatementsOnline; i++) {
           logger.reset();
           onlineDiv.next();
           
           timeBIP += logger.getRunningTime(EVENT_SOLVING_BIP);
           timeBIP += logger.getRunningTime(EVENT_FORMULATING_BIP);
           timeInum += logger.getRunningTime(EVENT_POPULATING_INUM);
           
           /**
            * Compare with initial configuration
            * Might need to revisit later
            */
           onlineEntry.addCosts(onlineDiv.getTotalCostInitialConfiguration(),
                   onlineDiv.getTotalCost());
           if (onlineDiv.isNeedToReconfiguration()) {
               onlineEntry.addReconfiguration(i);
               Rt.p(" RECONFIGURATION AT : " + i);
           }
           
           //onlineEntry.addCosts(-1, onlineDiv.getTotalCost());
           
           if (i % 20 == 0) {
               Rt.p(" AFTER process statement: " + i);
               onlineEntry.setTimes(timeBIP, timeInum, 
                       (System.currentTimeMillis() - startTime));
               Rt.p(" store in file = " + onlineFile.getName());
               serializeOnlineResult(onlineEntry, onlineFile);
           }
       }
       
       onlineEntry.setTimes(timeBIP, timeInum, 
                       (System.currentTimeMillis() - startTime));
       Rt.p(" store in file = " + onlineFile.getName());
       serializeOnlineResult(onlineEntry, onlineFile);
   } 
}
