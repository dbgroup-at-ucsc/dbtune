package edu.ucsc.dbtune.bip.util;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is a singleton that records the running time of a corresponding BIP solver.
 * It breaks the running time of BIP into three main components:
 * <p> 
 * <ol> 
 *      <li> Communicating with INUM to populate INUM's space </li>
 *      <li> Formulating the BIP </li>
 *      <li> Solving BIP </li> 
 * </ol>
 * </p>
 * 
 * @author Quoc Trung Tran
 *
 */
public class LogListener 
{   
    private static LogListener instance;
    public static int EVENT_PREPROCESS = 0;
    public static int EVENT_POPULATING_INUM = 1;
    public static int EVENT_FORMULATING_BIP = 2;
    public static int EVENT_SOLVING_BIP = 3;    
    private Map<Integer, Double> mapEventTime;
    double startTimer;   
    
    private LogListener()
    {
        mapEventTime = new HashMap<Integer, Double>();
    }
    
    /**
     * Initialize a singleton object of this class
     *  
     * @return
     *      The singleton of this class
     */
    public static synchronized LogListener getInstance() {
        if (instance == null)
            instance = new LogListener();
        
        return instance;
    }
    
    /**
     * Reset the start timer to the current system time
     */
    public void setStartTimer()
    {
        startTimer = System.currentTimeMillis();
    }
    
    /**
     * Log the event, the running time is calculated as the delta between
     * the current time and the last time that is logged by {@code setStartTimer}     
     * The time of the same event is summed up.           
     * 
     * @param event
     *      The ID of the event
     */
    public void onLogEvent(int event)
    {
        // get the running time
        double currentTime = System.currentTimeMillis(); 
        double time = currentTime - startTimer;
        
        // update into the map       
        Object found = mapEventTime.get(new Integer(event));
        if (found == null)
            mapEventTime.put(new Integer(event), new Double(time));
        else { 
            time += (Double) found;
            mapEventTime.put(new Integer(event), new Double(time));
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("LogListener: \n"); 
        double totalTime = 0.0;
        
        Object found = mapEventTime.get(new Integer(EVENT_POPULATING_INUM));
        if (found != null) {
            str.append("Time to populate INUM space: " + (Double) found + " millis. \n");
            totalTime += (Double) found;
        }
        
        found = mapEventTime.get(new Integer(EVENT_FORMULATING_BIP));
        if (found != null) {
            str.append("Time to formulate BIP: " + (Double) found + " millis. \n");
            totalTime += (Double) found;
        }
        
        found = mapEventTime.get(new Integer(EVENT_SOLVING_BIP));
        if (found != null) {
            str.append("Time to solve BIP: " + (Double) found + " millis. \n");
            totalTime += (Double) found;
        }
        
        str.append("Total processing time: " + totalTime + " millis. \n");
        return str.toString();
    }
}