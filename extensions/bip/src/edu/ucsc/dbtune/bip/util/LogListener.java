package edu.ucsc.dbtune.bip.util;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is a singleton that records the running time of a corresponding BIP solver.
 * It also breaks the running time of BIP into three main components:
 * (1) communicating with INUM to populate INUM's space,
 * (2) formulating the BIP, and
 * (3) solving BIP. 
 * 
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public class LogListener 
{   
    private static LogListener instance;
    public static int EVENT_PREPROCESS = 0;
    public static int EVENT_INUM_POPULATING = 1;
    public static int EVENT_BIP_FORMULATING = 2;
    public static int EVENT_BIP_SOLVING = 3;    
    private Map<Integer, Double> mapEventTime;
    double startTimer;    
    
    
    private LogListener()
    {
        mapEventTime = new HashMap<Integer, Double>();
        startTimer = System.currentTimeMillis();
    }
    
    /**
     * The starting timer is recorded at the time this object is initiated.
     *  
     * @return
     *      The singleton of this class
     */
    public static synchronized LogListener getInstance() {
        if (instance == null) {
            instance = new LogListener();
        }
        
        return instance;
    }
    
    /**
     * Log the event, the running time is calculated as the delta between
     * the current time and the last time that is logged by this object.
     * The last time logged by this object is reset to be the current time.          
     * 
     * @param event
     *      The ID of the event
     */
    public void onLogEvent(int event)
    {
        // get the running time
        double currentTime = System.currentTimeMillis(); 
        double time = currentTime - startTimer;
        // reset the timer
        startTimer = currentTime;
        // update into the map       
        Object found = mapEventTime.get(new Integer(event));
        if (found == null) {
            mapEventTime.put(new Integer(event), new Double(time));
        } else {
            time += (Double) found;
            mapEventTime.put(new Integer(event), new Double(time));
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("LogListener: \n"); 
        double totalTime = 0.0;
        
        Object found = mapEventTime.get(new Integer(EVENT_INUM_POPULATING));
        if (found != null) {
            str.append("Time to populate INUM space: " + (Double) found + " millis. \n");
            totalTime += (Double) found;
        }
        
        found = mapEventTime.get(new Integer(EVENT_BIP_FORMULATING));
        if (found != null) {
            str.append("Time to formulate BIP: " + (Double) found + " millis. \n");
            totalTime += (Double) found;
        }
        
        found = mapEventTime.get(new Integer(EVENT_BIP_SOLVING));
        if (found != null) {
            str.append("Time to solve BIP: " + (Double) found + " millis. \n");
            totalTime += (Double) found;
        }
        
        str.append("Total processing time: " + totalTime + " millis. \n");
        return str.toString();
    }
}