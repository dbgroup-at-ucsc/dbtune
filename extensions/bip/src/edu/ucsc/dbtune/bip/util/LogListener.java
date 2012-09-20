package edu.ucsc.dbtune.bip.util;

import java.util.HashMap;
import java.util.Iterator;
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
    public static int EVENT_PREPROCESS      = 0;
    public static int EVENT_POPULATING_INUM = 1;
    public static int EVENT_FORMULATING_BIP = 2;
    public static int EVENT_SOLVING_BIP     = 3;
    
    private Map<Integer, Double> mapEventTime;
    double  startTimer;   
    
    private LogListener()
    {
        mapEventTime = new HashMap<Integer, Double>();
    }
    
    /**
     * Initialize a singleton object of this class
     *  
     * @return
     *      An instance of this class
     */
    public static synchronized LogListener getInstance() 
    {   
        /*
        if (instance == null)
            instance = new LogListener();
        */
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
            mapEventTime.put(new Integer(event), time);
        else { 
            time += (Double) found;
            mapEventTime.put(new Integer(event), time);
        }
    }
    
    /**
     * Get the running time of a particular event (e.g., populate INUM space)
     * 
     * @param event
     *      The event ID
     * @return
     *      The running time or 0.0 if this event has not been recorded.
     */
    public double getRunningTime(int event)
    {
        Double found = mapEventTime.get(event);
        if (found != null) 
            return found;
        else {
            System.out.println(" This event has not been recorded");
            return 0.0;
        }
    }

    /**
     * Retrieve the total running time
     * @return
     */
    public double getTotalRunningTime()
    {
        double total = 0.0;
        Iterator iter = mapEventTime.entrySet().iterator();
        Map.Entry entry;
        
        while (iter.hasNext()) {
            entry = (Map.Entry) iter.next();
            total += (Double) entry.getValue();
        }
        return total;
    }
    
    @Override
    public String toString() 
    {
        StringBuilder str = new StringBuilder();
        str.append("LogListener: \n"); 
        double totalTime = 0.0;
        
        Double found = mapEventTime.get(EVENT_POPULATING_INUM);
        if (found != null) {
            str.append("Time to populate INUM space: " + found + " millis. \n");
            totalTime += found;
        }
        
        found = mapEventTime.get(EVENT_FORMULATING_BIP);
        if (found != null) {
            str.append("Time to formulate BIP: " + found + " millis. \n");
            totalTime += found;
        }
        
        found = mapEventTime.get(EVENT_SOLVING_BIP);
        if (found != null) {
            str.append("Time to solve BIP: " + found + " millis. \n");
            totalTime += found;
        }
        
        str.append("Total processing time: " + totalTime + " millis. \n");
        return str.toString();
    }
}
