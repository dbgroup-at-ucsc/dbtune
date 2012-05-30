package edu.ucsc.dbtune.bip;

import java.util.List;

/**
 * Keep the result
 * @author Quoc Trung Tran
 *
 */
public class DivTestEntry
{
    public String type;
    public List<Double> result;
    
    public DivTestEntry(String t, List<Double> r)
    {
        this.type = t;
        this.result = r;
    }
    
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(" type = " + type + "\n");
        
        for (double v : result)
            sb.append(Math.round(v) + "\n");
        
        return sb.toString();
    }
    
}
