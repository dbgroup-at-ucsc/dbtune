package edu.ucsc.dbtune.bip.util;

/**
 * Variables constructed in the BIP (e.g., y(empty, 0), x(c, kia), ...)
 * 
 * @author tqtrung
 *
 */
public class BIPVariable 
{
    private String name;
    private int type;
    
    public BIPVariable(String _name, int _type)
    {
        this.name = _name;
        this.type = _type;
    }
    
    public String getName()
    {
        return name;
    }
    
    public int getType()
    {
        return type;
    }
}
