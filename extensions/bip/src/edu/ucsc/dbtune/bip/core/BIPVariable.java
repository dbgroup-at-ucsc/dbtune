package edu.ucsc.dbtune.bip.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A variable object constructed in the BIP.
 * For example: y(empty, 0) for the internal plan cost
 * x(empty, 1, 0) for the index access cost 
 * 
 * @author Quoc Trung Tran
 *
 */
public class BIPVariable 
{
    private String name;
    private int    type; 
    private int    inMemoryID;
    
    /** used to uniquely identify each instance of the class. */
    public static AtomicInteger IN_MEMORY_ID = new AtomicInteger(0);
    
    public static void resetIdGenerator()
    {
        IN_MEMORY_ID = new AtomicInteger(0);
    }
    
    /**
     * Construct a {@code BIPVariable} object with the given name and type
     * @param _name
     *      The name of the variable (e.g., y(1), s(2))
     * @param _type
     *      The type of variable (e.g., VAR_X, VAR_S, VAR_Y)
     */
    public BIPVariable(String _name, int _type)
    {
        this.name = _name;
        this.type = _type;
        
        this.inMemoryID = BIPVariable.IN_MEMORY_ID.getAndIncrement();
    }
    
    /**
     * Retrieve the name of the BIP variable
     * @return
     *      The name in string
     */
    public String getName()
    {
        return name;
    }
    
    /**
     * Retrieve the type of the BIP variable (e.g., variable Y, X, S)
     * @return
     *      The type of the variable in {@code integer} type
     */
    public int getType()
    {
        return type;
    }
    
    /**
     * Retrieve the identifier of this object
     * 
     * @return
     *      The ID.
     */
    public int getId()
    {
        return inMemoryID;
    }
}
