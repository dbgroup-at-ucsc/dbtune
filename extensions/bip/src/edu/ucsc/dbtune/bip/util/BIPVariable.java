package edu.ucsc.dbtune.bip.util;

/**
 * A variable object constructed in the BIP.
 * For example: y(empty, 0) for the internal plan cost
 * x(empty, 1, 0) for the index access cost 
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public class BIPVariable 
{
    private String name;
    private int type; 
    
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
}
