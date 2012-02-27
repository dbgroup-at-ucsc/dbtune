package edu.ucsc.dbtune.bip.div;

import edu.ucsc.dbtune.bip.core.BIPVariable;

public class DivVariable extends BIPVariable 
{
    private int replica;
    
    /**
     * Constructor of an variable that is used in {@code DivBIP}.
     * 
     * @param _name
     *      The name of variable
     * @param _type
     *      The type of variable (e.g., VAR_Y, VAR_X)
     * @param _replica
     *      The replica on which the variable is defined
     */
    public DivVariable(String _name, int _type, int _replica)
    {
        super(_name, _type);
        this.replica = _replica;
    }
    
    /**
     * Retrieve the replica that the variable is defined
     * 
     * @return
     *      The replica ID.
     */
    public int getReplica()
    {
        return this.replica;
    }
}
