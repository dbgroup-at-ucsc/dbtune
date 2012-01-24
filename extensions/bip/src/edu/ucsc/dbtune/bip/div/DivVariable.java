package edu.ucsc.dbtune.bip.div;

import edu.ucsc.dbtune.bip.core.BIPVariable;

public class DivVariable extends BIPVariable 
{
    private int replica;
    
    public DivVariable(String _name, int _type, int _replica)
    {
        super(_name, _type);
        this.replica = _replica;
    }
    
    public int getReplica()
    {
        return this.replica;
    }
}
