package edu.ucsc.dbtune.bip.sim;

import edu.ucsc.dbtune.bip.util.BIPVariable;

public class SimVariable extends BIPVariable 
{
    private int window;
    
    public SimVariable(String _name, int _type, int _window)
    {
        super(_name, _type);
        this.window = _window;
    }
    
    public int getWindow()
    {
        return this.window;
    }
}
