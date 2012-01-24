package edu.ucsc.dbtune.bip.sim;

import edu.ucsc.dbtune.bip.core.BIPVariable;

public class SimVariable extends BIPVariable 
{
    private int window;
 
    /**
     * Constructor of a {@code SimVariable} object
     * @param _name
     *      The name of the variable; for example, y(9, 0,0), x(0, 0,0,0,0), create(1, 1)
     * @param _type
     *      The type of the variable; for example, Y, X, CREATE, DROP, etc. 
     * @param _window
     *      The maintenance window on which the variable is defined
     */
    public SimVariable(String _name, int _type, int _window)
    {
        super(_name, _type);
        this.window = _window;
    }
    
    /**
     * Retrieve the window that the variable is defined on
     * 
     * @return
     *      The window ID
     */
    public int getWindow()
    {
        return this.window;
    }
}
