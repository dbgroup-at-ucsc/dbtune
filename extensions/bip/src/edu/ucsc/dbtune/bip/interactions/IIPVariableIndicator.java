package edu.ucsc.dbtune.bip.interactions;


public class IIPVariableIndicator 
{
    private int theta, typeVariable, q, k, i, a;
    private int fHashCode;
    
    /**
     * A {@code IIPVarible} is defined based on the following
     * four parameters:
     * 
     * @param _theta
     *      Variables are defined on either VAR_EMPTY, VAR_C, VAR_D, VAR_CD           
     * @param _typeVariable
     *      The type of variable (e.g., VAR_X, VAR_Y)
     * @param _k
     *      The query plan ID
     * @param _a
     *      The index ID
     */
    public IIPVariableIndicator(int _theta, int _typeVariable, int _q, int _k, int _i, int _a)
    {
        this.theta = _theta;
        this.typeVariable = _typeVariable;
        this.q = _q;
        this.k = _k;
        this.i = _i;
        this.a = _a;
        fHashCode = 0;
    }
    

    @Override
    public boolean equals(Object obj) 
    {
        if (!(obj instanceof IIPVariableIndicator))
            return false;

        IIPVariableIndicator var = (IIPVariableIndicator) obj;
        
        if ( (this.theta != var.theta) ||
             (this.typeVariable != var.typeVariable) ||
             (this.q != var.q) ||
             (this.k != var.k) ||
             (this.i != var.i) ||
             (this.a != var.a) ) {
            return false;
        }
        
        return true;
    }

    @Override
    public int hashCode() 
    {
        if (fHashCode == 0) {
            int result = 0;
            
            result = 37 * result + this.theta;
            result = 37 * result + this.typeVariable;
            result = 37 * result + this.q;
            result = 37 * result + this.k;
            result = 37 * result + this.i;
            result = 37 * result + this.a;
            fHashCode = result;
        }
        
        return fHashCode;
    }


    @Override
    public String toString() {
        return "IIPVariableIndex [a=" + a + ", fHashCode=" + fHashCode + ", q=" + q
                 + ", k=" + k + ", theta=" + theta + ", typeVariable="
                 + typeVariable + "]";
    }    
}
