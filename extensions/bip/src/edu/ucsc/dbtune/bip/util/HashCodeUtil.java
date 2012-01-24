package edu.ucsc.dbtune.bip.util;

public final class HashCodeUtil 
{
	 /**
	  * An initial value for a <code>hashCode</code>, to which is added contributions
	  * from fields. Using a non-zero value decreases collisions of <code>hashCode</code>
	  * values.
	  */
	  public static final int SEED = 23;

	  /**
	  * booleans.
	  */
	  public static int hash( int aSeed, boolean aBoolean ) 
	  {	    
	    return firstTerm( aSeed ) + ( aBoolean ? 1 : 0 );
	  }

	  /**
	      * chars.
	      */
	  public static int hash( int aSeed, String aString ) 
	  {        
	      int total = 0;
	      for (int i = 0; i < aString.length(); i++) {
	          total += aString.charAt(i);
	      }
	       return firstTerm( aSeed ) + total;
	  }
	      
	  /**
	  * chars.
	  */
	  public static int hash( int aSeed, char aChar ) 
	  {
	   
	    return firstTerm( aSeed ) + (int)aChar;
	  }

	  /**
	  * ints.
	  */
	  public static int hash( int aSeed , int aInt ) 
	  {
	    /*
	    * Implementation Note
	    * Note that byte and short are handled by this method, through
	    * implicit conversion.
	    */	  
	    return firstTerm( aSeed ) + aInt;
	  }

	  /**
	  * longs.
	  */
	  public static int hash( int aSeed , long aLong ) 
	  {	    
	    return firstTerm(aSeed)  + (int)( aLong ^ (aLong >>> 32) );
	  }

	  /**
	  * floats.
	  */
	  public static int hash( int aSeed , float aFloat )
	  {
	    return hash( aSeed, Float.floatToIntBits(aFloat) );
	  }

	  /**
	  * doubles.
	  */
	  public static int hash( int aSeed , double aDouble ) 
	  {
	    return hash( aSeed, Double.doubleToLongBits(aDouble) );
	  }
	  
	  private static final int fODD_PRIME_NUMBER = 37;
	  private static int firstTerm( int aSeed )
	  {
	    return fODD_PRIME_NUMBER * aSeed;
	  }
}

	  
