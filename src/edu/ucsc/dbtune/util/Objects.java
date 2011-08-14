/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
package edu.ucsc.dbtune.util;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Objects {
    private Objects(){}
    
    /**
     * check if two objects are equal.
     * @param o1
     *      first object.
     * @param o2
     *      second object.
     * @return
     *      {@code true} if equal. {@code false} otherwise.
     */
    public static boolean equals(Object o1, Object o2) {
        return o1 == o2 || !(o1 == null || o2 == null) && o1.equals(o2);
    }

    /**
     * generates a hashCode using the {@link Arrays} object.
     * @param objects
     *      objects to be used in the hashCode generation.
     * @return
     *      hashcode.
     */
    public static int hashCode(Object... objects){
      int result = HashCodeUtil.SEED;
      for(Object each : objects){
         result = HashCodeUtil.hash(result, each); 
      }
      return result;
    }

    /**
     * another alternative to explicit casting of objects when you know in advance
     * the class that you wanted to cast to... let me repeat again, this is a good method
     * when u know the class to which u want to downcast.
     * @param me
     *      object to be cast.
     * @param to
     *      cast to this class.
     * @param <T>
     *      type bound.
     * @param <E>
     *      type bound.
     * @return
     *      E or cast object.
     */
    public static <T, E extends T> E cast(T me, Class<? extends E> to){
        return !to.isPrimitive() ? to.cast(me) : Objects.<E>as(me);
    }

    @SuppressWarnings({"unchecked"})
    public static <T> T as(Object obj){
        return (T) obj;
    }

    public static <T> Class<T> discoverClass(T instance){
        return as(instance.getClass());
    }

    /**
     * check if there is a bond between an owner class (the one who defines
     * the members that can be accepter by the club) and an applicant object.
     * @param owner
     *      club's owner.
     * @param applicant
     *      applicant class who wishes to be part of the club
     *      which has allowed the owner to run a background check
     *      on it..
     * @return
     *      {@code true} if both classes are in the same club.
     */
    public static boolean inTheClub(Class<?> owner, Class<?> applicant){
        return ((owner.isAssignableFrom(applicant)) || (owner == applicant) || (applicant.isInstance(owner)));
    }

    /**
     * discover the raw type (class<?>) of type.
     * @param type
     *      type to be examined.
     * @return
     *      raw type in the form of a class.
     */
    public static Class<?> raw(Type type){
        if (type instanceof Class<?>) {
            // type is a normal class.
            return (Class<?>) type;

        } else if (type instanceof ParameterizedType) {
            final ParameterizedType parameterizedType = (ParameterizedType) type;

            // Neal isn't sure why getRawType() returns Type instead of Class, but suspects some pathological case related
            // to nested classes exists.
            final Type rawType = parameterizedType.getRawType();
            if(!(rawType instanceof Class)){
              throw new ClassCastException();
            }

            return (Class<?>) rawType;
        } else if (type instanceof GenericArrayType) {
            final Type componentType = ((GenericArrayType)type).getGenericComponentType();
            return Array.newInstance(raw(componentType), 0).getClass();
        } else if (type instanceof TypeVariable) {
            // we could use the variable's bounds, but that'll won't work if there are multiple.
            // having a raw type that's more general than necessary is okay
            return Object.class;
        } else {
            throw new IllegalArgumentException("Expected a Class, ParameterizedType, or "
                + "GenericArrayType, but <" + type + "> is of type " + type.getClass().getName());
        }
    }
  
  
  /**
  * Collected methods which allow easy implementation of <code>hashCode</code>.
  *
  * Example use case:
  * <pre>
  *  public int hashCode(){
  *    int result = HashCodeUtil.SEED;
  *    //collect the contributions of various fields
  *    result = HashCodeUtil.hash(result, fPrimitive);
  *    result = HashCodeUtil.hash(result, fObject);
  *    result = HashCodeUtil.hash(result, fArray);
  *    return result;
  *  }
  * </pre>
   *
   * @see <a href="http://www.javapractices.com/topic/TopicAction.do?Id=28">JavaPractices</a>
  */
  private static final class HashCodeUtil {
  
    /**
    * An initial value for a <code>hashCode</code>, to which is added contributions
    * from fields. Using a non-zero value decreases collisons of <code>hashCode</code>
    * values.
    */
    public static final int SEED = 23;
  
  
    private static final int ODD_PRIME_NUMBER = 37;
  
    private static int firstTerm( int aSeed ){
      return ODD_PRIME_NUMBER * aSeed;
    }
  
    /**
    * booleans.
    */
    public static int hash( int aSeed, boolean aBoolean ) {
      return firstTerm( aSeed ) + ( aBoolean ? 1 : 0 );
    }
  
    /**
    * chars.
    */
    public static int hash( int aSeed, char aChar ) {
      return firstTerm( aSeed ) + (int)aChar;
    }
  
    /**
    * ints.
    */
    public static int hash( int aSeed , int aInt ) {
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
    public static int hash( int aSeed , long aLong ) {
      return firstTerm(aSeed)  + (int)( aLong ^ (aLong >>> 32) );
    }
  
    /**
    * floats.
    */
    public static int hash( int aSeed , float aFloat ) {
      return hash( aSeed, Float.floatToIntBits(aFloat) );
    }
  
    /**
    * doubles.
    */
    public static int hash( int aSeed , double aDouble ) {
      return hash( aSeed, Double.doubleToLongBits(aDouble) );
    }
  
    /**
    * <code>aObject</code> is a possibly-null object field, and possibly an array.
    *
    * If <code>aObject</code> is an array, then each element may be a primitive
    * or a possibly-null object.
    */
    public static int hash( int aSeed , Object aObject ) {
      int result = aSeed;
      if ( aObject == null) {
        result = hash(result, 0);
      } else if (!isArray(aObject) ) {
        result = hash(result, aObject.hashCode());
      } else {
        int length = Array.getLength(aObject);
        for (int idx = 0; idx < length; ++idx) {
          Object item = Array.get(aObject, idx);
          //recursive call!
          result = hash(result, item);
        }
      }
      return result;
    }
  
    private static boolean isArray(Object anObject){
      return anObject.getClass().isArray();
    }
  }   

}

