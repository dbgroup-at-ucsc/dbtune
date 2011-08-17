package edu.ucsc.dbtune.util;

import org.junit.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import static com.google.caliper.internal.guava.base.Preconditions.checkArgument;
import static edu.ucsc.dbtune.util.Objects.as;
import static org.junit.Assert.assertNotSame;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class TypeReferenceTest {

    @SuppressWarnings("unchecked")
    @Test(expected = IllegalArgumentException.class)
    public void testBasicArrayListExample() throws Exception {
        // Consider GuardingList<T>, which enforces type safety at runtime...
        final List<String> o = new GuardingList<String>(new TypeReference<String>(){});
        o.add("Yeah!");
        @SuppressWarnings("rawtypes")
        final List raw = as(o);
        raw.add(1234);  // it should throw an illegal argument exception 
    }

    @SuppressWarnings("unchecked")
    @Test(expected = IllegalArgumentException.class)
    public void testDealingWithRawTypes() throws Exception {
        @SuppressWarnings("rawtypes")
        final Dancing salsa = salsaDancingToRaw();
        @SuppressWarnings("rawtypes")
        final Dancing tango = tangoDancingToRaw();
        assertNotSame(tango, salsa);
        salsa.mix(tango);
    }


    @SuppressWarnings("rawtypes")
    static Dancing salsaDancingToRaw(){
        return new SalsaDancing(new TypeReference<SalsaDancing>(){});
    }

    @SuppressWarnings("rawtypes")
    static Dancing tangoDancingToRaw(){
        return new TangoDancing(new TypeReference<TangoDancing>(){});
    }


    interface Dancing <D extends Dancing<D>> {
        void mix(Dancing<D> that);
        D yeahLalala();
    }

    static class SalsaDancing implements Dancing<SalsaDancing>{
        private final TypeReference<SalsaDancing> ref;

        SalsaDancing(TypeReference<SalsaDancing> ref){
            this.ref = ref;
        }

        @Override
        public void mix(Dancing<SalsaDancing> that) {
            if(!ref.getGenericClass().isInstance(that)) throw new IllegalArgumentException();
        }

        @Override
        public SalsaDancing yeahLalala() {
            if(!ref.getGenericClass().isInstance(this)) throw new IllegalArgumentException();
            return this;
        }

        @Override
        public String toString() {
            return "salsa";
        }
    }

    static class TangoDancing implements Dancing<TangoDancing>{
        private final TypeReference<TangoDancing> ref;
        private Dancing<TangoDancing> that;

        TangoDancing(TypeReference<TangoDancing> ref){
            this.ref = ref;
        }

        @Override
        public void mix(Dancing<TangoDancing> that) {
            if(!ref.getGenericClass().isInstance(that)) throw new IllegalArgumentException();
            this.that = that;
        }

        @Override
        public TangoDancing yeahLalala() {
            return this;
        }

        @Override
        public String toString() {
            return "tango(" + that + ")";
        }
    }

    static class GuardingList<T> extends AbstractList<T> {
      private final TypeReference<T> elementType;
      private final ArrayList<T> delegate = new ArrayList<T>();

      GuardingList(TypeReference<T> elementType) {
        this.elementType = elementType;
      }

      public boolean add(T t) {
        checkArgument(elementType.getGenericClass().isInstance(t),
            "Cannot add %s which is not of type %s", t, elementType);
        return delegate.add(t);
      }

      public T get(int i) {
        return delegate.get(i);
      }

      public int size() {
        return delegate.size();
      }
    }
}
