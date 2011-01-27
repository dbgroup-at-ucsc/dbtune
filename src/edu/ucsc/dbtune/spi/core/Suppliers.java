package edu.ucsc.dbtune.spi.core;

import edu.ucsc.dbtune.util.Checks;

import java.io.Serializable;

import static edu.ucsc.dbtune.util.Checks.checkNotNull;

/**
 * a list of useful result suppliers and {@code submitting methods} that deal with commands.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Suppliers {
    private Suppliers(){}

    /**
     * @param instance
     *      instance to be wrapped into a supplier.
     * @return
     *      a supplier that always supplies {@code instance}.
     */
    public static <T> Supplier<T> ofInstance(T instance) {
        return new SupplierOfInstance<T>(instance);
    }

    /**
     * Returns a supplier which caches the instance retrieved during the first
     * call to {@code get()} and returns that value on subsequent calls to
     * {@code get()}. See:
     * <a href="http://en.wikipedia.org/wiki/Memoization">memoization</a>
     *
     * <p>The returned supplier is thread-safe. The supplier's serialized form
     * does not contain the cached value, which will be recalculated when {@code
     * get()} is called on the reserialized instance.
     * @param delegate
     *      delegate to be cached.
     * @return
     *      a memoized supplier.
     */
    public static <T> Supplier<T> memoize(Supplier<T> delegate) {
        return new MemoizingSupplier<T>(checkNotNull(delegate));
    }

    /**
    * Returns a supplier whose {@code get()} method synchronizes on
    * {@code delegate} before calling it, making it thread-safe.
     * @param delegate
     *      delegate to be synchronized.
     * @return
     *      a synchronized supplier.
    */
   public static <T> Supplier<T> synchronizedSupplier(Supplier<T> delegate) {
        return new ThreadSafeSupplier<T>(Checks.checkNotNull(delegate));
   }

    public static class SupplierComposition<R, E extends Exception> implements Supplier<R>, Serializable {
        private static final long serialVersionUID = 0;

        final Function<? extends R, E> function;
        final Supplier<Parameter> first;
        SupplierComposition(
                Function<? extends R, E> function,
                Supplier<Parameter> first
        ) {
            this.function = function;
            this.first    = first;
        }

        @Override
        public R get() {
            try {
                return function.apply(first.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class SupplierOfInstance<T> implements Supplier<T>, Serializable {
        private static final long serialVersionUID = 0;

        final T instance;
        SupplierOfInstance(T instance) {
          this.instance = instance;
        }

        public T get() {
          return instance;
        }

    }

    public static class MemoizingSupplier<T> implements Supplier<T>, Serializable {
        private static final long serialVersionUID = 0;
        final Supplier<T> delegate;
        transient boolean initialized;
        transient T value;

        MemoizingSupplier(Supplier<T> delegate) {
            this.delegate = delegate;
        }

        public synchronized T get() {
            if (!initialized) {
                value = delegate.get();
                initialized = true;
            }
            return value;
        }
   }

    static class ThreadSafeSupplier<T> implements Supplier<T>, Serializable {
       private static final long serialVersionUID = 0;
       final Supplier<T> delegate;
       ThreadSafeSupplier(Supplier<T> delegate) {
           this.delegate = delegate;
       }

       @Override
       public T get() {
           synchronized (delegate) {
               return delegate.get();
           }
       }
   }
}
