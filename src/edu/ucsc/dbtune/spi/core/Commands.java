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

package edu.ucsc.dbtune.spi.core;

import edu.ucsc.dbtune.util.Checks;

import java.io.Serializable;

import static edu.ucsc.dbtune.util.Checks.checkNotNull;

/**
 * a list of useful result suppliers and {@code submitting methods} that deal with commands.
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Commands {
    private Commands(){}

    /**
     * Returns the composition of two commands (functions). For {@code f: A->B} and
     * {@code g: B->C}, composition is defined as the function h such that
     * {@code h(a) == g(f(a))} for each {@code a}.
     *
     * @see <a href="//en.wikipedia.org/wiki/Function_composition">
     * function composition</a>
     *
     * @param f the first function to apply
     * @param g the second function to apply
     * @return the composition of {@code f} and {@code g}
     */
    public static <C, E extends Exception> Command<C, E> compose(Command<Parameter, E> f, Command<C, E> g){
        return new CommandComposition<C, E>(f, g);
    }

    public static <R, E extends Exception> R supplyValue(Command<R, E> command, Object... o){
        return supplyValue(command, Parameters.makeAnonymousParameter(o));
    }

    public static <R, E extends Exception> R supplyValue(Command<R, E> command, Parameter first){
        return Commands.<R, E>submit(command, first).get();
    }

    public static <R, E extends Exception> Supplier<R> submit(Command<R, E> command, Object... o){
        return submit(command, Parameters.makeAnonymousParameter(o));
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    public static <R, E extends Exception> Supplier<R> submit(Command<R, E> command, Parameter first){
        final Supplier<Parameter> param = ofInstance(first);
        return Commands.<R, E>submit(command, param);
    }

    public static <R, E extends Exception> Supplier<R> submit(Command<R, E> function, Supplier<Parameter> first){
        return new SupplierComposition<R, E>(function, first);
    }

    public static void submitAll(Supplier<?>... suppliers){
        for(Supplier<?> each : suppliers){
            each.get();
        }
    }

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

    private static class CommandComposition<C, E extends Exception> implements Command<C, E>, Serializable {
        private static final long serialVersionUID = 0;
        private final Command<Parameter, E> first;
        private final Command<C, E> second;

        public CommandComposition(Command<Parameter, E> first, Command<C, E> second){
            this.first  = checkNotNull(first);
            this.second = checkNotNull(second);
        }
        
        @Override
        public C apply(Parameter input) throws E {
            return second.apply(first.apply(input));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CommandComposition) {
                CommandComposition<?, ?> that = (CommandComposition<?, ?>) obj;
                return second.equals(that.second) && first.equals(that.first);
            }

            return false;
        }

        @Override
        public int hashCode() {
            return second.hashCode() ^ first.hashCode();
        }

        @Override
        public String toString() {
            return second.toString() + "(" + first.toString() + ")";
        }
    }

    public static class SupplierComposition<R, E extends Exception> implements Supplier<R>, Serializable {
        private static final long serialVersionUID = 0;

        final Command<? extends R, E> function;
        final Supplier<Parameter> first;
        SupplierComposition(
                Command<? extends R, E> function,
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

  private static class ThreadSafeSupplier<T> implements Supplier<T>, Serializable {
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
