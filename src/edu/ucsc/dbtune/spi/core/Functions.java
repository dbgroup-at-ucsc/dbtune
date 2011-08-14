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

import java.io.Serializable;

import static edu.ucsc.dbtune.util.Checks.checkNotNull;

/**
 * a list of useful commands.
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
//todo(Huascar) rename to Functions
public class Functions {
    private Functions(){}

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
    public static <C, E extends Exception> Function<C, E> compose(Function<Parameter, E> f, Function<C, E> g){
        return new FunctionComposition<C, E>(f, g);
    }

    public static <R, E extends Exception> R supplyValue(Function<R, E> command, Object... o){
        return supplyValue(command, Parameters.makeAnonymousParameter(o));
    }

    public static <R, E extends Exception> R supplyValue(Function<R, E> command, Parameter first){
        return Functions.<R, E>submit(command, first).get();
    }

    public static <R, E extends Exception> Supplier<R> submit(Function<R, E> command, Object... o){
        return submit(command, Parameters.makeAnonymousParameter(o));
    }

    public static <R, E extends Exception> Supplier<R> submit(Function<R, E> command, Parameter first){
        final Supplier<Parameter> param = Suppliers.ofInstance(first);
        return Functions.<R, E>submit(command, param);
    }

    public static <R, E extends Exception> Supplier<R> submit(Function<R, E> function, Supplier<Parameter> first){
        return new Suppliers.SupplierComposition<R, E>(function, first);
    }

    public static void submitAll(Supplier<?>... suppliers){
        for(Supplier<?> each : suppliers){
            each.get();
        }
    }

    private static class FunctionComposition<C, E extends Exception> implements Function<C, E>, Serializable {
        private static final long serialVersionUID = 0;
        private final Function<Parameter, E> first;
        private final Function<C, E> second;

        public FunctionComposition(Function<Parameter, E> first, Function<C, E> second){
            this.first  = checkNotNull(first);
            this.second = checkNotNull(second);
        }
        
        @Override
        public C apply(Parameter input) throws E {
            return second.apply(first.apply(input));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FunctionComposition) {
                FunctionComposition<?, ?> that = (FunctionComposition<?, ?>) obj;
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

}
