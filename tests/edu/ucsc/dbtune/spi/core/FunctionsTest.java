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

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.ucsc.dbtune.spi.core.Functions.compose;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import com.google.common.base.Supplier;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class FunctionsTest {
    @Test
    public void testShareResourceViaCommandComposition() throws Exception {
        // the scenario goes like this:
        // image SomeResource is the execution of a query which results in a
        // ResultSet object.
        // Note: the code for executing that query and then returning that ResultSet
        // was the same in two or three other commands (code duplication)
        // we therefore created a single command that encapsulated that query execution
        // and return of a ResultSet and compose it with other commands that needed that
        // same ResultSet. This is the rationale of command composition (sharing a resource
        // among other commands that need it).
        final Supplier<Boolean> answer = Functions.submit(compose(new Speaker(), new Listener()), "Hello World!");
        assertTrue("it should be true; i.e., message was told.", answer.get());
        final Supplier<Boolean> noAnswer = Functions.submit(compose(new Speaker(), new Listener()));
        assertFalse("it should be true; i.e., message was told.", noAnswer.get());
    }

    @Test
    public void testExecutionOfManySubmittedCommandsWithNotReturn() throws Exception {
        // all or none...either all commands got executed (true return) or none of them (false return)
        final AtomicBoolean executionHistoryYesOrNot = new AtomicBoolean(false);
        final AtomicInteger commandCounter           = new AtomicInteger(0);
        final Function<Boolean, RuntimeException> openCommand = new Function<Boolean, RuntimeException>(){
            @Override
            public Boolean apply(Parameter input) throws RuntimeException {
                final String message = input.getParameterValue(String.class);
                System.out.println("command" + commandCounter.incrementAndGet() + " said: "  + message);
                executionHistoryYesOrNot.set(message != null);
                return executionHistoryYesOrNot.get();
            }
        };

        Functions.submitAll(
                Functions.submit(openCommand, "got executed...."),
                Functions.submit(openCommand, "got executed...."),
                Functions.submit(openCommand, "got executed....")
        );

        assertTrue("all commands got executed.", executionHistoryYesOrNot.get());
        assertTrue("three commands were executed.", commandCounter.get() == 3);

    }

    @Test
    public void testSuppliedResultByCommand() throws Exception {
        final ArgumentApplicationCounter counter = new ArgumentApplicationCounter();
        final Supplier<Integer> value = Functions.submit(counter);
        Functions.submitAll(value, value);
        final Integer threeTimes = value.get(); // this is an extra call, so the counter == 3
        assertTrue("only three applications", threeTimes.compareTo(3) == 0);
    }

    public static class ArgumentApplicationCounter implements Function<Integer, RuntimeException> {
        private int counter;
        ArgumentApplicationCounter(){
            counter = 0;
        }

        @Override
        public Integer apply(Parameter input) throws RuntimeException {
            return ++counter;
        }
    }


    private static class Speaker implements Function<Parameter, RuntimeException> {
        @Override
        public Parameter apply(Parameter input) throws RuntimeException {
            try {
                final String message = input.getParameterValue(String.class);
                return Parameters.makeAnonymousParameter(new ResultSetResource(message));
            } catch(Exception e){
                return Parameters.makeAnonymousParameter(new ResultSetResource(null));
            }
        }
    }

    private static class Listener implements Function<Boolean, RuntimeException> {
        @Override
        public Boolean apply(Parameter input) throws RuntimeException {
            final ResultSetResource resource = input.getParameterValue(ResultSetResource.class);
            if(resource == null || !resource.isValidMessage()) return false;
            resource.tell();
            return true;  //this is okay
        }
    }

    private static class ResultSetResource {
        final String message;
        ResultSetResource(String message){
            this.message = message;
        }

        boolean isValidMessage(){
            return message != null;
        }

        void tell(){
            System.out.println(message);
        }
    }
}
