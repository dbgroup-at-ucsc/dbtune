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
package edu.ucsc.satuning.spi;

import edu.ucsc.satuning.util.Util;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class ParametersTest {
    @Test
    public void testSingleParameter() throws Exception {
        final Parameter p = Parameters.makeNamedParameter("string", "uno");
        assertEquals("same strings containing 'uno'", "uno", p.getParameterValue(String.class));
    }
    
    @Test
    public void testSingleParameterWithMultipleValues() throws Exception {
        final Parameter p = Parameters.makeNamedParameter("Just", "dos", 3, 3.1415);
        assertEquals("same integer with value of 3", Integer.valueOf(3), p.getParameterValue(Integer.class));
        assertTrue("same double with value of 3.1415", Double.compare(3.1415, p.getParameterValue(Double.class)) == 0);
    }

    @Test(expected = NullPointerException.class)
    public void testPassingNullInVarArgs() throws Exception {
        final Parameter nullParameter = Parameters.makeAnonymousParameter((Object)null);
        nullParameter.getParameterValue(null);
        fail("it should not get here");
    }

    @Test
    public void testDualKeyParameter() throws Exception {
        @SuppressWarnings({"UnnecessaryBoxing"})
        final Parameter p = Parameters.makeAnonymousParameter(true);
        assertTrue(p.getParameterValue(Boolean.class));
        assertTrue(p.getParameterValue(boolean.class));        
    }

    @Test
    public void testInterfaceRetrievalNotClass() throws Exception {
        final Parameter param = Parameters.makeAnonymousParameter(new SomeTypeImpl());
        final SomeType  type  = param.getParameterValue(SomeType.class);
        assertNotNull(type);
        assertSame("it should say Hello world!", "Hello World!", type.name());
    }

    @Test
    public void testInterfaceRetrievalMultipleImpls() throws Exception {
        final Parameter param = Parameters.makeAnonymousParameter(new SomeTypeImpl(), new SomeOtherImpl());
        final SomeType  type  = param.getParameterValue(SomeType.class);
        assertNotNull(type);
        //assertNotSame("it should not say You again?", "You again?", type.name());
        final SomeType  other = param.getParameterValue(SomeOtherImpl.class);
        assertNotNull(other);
        //assertSame("it should say You again?", "You again?", other.name());
    }

    @Test
    public void testGenericInterfaceCase() throws Exception {
        final List<SomeType> types = Util.newList();
        types.add(new SomeTypeImpl());
        final Parameter generic = Parameters.makeAnonymousParameter(types.iterator());
        @SuppressWarnings({"unchecked"})
        final Iterator<SomeType> itr = generic.getParameterValue(Iterator.class);
        assertNotNull("iterator should not be null", itr);
        for(; itr.hasNext();){
            assertNotNull(itr.next());
        }
    }

    public interface SomeType {
          String name();
    }

    public static class SomeTypeImpl implements SomeType{

        @Override
        public String name() {
            return "Hello World!";
        }
    }

    public static class SomeOtherImpl implements SomeType {

        @Override
        public String name() {
            return "You again?";
        }
    }

}
