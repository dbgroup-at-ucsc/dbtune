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

import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class CommonDataStructuresTest {
    @Test
    public void testBasicScenarioOfQueue() throws Exception {
        final Queue<Integer> queue = new Queue<Integer>();
        populateQueue(queue, 1000);

        assertSame("first element is 0", queue.fetch(0), 0);
        while(!queue.isEmpty()){
            queue.remove();
        }
        assertTrue("Queue is empty", queue.isEmpty());
        assertTrue("Queue's count is 0", queue.count() == 0);

        final Queue<Integer> queue2 = new Queue<Integer>();
        populateQueue(queue2, 100);
        queue2.clear();
        assertTrue("Queue is empty", queue2.isEmpty());
        assertTrue("Queue's count is 0", queue2.count() == 0);
    }

    @Test
    public void testQueueReplaceElements() throws Exception {
        final Queue<Integer> queue = new Queue<Integer>();
        populateQueue(queue, 10);
        queue.replace(4, 55);
        assertTrue("Queue is not empty", !queue.isEmpty());
        assertTrue("Queue's count is 10", queue.count() == 10);
        assertSame("element is 55", queue.fetch(4), 55);
    }

    @Test
    public void testStackSwappingElements() throws Exception {
        final Stack<Integer> stack = new Stack<Integer>();
        populateStack(stack, 10);
        stack.swap(99);
        assertSame("101 should be the top.", stack.peek(), 99);
    }

    @Test
    public void testStackExhaustively() throws Exception {
        final Stack<Integer> stack = new Stack<Integer>();
        populateStack(stack, 1000);
        stack.swap(101);
        final int top = stack.peek();
        assertSame("101 should be the top.", top, 101);
        stack.popAll();
        assertTrue("the stack should be emoty", stack.isEmpty());
    }

    private static void populateStack(Stack<Integer> stack, int size){
        for(int idx = 0; idx < size; idx++){
            stack.push(idx);
        }
    }

    private static void populateQueue(Queue<Integer> stack, int size){
        for(int idx = 0; idx < size; idx++){
            stack.add(idx);
        }
    }    
}
