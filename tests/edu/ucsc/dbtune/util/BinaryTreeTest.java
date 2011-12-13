package edu.ucsc.dbtune.util;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static edu.ucsc.dbtune.util.BinaryTree.LEFT;
import static edu.ucsc.dbtune.util.BinaryTree.RIGHT;

/**
 * @author Ivo Jimenez (ivo@cs.ucsc.edu.com)
 */
public class BinaryTreeTest {
    @Test
    public void testBasicUsage() {
        BinaryTree<String> tree = new BinaryTree<String>("F");

        assertThat(tree.getRootElement(), is("F"));
        assertThat(tree.contains("F"),    is(true));
        assertThat(tree.contains("B"),    is(false));
        assertThat(tree.contains("G"),    is(false));
        assertThat(tree.size(),           is(1));

        tree.setChild("F", "B", LEFT);

        assertThat(tree.size(), is(2));

        tree.setChild("F", "G", RIGHT);

        assertThat(tree.size(),        is(3));
        assertThat(tree.contains("B"), is(true));
        assertThat(tree.contains("G"), is(true));

        try {
            tree.setChild("B", "G", LEFT);
            fail("No exception caught");
        } catch(IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Child value already in tree"));
        }

        try {
            tree.setChild("F", "C", LEFT);
            fail("No exception caught");
        } catch(IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Parent already has child"));
        }
    }
}
