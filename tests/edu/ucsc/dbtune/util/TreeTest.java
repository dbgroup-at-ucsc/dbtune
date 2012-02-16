package edu.ucsc.dbtune.util;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class TreeTest
{
    /**
     *
     */
    @Test
    public void testBasicUsage()
    {
        Tree<String> tree = new Tree<String>("A");

        tree.setChild("A", "B");
        tree.setChild("A", "C");
        tree.setChild("A", "D");
        tree.setChild("B", "E");
        tree.setChild("B", "F");
        tree.setChild("B", "G");
        tree.setChild("B", "H");
        tree.setChild("C", "I");
        tree.setChild("D", "J");
        tree.setChild("E", "K");
        tree.setChild("E", "L");
        tree.setChild("E", "M");
        tree.setChild("F", "N");
        tree.setChild("G", "O");
        tree.setChild("G", "P");
        tree.setChild("H", "Q");
        tree.setChild("H", "R");
        tree.setChild("H", "S");
        tree.setChild("H", "T");

        assertThat(tree.contains("A"), is(true));
        assertThat(tree.contains("B"), is(true));
        assertThat(tree.contains("C"), is(true));
        assertThat(tree.contains("D"), is(true));
        assertThat(tree.contains("E"), is(true));
        assertThat(tree.contains("F"), is(true));
        assertThat(tree.contains("G"), is(true));
        assertThat(tree.contains("H"), is(true));
        assertThat(tree.contains("I"), is(true));
        assertThat(tree.contains("J"), is(true));
        assertThat(tree.contains("K"), is(true));
        assertThat(tree.contains("L"), is(true));
        assertThat(tree.contains("M"), is(true));
        assertThat(tree.contains("N"), is(true));
        assertThat(tree.contains("O"), is(true));
        assertThat(tree.contains("P"), is(true));
        assertThat(tree.contains("Q"), is(true));
        assertThat(tree.contains("R"), is(true));
        assertThat(tree.contains("S"), is(true));
        assertThat(tree.contains("T"), is(true));

        assertThat(tree.size(), is(20));

        assertThat(tree.getRootElement(), is("A"));

        String root = tree.getRootElement();

        assertThat(tree.getChildren(root).size(), is(3));
        assertThat(tree.getChildren(root).contains("B"), is(true));
        assertThat(tree.getChildren(root).contains("C"), is(true));
        assertThat(tree.getChildren(root).contains("D"), is(true));

        assertThat(tree.getParent("S"), is("H"));

        Tree<String> copy = new Tree<String>(tree);

        assertThat(tree.size(), is(copy.size()));
        assertThat(tree.getRootElement(), is(copy.getRootElement()));

        copy.remove("B");

        assertThat(copy.size(), is(tree.size() - 15));
    }
}
