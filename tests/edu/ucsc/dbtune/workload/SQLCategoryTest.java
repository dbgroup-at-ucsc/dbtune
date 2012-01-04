package edu.ucsc.dbtune.workload;

import org.junit.Test;

import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UNKNOWN;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for SQLCategoryTest.
 *
 * @author Ivo Jimenez
 */
public class SQLCategoryTest
{
    /**
     */
    @Test
    public void testContains()
    {
        assertThat(SELECT.contains("S"), is(true));
        assertThat(SELECT.contains("U"), is(false));
        assertThat(SELECT.contains("I"), is(false));
        assertThat(SELECT.contains("D"), is(false));
        assertThat(SELECT.contains("?"), is(false));

        assertThat(DELETE.contains("S"), is(false));
        assertThat(DELETE.contains("U"), is(false));
        assertThat(DELETE.contains("I"), is(false));
        assertThat(DELETE.contains("D"), is(true));
        assertThat(DELETE.contains("?"), is(false));

        assertThat(UPDATE.contains("S"), is(false));
        assertThat(UPDATE.contains("U"), is(true));
        assertThat(UPDATE.contains("I"), is(false));
        assertThat(UPDATE.contains("D"), is(false));
        assertThat(UPDATE.contains("?"), is(false));

        assertThat(INSERT.contains("S"), is(false));
        assertThat(INSERT.contains("U"), is(false));
        assertThat(INSERT.contains("I"), is(true));
        assertThat(INSERT.contains("D"), is(false));
        assertThat(INSERT.contains("?"), is(false));

        assertThat(INSERT.contains("S"), is(false));
        assertThat(INSERT.contains("U"), is(false));
        assertThat(INSERT.contains("I"), is(true));
        assertThat(INSERT.contains("D"), is(false));
        assertThat(INSERT.contains("?"), is(false));

        assertThat(NOT_SELECT.contains("S"), is(false));
        assertThat(NOT_SELECT.contains("U"), is(true));
        assertThat(NOT_SELECT.contains("I"), is(true));
        assertThat(NOT_SELECT.contains("D"), is(true));
        assertThat(NOT_SELECT.contains("?"), is(true));

        assertThat(UNKNOWN.contains("S"), is(false));
        assertThat(UNKNOWN.contains("U"), is(false));
        assertThat(UNKNOWN.contains("I"), is(false));
        assertThat(UNKNOWN.contains("D"), is(false));
        assertThat(UNKNOWN.contains("?"), is(true));

        assertThat(SELECT.contains(SELECT), is(true));
        assertThat(SELECT.contains(UPDATE), is(false));
        assertThat(SELECT.contains(INSERT), is(false));
        assertThat(SELECT.contains(DELETE), is(false));
        assertThat(SELECT.contains(UNKNOWN), is(false));
        assertThat(SELECT.contains(NOT_SELECT), is(false));

        assertThat(UPDATE.contains(SELECT), is(false));
        assertThat(UPDATE.contains(UPDATE), is(true));
        assertThat(UPDATE.contains(INSERT), is(false));
        assertThat(UPDATE.contains(DELETE), is(false));
        assertThat(UPDATE.contains(UNKNOWN), is(false));
        assertThat(UPDATE.contains(NOT_SELECT), is(true));

        assertThat(INSERT.contains(SELECT), is(false));
        assertThat(INSERT.contains(UPDATE), is(false));
        assertThat(INSERT.contains(INSERT), is(true));
        assertThat(INSERT.contains(DELETE), is(false));
        assertThat(INSERT.contains(UNKNOWN), is(false));
        assertThat(INSERT.contains(NOT_SELECT), is(true));

        assertThat(DELETE.contains(SELECT), is(false));
        assertThat(DELETE.contains(UPDATE), is(false));
        assertThat(DELETE.contains(INSERT), is(false));
        assertThat(DELETE.contains(DELETE), is(true));
        assertThat(DELETE.contains(UNKNOWN), is(false));
        assertThat(DELETE.contains(NOT_SELECT), is(true));

        assertThat(UNKNOWN.contains(SELECT), is(false));
        assertThat(UNKNOWN.contains(UPDATE), is(false));
        assertThat(UNKNOWN.contains(INSERT), is(false));
        assertThat(UNKNOWN.contains(DELETE), is(false));
        assertThat(UNKNOWN.contains(UNKNOWN), is(true));
        assertThat(UNKNOWN.contains(NOT_SELECT), is(true));

        assertThat(NOT_SELECT.contains(SELECT), is(false));
        assertThat(NOT_SELECT.contains(UPDATE), is(true));
        assertThat(NOT_SELECT.contains(INSERT), is(true));
        assertThat(NOT_SELECT.contains(DELETE), is(true));
        assertThat(NOT_SELECT.contains(UNKNOWN), is(true));
        assertThat(NOT_SELECT.contains(NOT_SELECT), is(true));
    }

    /**
     */
    @Test
    public void testParsing()
    {
        assertThat(SQLCategory.from("S"), is(SQLCategory.SELECT));
        assertThat(SQLCategory.from("U"), is(SQLCategory.UPDATE));
        assertThat(SQLCategory.from("I"), is(SQLCategory.INSERT));
        assertThat(SQLCategory.from("D"), is(SQLCategory.DELETE));
        assertThat(SQLCategory.from("?"), is(SQLCategory.UNKNOWN));
        assertThat(SQLCategory.from("SELECT * from blah"), is(SQLCategory.SELECT));
        assertThat(SQLCategory.from("UPDATE blah SET foo = 0"), is(SQLCategory.UPDATE));
        assertThat(SQLCategory.from("INSERT INTO blah(foo) VALUES 0"), is(SQLCategory.INSERT));
    }

    /**
     */
    @Test
    public void testEquality()
    {
        assertThat(SELECT.isSame(SELECT), is(true));
        assertThat(SELECT.isSame(UPDATE), is(false));
        assertThat(SELECT.isSame(INSERT), is(false));
        assertThat(SELECT.isSame(DELETE), is(false));
        assertThat(SELECT.isSame(UNKNOWN), is(false));
        assertThat(SELECT.isSame(NOT_SELECT), is(false));

        assertThat(DELETE.isSame(SELECT), is(false));
        assertThat(DELETE.isSame(UPDATE), is(false));
        assertThat(DELETE.isSame(INSERT), is(false));
        assertThat(DELETE.isSame(DELETE), is(true));
        assertThat(DELETE.isSame(UNKNOWN), is(false));
        assertThat(DELETE.isSame(NOT_SELECT), is(true));

        assertThat(UPDATE.isSame(SELECT), is(false));
        assertThat(UPDATE.isSame(UPDATE), is(true));
        assertThat(UPDATE.isSame(INSERT), is(false));
        assertThat(UPDATE.isSame(DELETE), is(false));
        assertThat(UPDATE.isSame(UNKNOWN), is(false));
        assertThat(UPDATE.isSame(NOT_SELECT), is(true));

        assertThat(INSERT.isSame(SELECT), is(false));
        assertThat(INSERT.isSame(UPDATE), is(false));
        assertThat(INSERT.isSame(INSERT), is(true));
        assertThat(INSERT.isSame(DELETE), is(false));
        assertThat(INSERT.isSame(UNKNOWN), is(false));
        assertThat(INSERT.isSame(NOT_SELECT), is(true));

        assertThat(NOT_SELECT.isSame(SELECT), is(false));
        assertThat(NOT_SELECT.isSame(UPDATE), is(true));
        assertThat(NOT_SELECT.isSame(INSERT), is(true));
        assertThat(NOT_SELECT.isSame(DELETE), is(true));
        assertThat(NOT_SELECT.isSame(UNKNOWN), is(true));
        assertThat(NOT_SELECT.isSame(NOT_SELECT), is(true));

        assertThat(UNKNOWN.isSame(SELECT), is(false));
        assertThat(UNKNOWN.isSame(UPDATE), is(false));
        assertThat(UNKNOWN.isSame(INSERT), is(false));
        assertThat(UNKNOWN.isSame(DELETE), is(false));
        assertThat(UNKNOWN.isSame(UNKNOWN), is(true));
        assertThat(UNKNOWN.isSame(NOT_SELECT), is(true));
    }
}
