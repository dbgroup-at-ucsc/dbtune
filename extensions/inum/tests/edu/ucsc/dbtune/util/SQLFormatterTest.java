package edu.ucsc.dbtune.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class SQLFormatterTest
{
  private static final String FORMATTED_SHORT_QUERY = "\n"
      + "    Select\n"
      + "        * \n"
      + "    from\n"
      + "        lala \n"
      + "    where\n"
      + "        lala.ID == 1;";

  @Test public void testShortQueryFormat() throws Exception
 {
    final String shortQuery = "Select * from lala where lala.ID == 1;";
    final SqlFormatter formatter = new SqlFormatter(shortQuery);
    final String formatted = formatter.format();
    assertThat(formatted, equalTo(FORMATTED_SHORT_QUERY));
  }
}
