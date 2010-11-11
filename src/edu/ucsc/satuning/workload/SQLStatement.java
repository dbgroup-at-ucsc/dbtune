package edu.ucsc.satuning.workload;

import edu.ucsc.satuning.util.ToStringBuilder;

import java.util.Arrays;
import java.util.List;

/**
 * represents a {@code SQL statement}.
 */
public class SQLStatement {
	private final SQLCategory type;
	private final String sql;

    /**
     * construct a {@code SQLStatement} using a {@code SQLCategory} and the
     * actual {@code query statement}.
     * @param category
     *      a {@link SQLCategory} type.
     * @param query
     *      a sql query.
     */
	public SQLStatement(SQLCategory category, String query) {
		type    = category;
		sql     = query;
	}

    public SQLCategory getSQLCategory() {
        return type;
    }

    public String getSQL() {
        return sql;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<SQLStatement>(this)
               .add("type", getSQLCategory())
               .add("sql", getSQL())
               .toString();
    }

    /**
     * enum representing possible SQL categories.
     */
	public enum SQLCategory { 
        QUERY("S"),
        DML("I","U","D", "M", "UC", "DC"),
        OTHER();

        private final List<String> code;
        SQLCategory(String... code){
            this.code = Arrays.asList(code);
        }

        boolean contains(String code){
            return code.contains(code);
        }

        public static SQLCategory from(String code){
            for(SQLCategory each : values()){
                if(each.contains(code)){
                    return each;
                }
            }
            return OTHER;
        }

        @Override
        public String toString() {
            return code.toString();
        }
    }
}
