package edu.ucsc.dbtune.workload;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Enum representing possible SQL categories.
 *
 * @author Karl Schnaitter
 * @author Huascar Sanchez
 * @author Ivo Jimenez
 */
public enum SQLCategory
{
    SELECT("S"),
    INSERT("I"),
    UPDATE("U"),
    DELETE("D"),
    /**
     * Convenience element that represents all DML statements but {@link #SELECT}
     */
    NOT_SELECT("I", "U", "D");

    /** codes corresponding to the category */
    private final List<String> code;

    /**
     * Creates a new category with the given code-names.
     *
     * @param code
     *     one or more strings containing the codes of the category
     */
    SQLCategory(String... code)
    {
        this.code = Arrays.asList(code);
    }

    /**
     * Checks whether or not the given category is part of another. For aggregated categories (eg. 
     * NOT_SELECT), a category defines broader categories.
     *
     * @param category
     *     the category that is checked against this one.
     * @return
     *     {@code true} if the category contains the given {@code category}; {@code false} 
     *     otherwise.
     */
    boolean contains(SQLCategory category)
    {
        for (String id : category.code) {
            if (contains(id)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether or not the given code is part of this category.
     *
     * @param code
     *     the code that is checked against the list of codes contained in {@code this}.
     * @return
     *     {@code true} if the category contains the given {@code code}; {@code false} otherwise.
     */
    boolean contains(String code)
    {
        return this.code.contains(code);
    }

    /**
     * Returns the category that contains the given code or, if the string is a SQL statement, the 
     * category that corresponds to it.
     *
     * @param codeOrSQL
     *     code used to look for a category; this can also be a SQL statement.
     * @return
     *     the category whose one of its codes matches the given {@code code}
     * @throws SQLException
     *     if no category can't be extracted from the given string.
     */
    public static SQLCategory from(String codeOrSQL) throws SQLException
    {
        if (codeOrSQL.length() == 1)
            for (SQLCategory category : values())
                if (category.contains(codeOrSQL))
                    return category;

        String sql = codeOrSQL.trim().toLowerCase();

        if (sql.startsWith("select") || sql.startsWith("with")) {
            return SQLCategory.SELECT;
        } else if (sql.startsWith("update")) {
            return SQLCategory.UPDATE;
        } else if (sql.startsWith("insert")) {
            return SQLCategory.INSERT;
        } else if (sql.startsWith("delete")) {
            return SQLCategory.DELETE;
        } else {
            throw new SQLException("Can't determine category for " + codeOrSQL);
        }
    }

    /**
     * Compares another category against this one.
     *
     * @param that
     *     other statement to compare against this
     * @return
     *     {@code true} if the given category is the same as {@code this} one; {@code false} 
     *     otherwise.
     */
    public boolean isSame(SQLCategory that)
    {
        return this == that || this.contains(that) || that.contains(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        if (this.isSame(SELECT)) {
            return "SELECT";
        } else if (this.isSame(SELECT)) {
            return "UPDATE";
        } else if (this.isSame(SELECT)) {
            return "INSERT";
        } else if (this.isSame(SELECT)) {
            return "DELETE";
        } else {
            return "UNKNOWN";
        }
    }
}
