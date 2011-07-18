package edu.ucsc.dbtune.tools.cmudb.commons;

import Zql.ParseException;
import Zql.ZConstant;
import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZFromItem;
import Zql.ZGroupBy;
import Zql.ZOrderBy;
import Zql.ZQuery;
import Zql.ZSelectItem;
import Zql.ZUtils;
import Zql.ZqlParser;
import edu.ucsc.dbtune.tools.cmudb.model.ColumnsGatherer;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Feb 29, 2008
 * Time: 2:53:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class ZqlUtils {
    public static final HashSet AGGR_OPER = new HashSet();

    static {
        AGGR_OPER.add("AND");
        AGGR_OPER.add("OR");
        AGGR_OPER.add("NOT");

        prepareZqlParser();
    }


    public static final ZQuery addSchemaToTables(ZQuery query, String schema) {
        Vector fromVector = query.getFrom();
        Vector newVector = new Vector(fromVector.size());
        for (Iterator iterator = fromVector.iterator(); iterator.hasNext();) {
            ZFromItem item = (ZFromItem) iterator.next();
            if (item.getSchema() == null) {
                item = new ZFromItem(schema + "." + item.getTable() + (item.getAlias() != null ? " " + item.getAlias() : ""));
            }

            newVector.add(item);
        }

        ZExp whereClause = query.getWhere();
        ZExp exp = addSchemaToTables(whereClause, schema);

        ZQuery query1 = (ZQuery) Cloner.clone(query);
        query1.addWhere(exp);
        query1.addFrom(newVector);

        return query1;
    }

    public static final List getSelectColumns(ZQuery query) {
        Vector selects = query.getSelect();
        List columns = new ArrayList();
        for (Iterator iterator = selects.iterator(); iterator.hasNext();) {
            ZSelectItem item = (ZSelectItem) iterator.next();
            if(item.getAggregate() != null) {
                continue;
            }
            List c = getColumns(item.getExpression());
            if (!c.isEmpty())
                columns.addAll(c);
            else
                System.out.println("item = " + item);
        }

        return columns;
    }

    private static List getColumns(ZExp exp) {
        if (exp instanceof ZExpression) {
            ZExpression expression = (ZExpression) exp;
            Vector v = expression.getOperands();
            List retVals = new ArrayList();
            for (Iterator iterator = v.iterator(); iterator.hasNext();) {
                ZExp zExp = (ZExp) iterator.next();
                retVals.addAll(getColumns(zExp));
            }

            return retVals;
        } else if (exp instanceof ZConstant) {
            ZConstant constant = (ZConstant) exp;
            if (ZConstant.COLUMNNAME == constant.getType())
                return Arrays.asList(constant.getValue());
            else
                return Collections.EMPTY_LIST;
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    private static ZExp addSchemaToTables(ZExp exp, String schema) {
        if (exp instanceof ZExpression) {
            ZExpression expression = (ZExpression) exp;
            Vector operands = expression.getOperands();
            Vector newOperands = new Vector(operands.size());
            for (ListIterator iterator = operands.listIterator(); iterator.hasNext();) {
                ZExp operand = (ZExp) iterator.next();
                newOperands.add(addSchemaToTables(operand, schema));
            }

            ZExpression newExp = new ZExpression(expression.getOperator());
            newExp.setOperands(newOperands);

            return newExp;
        } else if (exp instanceof ZQuery) {
            return addSchemaToTables((ZQuery) exp, schema);
        } else
            return exp;
    }

    public static List<String> getUsedColumns(List orderBy) {
        if (orderBy == null) {
            return Collections.EMPTY_LIST;
        }

        Set orderByColumn = new LinkedHashSet();
        for (Iterator iterator = orderBy.iterator(); iterator.hasNext();) {
            ZOrderBy by = (ZOrderBy) iterator.next();
            _processExpression(by.getExpression(), orderByColumn, false);
        }

        return new ArrayList(orderByColumn);
    }

    public static List<String> getUsedColumns(ZExp exp, boolean followSubQuery) {
        Set<String> set = new HashSet();
        _processExpression(exp, set, followSubQuery);
        return new ArrayList(set);
    }

    private static void _processExpression(ZExp exp, Set set, boolean followSubQuery) {
        if (exp instanceof ZConstant) {
            ZConstant constant = (ZConstant) exp;
            if (constant.getType() == ZConstant.COLUMNNAME) {
                set.add(constant.getValue());
            }
        } else if (exp instanceof ZExpression) {
            ZExpression expression = (ZExpression) exp;
            List operands = expression.getOperands();
            for (int i = 0; i < operands.size(); i++) {
                ZExp zExp = (ZExp) operands.get(i);
                _processExpression(zExp, set, followSubQuery);
            }
        } else if (exp instanceof ZQuery) {
            if(followSubQuery)
                set.addAll(getUsedColumns((ZQuery) exp));
        }
    }

    public static Set<String> getUsedColumns(ZQuery query) {
        ColumnsGatherer gatherer = new ColumnsGatherer(query);
        Set columns = gatherer.getUsedColumns();
        Set set = new HashSet();
        for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
            String col = (String) iterator.next();
            set.add(col.toLowerCase());
        }
        return set;
    }

    public static ZQuery parseSQL(String sql) throws ParseException {
        ZqlParser parser = new ZqlParser(new ByteArrayInputStream((sql + ";").getBytes()));
        ZQuery query = (ZQuery) parser.readStatement();
        return query;
    }

    public static ZSelectItem getSelectItem(String selectString) throws ParseException {
        String str = "select " + selectString + " from dual";
        ZQuery query = ZqlUtils.parseSQL(str);
        return (ZSelectItem) query.getSelect().get(0);
    }

    public static ZExp parseExpression(String expression) throws ParseException {
        ZqlParser parser = new ZqlParser(new ByteArrayInputStream(expression.getBytes()));
        return parser.readExpression();
    }

    public static List getGroupByColumns(ZGroupBy groupBy) {
        if(groupBy  == null) {
            return Collections.EMPTY_LIST;
        }
        List<String> columns = new ArrayList();
        Vector v = groupBy.getGroupBy();
        for (int i = 0; i < v.size(); i++) {
            ZExp exp = (ZExp) v.elementAt(i);
            columns.add(exp.toString());
        }

        return columns;
    }

    public static ZConstant getNewColumn(String fieldName) {
        return new ZConstant(fieldName, ZConstant.COLUMNNAME);
    }

    public static void prepareZqlParser() {
        System.out.println("preparing the zql parser");
        ZUtils.addCustomFunction("to_char", 2);
        ZUtils.addCustomFunction("datepart", 2);
        ZUtils.addCustomFunction("dateadd", 3);
        ZUtils.addCustomFunction("substring", 3);
        ZUtils.addCustomFunction("power", 2);
        ZUtils.addCustomFunction("round", 2);
        ZUtils.addCustomFunction("cast", 1);
        ZUtils.addCustomFunction("abs", 1);
        ZUtils.addCustomFunction("cos", 1);
        ZUtils.addCustomFunction("sqrt", 1);
        ZUtils.addCustomFunction("Log10", 1);
        ZUtils.addCustomFunction("timestamp", 1);
        ZUtils.addCustomFunction("to_date", 1);
        ZUtils.addCustomFunction("year", 1);
        ZUtils.addCustomFunction("convert", 2);
    }
}
