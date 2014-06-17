/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
 **********************************************************************/
package org.datanucleus.store.neodatis.query;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Map;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.InvokeExpression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.OrderExpression;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.query.expression.Expression.Operator;
import org.datanucleus.query.symbol.SymbolTable;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.neodatis.odb.core.query.criteria.ICriterion;
import org.neodatis.odb.core.query.criteria.Where;

/**
 * Class which maps a compiled query to a NeoDatis Criteria query.
 * Utilises the filter and ordering components of the java query and adds them to the underlying Criteria query. 
 * All other components are not handled here and instead processed by the in-memory evaluator.
 */
public class QueryToCriteriaMapper extends AbstractExpressionEvaluator
{
    String candidateAlias;

    /** Filter expression. */
    Expression filterExpr;

    /** Ordering expression(s). */
    Expression[] orderingExpr;

    /** Input parameters. */
    Map parameters;

    /** Symbol table for the compiled query. */
    SymbolTable symtbl;

    /** Criteria Query that we are updating. */
    org.neodatis.odb.impl.core.query.criteria.CriteriaQuery query;

    Deque stack = new ArrayDeque();

    /**
     * Constructor.
     * @param query Criteria query to be updated with the filter/ordering
     * @param compilation Generic query compilation
     * @param parameters Parameters needed
     */
    public QueryToCriteriaMapper(org.neodatis.odb.impl.core.query.criteria.CriteriaQuery query, 
            QueryCompilation compilation, Map parameters)
    {
        this.parameters = parameters;
        this.orderingExpr = compilation.getExprOrdering();
        this.query = query;
        this.filterExpr = compilation.getExprFilter();
        this.symtbl = compilation.getSymbolTable();
        this.candidateAlias = compilation.getCandidateAlias();
    }

    public void compile()
    {
        if (filterExpr != null)
        {
            // Define the filter on the Criteria query
            filterExpr.evaluate(this);
            if (!stack.isEmpty())
            {
                Object where = stack.pop();
                if (where instanceof ICriterion)
                {
                    query.setCriterion((ICriterion)where);
                    if (NucleusLogger.QUERY.isDebugEnabled())
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", "query.setCriterion(where);"));
                    }
                }
            }
            // TODO Should it be empty ever ? throw exception?
        }

        if (orderingExpr != null)
        {
            // Define the ordering on the Criteria query
            StringBuilder orderFieldsAsc = new StringBuilder();
            StringBuilder orderFieldsDesc = new StringBuilder();
            for (int i = 0; i < orderingExpr.length; i++)
            {
                String orderFieldName = ((PrimaryExpression)orderingExpr[i].getLeft()).getId();

                if (((OrderExpression)orderingExpr[i]).getSortOrder() == null ||
                    ((OrderExpression)orderingExpr[i]).getSortOrder().equals("ascending"))
                {
                    if (orderFieldsAsc.length() > 0)
                    {
                        orderFieldsAsc.append(',');
                    }
                    orderFieldsAsc.append(orderFieldName);
                }
                else
                {
                    if (orderFieldsDesc.length() > 0)
                    {
                        orderFieldsDesc.append(',');
                    }
                    orderFieldsDesc.append(orderFieldName);
                }
            }
            // TODO In NeoDatis Criteria you cannot do mixed ordering "field1 ASC, field2 DESC"
            if (orderFieldsAsc.length() > 0)
            {
                query.orderByAsc(orderFieldsAsc.toString());
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria",
                        "query.orderByAsc(" + orderFieldsAsc.toString() + ");"));
                }
            }
            else if (orderFieldsDesc.length() > 0)
            {
                query.orderByDesc(orderFieldsDesc.toString());
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria",
                        "query.orderByDesc(" + orderFieldsDesc.toString() + ");"));
                }
            }
        }

        if (this.symtbl.hasSymbol(candidateAlias))
        {
            // Query has a set of candidates defined for it
            if (parameters != null && parameters.get(candidateAlias) != null &&
                parameters.get(candidateAlias) instanceof Collection)
            {
                Collection candidates = (Collection) parameters.get(candidateAlias);
                if (candidates.isEmpty())
                {
                    // TODO how to exclude all objects in Criteria?
                }
                else
                {
                    // TODO Implement this
                }
            }
        }
    }

    /**
     * Method to process the supplied OR expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processOrExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        Object boolExpr;

        if (left instanceof ICriterion && right instanceof ICriterion)
        {
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                    "Where.or().add(" + left + ").add(" + right + ")"));
            }
            boolExpr = Where.or().add((ICriterion)left).add((ICriterion)right);
        }
        else
        {
            if (left == Boolean.TRUE || right == Boolean.TRUE)
            {
                stack.push(Boolean.TRUE);
                return Boolean.TRUE;
            }

            stack.push(Boolean.FALSE);
            return Boolean.FALSE;
        }
        stack.push(boolExpr);
        return stack.peek();
    }

    /**
     * Method to process the supplied AND expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processAndExpression(Expression expr)
    {
        final Object right = stack.pop();
        final Object left = stack.pop();
        Object boolExpr;

        if (left instanceof ICriterion && right instanceof ICriterion)
        {
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                    "Where.and().add(" + left + ").add(" + right + ")"));
            }
            boolExpr = Where.and().add((ICriterion)left).add((ICriterion)right);
        }
        else
        {
            if (left == right && left == Boolean.TRUE)
            {
                stack.push(Boolean.TRUE);
                return Boolean.TRUE;
            }

            stack.push(Boolean.FALSE);
            return Boolean.FALSE;
        }
        stack.push(boolExpr);
        return stack.peek();
    }

    /**
     * Method to process the supplied EQ expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processEqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        Object boolExpr;
        if (left instanceof PrimaryExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_EQ, (PrimaryExpression)left, 
                ((Literal)right).getLiteral());
        }
        else if (right instanceof PrimaryExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_EQ, 
                (PrimaryExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof PrimaryExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_EQ, 
                (PrimaryExpression)left, right);
        }
        else if (right instanceof PrimaryExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_EQ, 
                (PrimaryExpression)right, left);
        }
        else
        {
            // TODO What of other combos?
            boolExpr = left.equals(right) ? Boolean.TRUE : Boolean.FALSE;
        }

        stack.push(boolExpr);
        return stack.peek();
    }

    /**
     * Method to process the supplied NOTEQ expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processNoteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        Object boolExpr;
        if (left instanceof PrimaryExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_NOTEQ, 
                (PrimaryExpression)left, ((Literal)right).getLiteral());
        }
        else if (right instanceof PrimaryExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_NOTEQ, 
                (PrimaryExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof PrimaryExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_NOTEQ, 
                (PrimaryExpression)left, right);
        }
        else if (right instanceof PrimaryExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_NOTEQ, 
                (PrimaryExpression)right, left);
        }
        else
        {
            // TODO What of other combos?
            boolExpr = left.equals(right) ? Boolean.FALSE : Boolean.TRUE;
        }

        stack.push(boolExpr);
        return stack.peek();
    }

    /**
     * Method to process the supplied GT expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processGtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        Object boolExpr;
        if (left instanceof PrimaryExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_GT, 
                (PrimaryExpression)left, ((Literal)right).getLiteral());
        }
        else if (right instanceof PrimaryExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_LTEQ, 
                (PrimaryExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof InvokeExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_GT, 
                (InvokeExpression)left, ((Literal)right).getLiteral());
        }
        else if (right instanceof InvokeExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_LTEQ, 
                (InvokeExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof PrimaryExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_GT, 
                (PrimaryExpression)left, right);
        }
        else if (right instanceof PrimaryExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_LTEQ, 
                (PrimaryExpression)right, left);
        }
        else if (left instanceof InvokeExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_GT, 
                (InvokeExpression)left, right);
        }
        else if (right instanceof InvokeExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_LTEQ, 
                (InvokeExpression)right, left);
        }
        else
        {
            // TODO What of other combos?
            boolExpr = left.equals(right) ? Boolean.FALSE : Boolean.TRUE;
        }

        stack.push(boolExpr);
        return stack.peek();
    }

    /**
     * Method to process the supplied LT expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processLtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        Object boolExpr;
        if (left instanceof PrimaryExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_LT, 
                (PrimaryExpression)left, ((Literal)right).getLiteral());
        }
        else if (right instanceof PrimaryExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_GTEQ, 
                (PrimaryExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof InvokeExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_LT, 
                (InvokeExpression)left, ((Literal)right).getLiteral());
        }
        else if (right instanceof InvokeExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_GTEQ, 
                (InvokeExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof PrimaryExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_LT, 
                (PrimaryExpression)left, right);
        }
        else if (right instanceof PrimaryExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_GTEQ, 
                (PrimaryExpression)right, left);
        }
        else if (left instanceof InvokeExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_LT, 
                (InvokeExpression)left, right);
        }
        else if (right instanceof InvokeExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_GTEQ, 
                (InvokeExpression)right, left);
        }
        else
        {
            // TODO What of other combos?
            boolExpr = left.equals(right) ? Boolean.FALSE : Boolean.TRUE;
        }

        stack.push(boolExpr);
        return stack.peek();
    }

    /**
     * Method to process the supplied GTEQ expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processGteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        Object boolExpr;
        if (left instanceof PrimaryExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_GTEQ, 
                (PrimaryExpression)left, ((Literal)right).getLiteral());
        }
        else if (right instanceof PrimaryExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_LT, 
                (PrimaryExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof InvokeExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_GTEQ, 
                (InvokeExpression)left, ((Literal)right).getLiteral());
        }
        else if (right instanceof InvokeExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_LT, 
                (InvokeExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof PrimaryExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_GTEQ, 
                (PrimaryExpression)left, right);
        }
        else if (right instanceof PrimaryExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_LT, 
                (PrimaryExpression)right, left);
        }
        else if (left instanceof InvokeExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_GTEQ, 
                (InvokeExpression)left, right);
        }
        else if (right instanceof InvokeExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_LT, 
                (InvokeExpression)right, left);
        }
        else
        {
            // TODO What of other combos?
            boolExpr = left.equals(right) ? Boolean.FALSE : Boolean.TRUE;
        }

        stack.push(boolExpr);
        return stack.peek();
    }

    /**
     * Method to process the supplied LTEQ expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processLteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        Object boolExpr;
        if (left instanceof PrimaryExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_LTEQ, 
                (PrimaryExpression)left, ((Literal)right).getLiteral());
        }
        else if (right instanceof PrimaryExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_GT, 
                (PrimaryExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof InvokeExpression && right instanceof Literal)
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_LTEQ, 
                (InvokeExpression)left, ((Literal)right).getLiteral());
        }
        else if (right instanceof InvokeExpression && left instanceof Literal)
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_GT, 
                (InvokeExpression)right, ((Literal)left).getLiteral());
        }
        else if (left instanceof PrimaryExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_LTEQ, 
                (PrimaryExpression)left, right);
        }
        else if (right instanceof PrimaryExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForPrimaryLiteralValue(Expression.OP_GT, 
                (PrimaryExpression)right, left);
        }
        else if (left instanceof InvokeExpression && !(right instanceof Expression))
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_LTEQ, 
                (InvokeExpression)left, right);
        }
        else if (right instanceof InvokeExpression && !(left instanceof Expression))
        {
            boolExpr = getRelationalExprForInvokeLiteralValue(Expression.OP_GT, 
                (InvokeExpression)right, left);
        }
        else
        {
            // TODO What of other combos?
            boolExpr = left.equals(right) ? Boolean.FALSE : Boolean.TRUE;
        }

        stack.push(boolExpr);
        return stack.peek();
    }

    /**
     * Method to process the supplied primary expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processPrimaryExpression(PrimaryExpression expr)
    {
        Object value = (parameters != null ? parameters.get(expr.getId()) : null);
        if (value != null)
        {
            // Parameter/Variable
            stack.push(value);
            return value;
        }

        // Just push the PrimaryExpression on to the stack and handle it elsewhere since we likely need the whole list of tuples to specify the Where
        stack.push(expr);
        return expr;
    }

    /**
     * Method to process the supplied parameter expression.
     * @param expr The expression
     * @return The result
     */
    protected Object processParameterExpression(ParameterExpression expr)
    {
        Object value = QueryUtils.getValueForParameterExpression(parameters, expr);
        stack.push(value);
        return value;
    }

    /**
     * Method to process the supplied invoke expression.
     * To be implemented by subclasses.
     * @param invokeExpr The invocation expression
     * @return The result
     */
    protected Object processInvokeExpression(InvokeExpression invokeExpr)
    {
        Expression invokedExpr = invokeExpr.getLeft();
        String method = invokeExpr.getOperation();
        if (invokedExpr instanceof PrimaryExpression)
        {
            String field = ((PrimaryExpression)invokedExpr).getId();
            if (method.equals("startsWith"))
            {
                // TODO Check that the field is a String
                Literal param = (Literal)invokeExpr.getArguments().get(0);
                String arg = null;
                if (param.getLiteral() instanceof String)
                {
                    arg = (String)param.getLiteral();
                }
                else if (param.getLiteral() instanceof Character)
                {
                    arg = ((Character)param.getLiteral()).toString();
                }
                else if (param.getLiteral() instanceof Number)
                {
                    arg = ((Number)param.getLiteral()).toString();
                }
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                        "Where.like(" + field + ", '" + arg + "%')"));
                }
                Object boolExpr = Where.like(field, arg + "%");
                stack.push(boolExpr);
                return boolExpr;
            }
            else if (method.equals("endsWith"))
            {
                // TODO Check that the field is a String
                Literal param = (Literal)invokeExpr.getArguments().get(0);
                String arg = null;
                if (param.getLiteral() instanceof String)
                {
                    arg = (String)param.getLiteral();
                }
                else if (param.getLiteral() instanceof Character)
                {
                    arg = ((Character)param.getLiteral()).toString();
                }
                else if (param.getLiteral() instanceof Number)
                {
                    arg = ((Number)param.getLiteral()).toString();
                }
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                        "Where.like(" + field + ", '%" + arg + "')"));
                }
                Object boolExpr = Where.like(field, "%" + arg);
                stack.push(boolExpr);
                return boolExpr;
            }
            else if (method.equals("size"))
            {
                // collectionField.size(). Just return and process as a relational expression
                // TODO Check that the field is a Collection/Map
                stack.push(invokeExpr);
                return invokeExpr;
            }
            else if (method.equals("contains"))
            {
                // Support contains(Object)
                // TODO Check that the field is a Collection/Map
                Object obj = invokeExpr.getArguments().get(0);
                if (obj instanceof Literal)
                {
                    Object elementValue = ((Literal)obj).getLiteral();
                    Object boolExpr = Where.contain(field, elementValue);
                    stack.push(boolExpr);
                    return boolExpr;
                }

                throw new NucleusException("Method " + method + 
                        " is only currently supported with a literal argument for JDOQL with NeoDatis");
            }
            else if (method.equals("isEmpty"))
            {
                // Support isEmpty()
                // TODO Check that the field is a Collection/Map
                Object boolExpr = Where.sizeEq(field, 0);
                stack.push(boolExpr);
                return boolExpr;
            }
            else
            {
                // TODO Implement all other methods
                throw new NucleusException("Method " + method + 
                " is not currently supported with JDOQL for NeoDatis");
            }
        }

        throw new NucleusException("Attempt to invoke " + method + " on " + invokedExpr + " but this is not currently supported by NeoDatis JDOQL");
    }

    /**
     * Method to process the supplied invoke expression.
     * To be implemented by subclasses.
     * @param expr The expression
     * @return The result
     */
    protected Object processLiteral(Literal expr)
    {
        // Just return the Literal and process it in a relational expression
        stack.push(expr);
        return expr;
    }

    /**
     * Convenience method that takes a PrimaryExpression and returns the path to apply to the
     * query in terms of a "descend" argument.
     * @param expr The PrimaryExpression
     * @return The path
     */
    private String getPathForPrimaryExpression(PrimaryExpression expr)
    {
        // Find the path to apply. This currently only caters for direct fields, and fields of the candidate
        // TODO Cater for other candidate aliases from JPQL "FROM" clause
        String firstTuple = expr.getTuples().iterator().next();
        String exprPath = expr.getId();
        if (firstTuple.equals(candidateAlias))
        {
            exprPath = exprPath.substring(candidateAlias.length()+1);
        }
        return exprPath;
    }

    /**
     * Convenience method to convert a relational expression of "field {operator} value" into
     * the equivalent Criteria query. Supports OP_GT, OP_LT, OP_GTEQ, OP_LTEQ, OP_EQ, OP_NOTEQ.
     * @param op operator
     * @param fieldPrimary the primary (field) expression
     * @param literalValue The literal to compare with
     * @return The WHERE criterion
     */
    private Object getRelationalExprForPrimaryLiteralValue(Operator op, PrimaryExpression fieldPrimary, 
            Object literalValue)
    {
        Object boolExpr = null;

        String fieldPath = getPathForPrimaryExpression(fieldPrimary);
        // TODO NeoDatis Criteria 1.9.0-beta-2 doesn't do type conversion so if a field is "int"
        // and our literal is a "long" then it gives ClassCastException!
        if (literalValue instanceof Integer)
        {
            int intVal = ((Integer)literalValue).intValue();
            if (op == Expression.OP_GT)
            {
                boolExpr = Where.gt(fieldPath, intVal);
            }
            else if (op == Expression.OP_LT)
            {
                boolExpr = Where.lt(fieldPath, intVal);
            }
            else if (op == Expression.OP_GTEQ)
            {
                boolExpr = Where.ge(fieldPath, intVal);
            }
            else if (op == Expression.OP_LTEQ)
            {
                boolExpr = Where.le(fieldPath, intVal);
            }
            else if (op == Expression.OP_EQ)
            {
                boolExpr = Where.equal(fieldPath, intVal);
            }
            else if (op == Expression.OP_NOTEQ)
            {
                boolExpr = Where.not(Where.equal(fieldPath, intVal));
            }
        }
        else if (literalValue instanceof Long)
        {
            long longVal = ((Long)literalValue).longValue();
            if (op == Expression.OP_GT)
            {
                boolExpr = Where.gt(fieldPath, longVal);
            }
            else if (op == Expression.OP_LT)
            {
                boolExpr = Where.lt(fieldPath, longVal);
            }
            else if (op == Expression.OP_GTEQ)
            {
                boolExpr = Where.ge(fieldPath, longVal);
            }
            else if (op == Expression.OP_LTEQ)
            {
                boolExpr = Where.le(fieldPath, longVal);
            }
            else if (op == Expression.OP_EQ)
            {
                boolExpr = Where.equal(fieldPath, longVal);
            }
            else if (op == Expression.OP_NOTEQ)
            {
                boolExpr = Where.not(Where.equal(fieldPath, longVal));
            }
        }
        else if (literalValue instanceof Short)
        {
            short shortVal = ((Short)literalValue).shortValue();
            if (op == Expression.OP_GT)
            {
                boolExpr = Where.gt(fieldPath, shortVal);
            }
            else if (op == Expression.OP_LT)
            {
                boolExpr = Where.lt(fieldPath, shortVal);
            }
            else if (op == Expression.OP_GTEQ)
            {
                boolExpr = Where.ge(fieldPath, shortVal);
            }
            else if (op == Expression.OP_LTEQ)
            {
                boolExpr = Where.le(fieldPath, shortVal);
            }
            else if (op == Expression.OP_EQ)
            {
                boolExpr = Where.equal(fieldPath, shortVal);
            }
            else if (op == Expression.OP_NOTEQ)
            {
                boolExpr = Where.not(Where.equal(fieldPath, shortVal));
            }
        }
        else if (literalValue instanceof Double)
        {
            double doubleVal = ((Double)literalValue).doubleValue();
            if (op == Expression.OP_GT)
            {
                boolExpr = Where.gt(fieldPath, doubleVal);
            }
            else if (op == Expression.OP_LT)
            {
                boolExpr = Where.lt(fieldPath, doubleVal);
            }
            else if (op == Expression.OP_GTEQ)
            {
                boolExpr = Where.ge(fieldPath, doubleVal);
            }
            else if (op == Expression.OP_LTEQ)
            {
                boolExpr = Where.le(fieldPath, doubleVal);
            }
            else if (op == Expression.OP_EQ)
            {
                boolExpr = Where.equal(fieldPath, doubleVal);
            }
            else if (op == Expression.OP_NOTEQ)
            {
                boolExpr = Where.not(Where.equal(fieldPath, doubleVal));
            }
        }
        else if (literalValue instanceof Float)
        {
            float floatVal = ((Float)literalValue).floatValue();
            if (op == Expression.OP_GT)
            {
                boolExpr = Where.gt(fieldPath, floatVal);
            }
            else if (op == Expression.OP_LT)
            {
                boolExpr = Where.lt(fieldPath, floatVal);
            }
            else if (op == Expression.OP_GTEQ)
            {
                boolExpr = Where.ge(fieldPath, floatVal);
            }
            else if (op == Expression.OP_LTEQ)
            {
                boolExpr = Where.le(fieldPath, floatVal);
            }
            else if (op == Expression.OP_EQ)
            {
                boolExpr = Where.equal(fieldPath, floatVal);
            }
            else if (op == Expression.OP_NOTEQ)
            {
                boolExpr = Where.not(Where.equal(fieldPath, floatVal));
            }
        }
        else if (literalValue instanceof Byte)
        {
            byte byteVal = ((Byte)literalValue).byteValue();
            if (op == Expression.OP_GT)
            {
                boolExpr = Where.gt(fieldPath, byteVal);
            }
            else if (op == Expression.OP_LT)
            {
                boolExpr = Where.lt(fieldPath, byteVal);
            }
            else if (op == Expression.OP_GTEQ)
            {
                boolExpr = Where.ge(fieldPath, byteVal);
            }
            else if (op == Expression.OP_LTEQ)
            {
                boolExpr = Where.le(fieldPath, byteVal);
            }
            else if (op == Expression.OP_EQ)
            {
                boolExpr = Where.equal(fieldPath, byteVal);
            }
            else if (op == Expression.OP_NOTEQ)
            {
                boolExpr = Where.not(Where.equal(fieldPath, byteVal));
            }
        }
        else if (literalValue instanceof Character)
        {
            char charVal = ((Character)literalValue).charValue();
            if (op == Expression.OP_GT)
            {
                boolExpr = Where.gt(fieldPath, charVal);
            }
            else if (op == Expression.OP_LT)
            {
                boolExpr = Where.lt(fieldPath, charVal);
            }
            else if (op == Expression.OP_GTEQ)
            {
                boolExpr = Where.ge(fieldPath, charVal);
            }
            else if (op == Expression.OP_LTEQ)
            {
                boolExpr = Where.le(fieldPath, charVal);
            }
            else if (op == Expression.OP_EQ)
            {
                boolExpr = Where.equal(fieldPath, charVal);
            }
            else if (op == Expression.OP_NOTEQ)
            {
                boolExpr = Where.not(Where.equal(fieldPath, charVal));
            }
        }
        else if (literalValue instanceof Comparable)
        {
            if (op == Expression.OP_GT)
            {
                boolExpr = Where.gt(fieldPath, (Comparable)literalValue);
            }
            else if (op == Expression.OP_LT)
            {
                boolExpr = Where.lt(fieldPath, (Comparable)literalValue);
            }
            else if (op == Expression.OP_GTEQ)
            {
                boolExpr = Where.ge(fieldPath, (Comparable)literalValue);
            }
            else if (op == Expression.OP_LTEQ)
            {
                boolExpr = Where.le(fieldPath, (Comparable)literalValue);
            }
            else if (op == Expression.OP_EQ)
            {
                boolExpr = Where.equal(fieldPath, literalValue);
            }
            else if (op == Expression.OP_NOTEQ)
            {
                boolExpr = Where.not(Where.equal(fieldPath, literalValue));
            }
        }
        else
        {
            if (op == Expression.OP_EQ)
            {
                boolExpr = Where.equal(fieldPath, literalValue);
            }
            else if (op == Expression.OP_NOTEQ)
            {
                boolExpr = Where.not(Where.equal(fieldPath, literalValue));
            }
            else
            {
                throw new NucleusException(
                    "Query includes " + fieldPath + " " + op + " " + literalValue +
                    " where the value is not of a supported NeoDatis query type for this relational operation");
            }
        }

        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            if (op == Expression.OP_GT)
            {
                NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                    "Where.gt(" + fieldPath + ", " + literalValue + ")"));
            }
            else if (op == Expression.OP_LT)
            {
                NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                    "Where.lt(" + fieldPath + ", " + literalValue + ")"));
            }
            else if (op == Expression.OP_GTEQ)
            {
                NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                    "Where.ge(" + fieldPath + ", " + literalValue + ")"));
            }
            else if (op == Expression.OP_LTEQ)
            {
                NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                    "Where.le(" + fieldPath + ", " + literalValue + ")"));
            }
            else if (op == Expression.OP_EQ)
            {
                NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                    "Where.equal(" + fieldPath + ", " + literalValue + ")"));
            }
            else if (op == Expression.OP_NOTEQ)
            {
                NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                    "Where.not(Where.equal(" + fieldPath + ", " + literalValue + "))"));
            }
        }
        if (boolExpr == null)
        {
            throw new NucleusException("Unable to convert " + fieldPath + " " + op + " " + literalValue +
                " into Criteria query constraint");
        }

        return boolExpr;
    }

    /**
     * Convenience method to convert a relational expression of "fieldPath.method() {operator} value" into
     * the equivalent Criteria query. Supports OP_GT, OP_LT, OP_GTEQ, OP_LTEQ, OP_EQ, OP_NOTEQ.
     * @param op operator
     * @param invokeExpr the invoke (field) expression
     * @param literalValue The value to compare with
     * @return The WHERE criterion
     */
    private Object getRelationalExprForInvokeLiteralValue(Operator op, InvokeExpression invokeExpr, 
            Object literalValue)
    {
        Object boolExpr = null;

        Expression invokedExpr = invokeExpr.getLeft();
        if (invokedExpr instanceof PrimaryExpression)
        {
            String field = ((PrimaryExpression)invokedExpr).getId();
            if (invokeExpr.getOperation().equals("size"))
            {
                // TODO Check if collection/map field
                int size = 0;
                if (literalValue instanceof Number)
                {
                    size = ((Integer)literalValue).intValue();
                }

                if (op == Expression.OP_GT)
                {
                    boolExpr = Where.sizeGt(field, size);
                }
                else if (op == Expression.OP_LT)
                {
                    boolExpr = Where.sizeLt(field, size);
                }
                else if (op == Expression.OP_GTEQ)
                {
                    boolExpr = Where.sizeLt(field, size);
                }
                else if (op == Expression.OP_LTEQ)
                {
                    boolExpr = Where.sizeLt(field, size);
                }
                else if (op == Expression.OP_EQ)
                {
                    boolExpr = Where.sizeEq(field, size);
                }
                else if (op == Expression.OP_NOTEQ)
                {
                    boolExpr = Where.sizeNe(field, size);
                }

                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    if (op == Expression.OP_GT)
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                            "Where.sizeGt(" + field + ", " + literalValue + ")"));
                    }
                    else if (op == Expression.OP_LT)
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                            "Where.sizeLt(" + field + ", " + literalValue + ")"));
                    }
                    else if (op == Expression.OP_GTEQ)
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                            "Where.sizeGe(" + field + ", " + literalValue + ")"));
                    }
                    else if (op == Expression.OP_LTEQ)
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                            "Where.sizeLe(" + field + ", " + literalValue + ")"));
                    }
                    else if (op == Expression.OP_EQ)
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria",
                            "Where.sizeEq(" + field + ", " + literalValue + ")"));
                    }
                    else if (op == Expression.OP_NOTEQ)
                    {
                        NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria", 
                            "Where.sizeNe(" + field + ", " + literalValue + ")"));
                    }
                }
            }
            else
            {
                throw new NucleusException("Attempt to invoke method " + invokeExpr.getOperation() +
                    " on field " + field + " but not currently supported by NeoDatis JDOQL");
            }

            if (boolExpr == null)
            {
                throw new NucleusException("Unable to convert " + field + " " + op + " " + literalValue +
                " into Criteria query constraint");
            }
            return boolExpr;
        }

        throw new NucleusException("Attempt to invoke " + invokeExpr.getOperation() + " on " +
                invokedExpr + " but this is not currently supported by NeoDatis JDOQL");
    }
}