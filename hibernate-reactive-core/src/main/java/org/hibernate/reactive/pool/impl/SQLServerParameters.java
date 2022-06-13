/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.util.Locale;

public class SQLServerParameters extends Parameters {

    public static final SQLServerParameters INSTANCE = new SQLServerParameters();

    private SQLServerParameters() {
    }

    @Override
    public String process(String sql) {
        if (isProcessingNotRequired(sql)) {
            return sql;
        }
        return new Parser(sql).result();
    }

    @Override
    public String process(String sql, int parameterCount) {
        if (isProcessingNotRequired(sql)) {
            return sql;
        }
        return new Parser(sql, parameterCount).result();
    }

    /* Offset and Fetch gets applied just before the execution of the query but because we know
     * how the string looks like for Sql Server, it's faster to replace the last bit instead
     * of processing the whole query
     */
    @Override
    public String processLimit(String sql, Object[] parameterArray, boolean hasOffset) {
        if ( isProcessingNotRequired( sql ) ) {
            return sql;
        }

        // Replace 'offset ? fetch next ? rows only' with the @P style parameters for Sql Server
        int index = hasOffset ? parameterArray.length - 1 : parameterArray.length;
        int pos = sql.indexOf( " offset " );
        if ( pos > -1 ) {
            // The dialect doesn't use a parameter if the offset is 0
            String offsetQueryString = sql.contains( " offset 0 " )
                    ? " offset 0"
                    : " offset @P" + index++;
            String sqlProcessed = sql.substring( 0, pos ) + offsetQueryString + " rows";
            if ( sql.contains( " fetch next ?" ) ) {
                sqlProcessed += " fetch next @P" + index + " rows only ";
            }
            return sqlProcessed;
        }
        // Different Hibernate ORM versions may use different lowercase/uppercase letters
        if ( sql.toLowerCase( Locale.ROOT ).startsWith( "select top(?)" ) ) {
            // 13 is the length of the string "select top(?)"
            String sqlProcessed = "select top(@P" + index + ")" + sql.substring( 13 );
            shiftValues( parameterArray );
            return sqlProcessed;
        }
        return sql;
    }

    /**
     * Left shift all the values in the array (moving the first value to the end)
     */
    private void shiftValues(Object[] parameterArray) {
        Object temp = parameterArray[0];
        System.arraycopy( parameterArray, 1, parameterArray, 0, parameterArray.length - 1 );
        parameterArray[parameterArray.length - 1] = temp;
    }

    private static class Parser {

        private boolean inString;
        private boolean inQuoted;
        private boolean inSqlComment;
        private boolean inCComment;
        private boolean escaped;
        private int count = 0;
        private final StringBuilder result;
        private int previous;

        private Parser(String sql) {
            this(sql, 10);
        }

        private Parser(String sql, int parameterCount) {
            result = new StringBuilder(sql.length() + parameterCount);
            sql.codePoints().forEach(this::append);
        }

        private String result() {
            return result.toString();
        }

        private void append(int codePoint) {
            if (escaped) {
                escaped = false;
            } else {
                switch (codePoint) {
                    case '\\':
                        escaped = true;
                        break;
                    case '"':
                        if (!inString && !inSqlComment && !inCComment) inQuoted = !inQuoted;
                        break;
                    case '\'':
                        if (!inQuoted && !inSqlComment && !inCComment) inString = !inString;
                        break;
                    case '-':
                        if (!inQuoted && !inString && !inCComment && previous == '-') inSqlComment = true;
                        break;
                    case '\n':
                        inSqlComment = false;
                        break;
                    case '*':
                        if (!inQuoted && !inString && !inSqlComment && previous == '/') inCComment = true;
                        break;
                    case '/':
                        if (previous == '*') inCComment = false;
                        break;
                    //TODO: $$-quoted strings
                    case '?':
                        if (!inQuoted && !inString) {
                            result.append("@P").append(++count);
                            previous = '?';
                            return;
                        }
                }
            }
            previous = codePoint;
            result.appendCodePoint(codePoint);
        }
    }
}
