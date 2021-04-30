/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

public class SQLServerParameters extends Parameters {

    static final SQLServerParameters INSTANCE = new SQLServerParameters();

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

    @Override
    public String processLimit(String sql, Object[] parameterArray, boolean hasOffset) {
        return sql;
    }

    private static class Parser {

        private boolean inString;
        private boolean inQuoted;
        private boolean inSqlComment;
        private boolean inCComment;
        private boolean escaped;
        private int count = 0;
        private StringBuilder result;
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
