/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.adaptor.impl;

import io.vertx.sqlclient.data.NullValue;

import java.sql.Types;

public class JdbcNull {
    private final int jdbcTypeCode;
    JdbcNull(int jdbcTypeCode) {
        this.jdbcTypeCode = jdbcTypeCode;
    }

    public NullValue toNullValue() {
        switch ( jdbcTypeCode ) {
            case Types.BOOLEAN:
            case Types.BIT: //we misuse BIT in H5
                return NullValue.Boolean;
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return NullValue.String;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                return NullValue.Double;
            case Types.BIGINT:
                return NullValue.Long;
            case Types.INTEGER:
                return NullValue.Integer;
            case Types.SMALLINT:
            case Types.TINYINT: //should really map to Byte
                return NullValue.Short;
            case Types.DECIMAL:
                return NullValue.BigDecimal;
            case Types.VARBINARY:
            case Types.BINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return NullValue.Buffer;
            case Types.TIMESTAMP:
                return NullValue.LocalDateTime;
            case Types.DATE:
                return NullValue.LocalDate;
            case Types.TIME:
                return NullValue.LocalTime;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return NullValue.OffsetDateTime;
            case Types.TIME_WITH_TIMEZONE:
                return NullValue.OffsetTime;
            default: return null;
        }
    }

    public int getJdbcTypeCode() {
        return jdbcTypeCode;
    }
}
