/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.type.descriptor.jdbc;

import java.sql.Types;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.tool.schema.extract.spi.ColumnTypeInformation;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.JdbcTypeConstructor;
import org.hibernate.type.spi.TypeConfiguration;

import static org.hibernate.dialect.DialectDelegateWrapper.extractRealDialect;

/**
 * Factory for {@link ReactiveArrayJdbcType}.
 */
public class ReactiveArrayJdbcTypeConstructor implements JdbcTypeConstructor {
	public static final ReactiveArrayJdbcTypeConstructor INSTANCE = new ReactiveArrayJdbcTypeConstructor();

	@Override
	public JdbcType resolveType(
			TypeConfiguration typeConfiguration,
			Dialect dialect,
			BasicType<?> elementType,
			ColumnTypeInformation columnTypeInformation) {
		Dialect realDialect = extractRealDialect( dialect );
		if ( realDialect instanceof OracleDialect ) {
			String typeName = columnTypeInformation == null ? null : columnTypeInformation.getTypeName();
			if ( typeName == null || typeName.isBlank() ) {
				typeName = ReactiveOracleArrayJdbcType.getTypeName( elementType, dialect );
			}
			return new ReactiveOracleArrayJdbcType( elementType.getJdbcType(), typeName );
		}
		return resolveType( typeConfiguration, dialect, elementType.getJdbcType(), columnTypeInformation );
	}

	@Override
	public JdbcType resolveType(
			TypeConfiguration typeConfiguration,
			Dialect dialect,
			JdbcType elementType,
			ColumnTypeInformation columnTypeInformation) {
		Dialect realDialect = extractRealDialect( dialect );
		if ( realDialect instanceof OracleDialect ) {
			// a bit wrong, since columnTypeInformation.getTypeName() is typically null!
			return new ReactiveOracleArrayJdbcType(
					elementType,
					columnTypeInformation == null ? null : columnTypeInformation.getTypeName()
			);
		}
		return new ReactiveArrayJdbcType( elementType );
	}

	@Override
	public int getDefaultSqlTypeCode() {
		return Types.ARRAY;
	}
}
