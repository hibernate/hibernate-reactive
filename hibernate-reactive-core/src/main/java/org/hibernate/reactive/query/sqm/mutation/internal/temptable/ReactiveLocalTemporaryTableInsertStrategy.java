/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import org.hibernate.internal.util.MutableObject;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.temptable.LocalTemporaryTableInsertStrategy;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandler;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandlerBuildResult;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableInsertStrategy;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

public class ReactiveLocalTemporaryTableInsertStrategy extends LocalTemporaryTableInsertStrategy
		implements ReactiveSqmMultiTableInsertStrategy {

	public ReactiveLocalTemporaryTableInsertStrategy(LocalTemporaryTableInsertStrategy insertStrategy) {
		super( insertStrategy.getTemporaryTable(), insertStrategy.getSessionFactory() );
	}

	@Override
	public MultiTableHandlerBuildResult buildHandler(SqmInsertStatement<?> sqmInsertStatement, DomainParameterXref domainParameterXref, DomainQueryExecutionContext context) {
		final MutableObject<JdbcParameterBindings> firstJdbcParameterBindings = new MutableObject<>();
		final MultiTableHandler multiTableHandler = new ReactiveTableBasedInsertHandler(
				sqmInsertStatement,
				domainParameterXref,
				getTemporaryTable(),
				getTemporaryTableStrategy(),
				isDropIdTables(),
				session -> {
					throw new UnsupportedOperationException( "Unexpected call to access Session uid" );
				},
				context,
				firstJdbcParameterBindings
		);
		return new MultiTableHandlerBuildResult( multiTableHandler, firstJdbcParameterBindings.get() );
	}

}
