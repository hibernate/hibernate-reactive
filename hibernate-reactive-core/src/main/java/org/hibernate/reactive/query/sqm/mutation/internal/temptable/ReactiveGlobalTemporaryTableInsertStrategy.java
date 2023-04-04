/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableStrategy;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableInsertStrategy;

/**
 * @see org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableInsertStrategy
 */
public class ReactiveGlobalTemporaryTableInsertStrategy extends GlobalTemporaryTableStrategy
		implements ReactiveGlobalTemporaryTableStrategy, ReactiveSqmMultiTableInsertStrategy {

	private final CompletableFuture<Void> tableCreatedStage = new CompletableFuture();

	private final CompletableFuture<Void> tableDroppedStage = new CompletableFuture();

	private boolean prepared;

	private boolean dropIdTables;

	public ReactiveGlobalTemporaryTableInsertStrategy(GlobalTemporaryTableStrategy strategy) {
		super( strategy.getTemporaryTable(), strategy.getSessionFactory() );
	}

	@Override
	public void prepare(
			MappingModelCreationProcess mappingModelCreationProcess,
			JdbcConnectionAccess connectionAccess) {
		prepare( mappingModelCreationProcess, connectionAccess, tableCreatedStage );
	}

	@Override
	public void release(SessionFactoryImplementor sessionFactory, JdbcConnectionAccess connectionAccess) {
		release( sessionFactory, connectionAccess, tableDroppedStage );
	}

	@Override
	public CompletionStage<Integer> reactiveExecuteInsert(
			SqmInsertStatement<?> sqmInsertStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		return tableCreatedStage.thenCompose( v -> new ReactiveTableBasedInsertHandler(
				sqmInsertStatement,
				domainParameterXref,
				getTemporaryTable(),
				getSessionFactory().getJdbcServices().getDialect().getTemporaryTableAfterUseAction(),
				// generally a global temp table should already track a Connection-specific uid,
				// but just in case a particular env needs it...
				ReactiveGlobalTemporaryTableStrategy::sessionIdentifier,
				getSessionFactory()
		).reactiveExecute( context ) );
	}


	@Override
	public boolean isPrepared() {
		return prepared;
	}

	@Override
	public void setPrepared(boolean prepared) {
		this.prepared = prepared;
	}

	@Override
	public boolean isDropIdTables() {
		return dropIdTables;
	}

	@Override
	public void setDropIdTables(boolean dropIdTables) {
		this.dropIdTables = dropIdTables;
	}

	@Override
	public CompletionStage<Void> getDropTableActionStage() {
		return tableDroppedStage;
	}

	@Override
	public CompletionStage<Void> getCreateTableActionStage() {
		return tableCreatedStage;
	}
}
