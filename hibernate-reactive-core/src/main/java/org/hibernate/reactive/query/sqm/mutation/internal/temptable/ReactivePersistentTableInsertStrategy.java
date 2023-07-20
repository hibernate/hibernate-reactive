/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableInsertStrategy;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableInsertStrategy;

public class ReactivePersistentTableInsertStrategy extends PersistentTableInsertStrategy
		implements ReactivePersistentTableStrategy, ReactiveSqmMultiTableInsertStrategy {

	private final InternalStage<Void> tableCreatedStage = InternalStage.newStage();

	private final InternalStage<Void> tableDroppedStage = InternalStage.newStage();

	private boolean prepared;

	private boolean dropIdTables;

	public ReactivePersistentTableInsertStrategy(PersistentTableInsertStrategy strategy) {
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
	public InternalStage<Integer> reactiveExecuteInsert(
			SqmInsertStatement<?> sqmInsertStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		return tableCreatedStage.thenCompose( v -> new ReactiveTableBasedInsertHandler(
				sqmInsertStatement,
				domainParameterXref,
				getTemporaryTable(),
				getSessionFactory().getJdbcServices().getDialect().getTemporaryTableAfterUseAction(),
				ReactivePersistentTableStrategy::sessionIdentifier,
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
	public InternalStage<Void> getDropTableActionStage() {
		return tableDroppedStage;
	}

	@Override
	public InternalStage<Void> getCreateTableActionStage() {
		return tableCreatedStage;
	}
}
