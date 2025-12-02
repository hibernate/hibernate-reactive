/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.util.concurrent.CompletableFuture;

import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.MutableObject;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableMutationStrategy;
import org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableStrategy;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandler;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandlerBuildResult;
import org.hibernate.query.sqm.tree.SqmDeleteOrUpdateStatement;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

/**
 * @see org.hibernate.query.sqm.mutation.internal.temptable.GlobalTemporaryTableMutationStrategy
 */
public class ReactiveGlobalTemporaryTableMutationStrategy extends GlobalTemporaryTableMutationStrategy
		implements ReactiveGlobalTemporaryTableStrategy, ReactiveSqmMultiTableMutationStrategy {

	private final CompletableFuture<Void> tableCreatedStage = new CompletableFuture<>();

	private final CompletableFuture<Void> tableDroppedStage = new CompletableFuture<>();

	private boolean prepared;

	private boolean dropIdTables;

	public ReactiveGlobalTemporaryTableMutationStrategy(GlobalTemporaryTableStrategy strategy) {
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
	public MultiTableHandlerBuildResult buildHandler(SqmDeleteOrUpdateStatement<?> sqmStatement, DomainParameterXref domainParameterXref, DomainQueryExecutionContext context) {
		final MutableObject<JdbcParameterBindings> firstJdbcParameterBindings = new MutableObject<>();
		final MultiTableHandler multiTableHandler = sqmStatement instanceof SqmDeleteStatement<?> sqmDelete
				? buildHandler( sqmDelete, domainParameterXref, context, firstJdbcParameterBindings )
				: buildHandler( (SqmUpdateStatement<?>) sqmStatement, domainParameterXref, context, firstJdbcParameterBindings );
		return new MultiTableHandlerBuildResult( multiTableHandler, firstJdbcParameterBindings.get() );
	}

	@Override
	public MultiTableHandler buildHandler(
			SqmUpdateStatement<?> sqmUpdate,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		return new ReactiveTableBasedUpdateHandler(
				sqmUpdate,
				domainParameterXref,
				getTemporaryTable(),
				getTemporaryTableStrategy(),
				false,
				// generally a global temp table should already track a Connection-specific uid,
				// but just in case a particular env needs it...
				session -> session.getSessionIdentifier().toString(),
				context,
				firstJdbcParameterBindingsConsumer
		);
	}

	@Override
	public MultiTableHandler buildHandler(
			SqmDeleteStatement<?> sqmDelete,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		final EntityPersister rootDescriptor = context.getSession().getFactory().getMappingMetamodel()
				.getEntityDescriptor( sqmDelete.getRoot().getEntityName() );
		if ( rootDescriptor.getSoftDeleteMapping() != null ) {
			return new ReactiveTableBasedSoftDeleteHandler(
					sqmDelete,
					domainParameterXref,
					getTemporaryTable(),
					getTemporaryTableStrategy(),
					false,
					// generally a global temp table should already track a Connection-specific uid,
					// but just in case a particular env needs it...
					session -> session.getSessionIdentifier().toString(),
					context,
					firstJdbcParameterBindingsConsumer
			);
		}
		else {
			return new ReactiveTableBasedDeleteHandler(
					sqmDelete,
					domainParameterXref,
					getTemporaryTable(),
					getTemporaryTableStrategy(),
					false,
					// generally a global temp table should already track a Connection-specific uid,
					// but just in case a particular env needs it...
					session -> session.getSessionIdentifier().toString(),
					context,
					firstJdbcParameterBindingsConsumer
			);
		}
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
}
