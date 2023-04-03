/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableStrategy;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.query.sqm.mutation.internal.temptable.ReactivePersistentTableStrategy;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Managed the creation of a {@link SessionFactory} that can shared among tests.
 */
public class SessionFactoryManager {

	private SessionFactory sessionFactory;
	private ReactiveConnectionPool poolProvider;

	public SessionFactoryManager() {
	}

	private boolean needsStart() {
		return sessionFactory == null || sessionFactory.isClosed();
	}

	public void start(Supplier<SessionFactory> supplier) {
		if ( needsStart() ) {
			sessionFactory = supplier.get();
			poolProvider = sessionFactory
					.unwrap( SessionFactoryImplementor.class )
					.getServiceRegistry().getService( ReactiveConnectionPool.class );
		}
	}

	public boolean isStarted() {
		return sessionFactory != null;
	}

	public SessionFactory getHibernateSessionFactory() {
		return sessionFactory;
	}

	public ReactiveConnectionPool getReactiveConnectionPool() {
		return poolProvider;
	}

	public CompletionStage<Void> stop() {
		CompletionStage<Void> releasedStage = voidFuture();
		if ( sessionFactory != null && sessionFactory.isOpen() ) {
			SessionFactoryImplementor sessionFactoryImplementor = sessionFactory.unwrap( SessionFactoryImplementor.class );
			MappingMetamodelImplementor mappingMetamodel = sessionFactoryImplementor
					.getRuntimeMetamodels()
					.getMappingMetamodel();
			final List<ReactivePersistentTableStrategy> reactiveStrategies = new ArrayList<>();
			mappingMetamodel.forEachEntityDescriptor(
					entityPersister -> addPersistentTableStrategy( reactiveStrategies, entityPersister )
			);
			if ( !reactiveStrategies.isEmpty() ) {
				releasedStage = loop( reactiveStrategies, strategy -> {
					( (PersistentTableStrategy) strategy )
							.release( sessionFactory.unwrap( SessionFactoryImplementor.class ), null );
					return strategy.getDropTableActionStage();
				} );

				releasedStage = releasedStage
						.whenComplete( (unused, throwable) -> sessionFactory.close() );
			}
		}
		return releasedStage
				.thenCompose( unused -> {
					final CompletionStage<Void> closeFuture;
					if ( poolProvider == null ) {
						closeFuture = voidFuture();
					}
					else {
						closeFuture = poolProvider.getCloseFuture();
					}
					poolProvider = null;
					sessionFactory = null;
					return closeFuture;
				} );
	}

	private void addPersistentTableStrategy(List<ReactivePersistentTableStrategy> reactiveStrategies, EntityPersister entityPersister) {
		if ( entityPersister.getSqmMultiTableMutationStrategy() instanceof ReactivePersistentTableStrategy ) {
			reactiveStrategies.add( (ReactivePersistentTableStrategy) entityPersister.getSqmMultiTableMutationStrategy() );
		}
		if ( entityPersister.getSqmMultiTableInsertStrategy() instanceof ReactivePersistentTableStrategy ) {
			reactiveStrategies.add( (ReactivePersistentTableStrategy) entityPersister.getSqmMultiTableInsertStrategy() );
		}
	}
}
