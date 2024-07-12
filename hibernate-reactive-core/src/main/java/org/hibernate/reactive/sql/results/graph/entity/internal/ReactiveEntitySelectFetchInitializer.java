/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.hibernate.EntityFilterException;
import org.hibernate.FetchNotFoundException;
import org.hibernate.Hibernate;
import org.hibernate.annotations.NotFoundAction;
import org.hibernate.engine.spi.EntityHolder;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchInitializer;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchInitializer
 */
public class ReactiveEntitySelectFetchInitializer<Data extends EntitySelectFetchInitializer.EntitySelectFetchInitializerData>
		extends EntitySelectFetchInitializer<Data> implements ReactiveInitializer<Data> {

	public static class ReactiveEntitySelectFetchInitializerData
			extends EntitySelectFetchInitializer.EntitySelectFetchInitializerData {

		public ReactiveEntitySelectFetchInitializerData(RowProcessingState rowProcessingState) {
			super( rowProcessingState );
		}

		public Object getEntityIdentifier() {
			return entityIdentifier;
		}

		public void setEntityIdentifier(Object entityIdentifier) {
			super.entityIdentifier = entityIdentifier;
		}
	}

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final boolean isEnhancedForLazyLoading;

	public ReactiveEntitySelectFetchInitializer(
			InitializerParent<?> parent,
			ToOneAttributeMapping fetchedAttribute,
			NavigablePath fetchedNavigable,
			EntityPersister concreteDescriptor,
			DomainResult<?> keyResult,
			boolean affectedByFilter,
			AssemblerCreationState creationState) {
		super(
				parent,
				fetchedAttribute,
				fetchedNavigable,
				concreteDescriptor,
				keyResult,
				affectedByFilter,
				creationState
		);
		this.isEnhancedForLazyLoading = concreteDescriptor.getBytecodeEnhancementMetadata().isEnhancedForLazyLoading();
	}

	@Override
	protected InitializerData createInitializerData(RowProcessingState rowProcessingState) {
		return new ReactiveEntitySelectFetchInitializerData( rowProcessingState );
	}

	@Override
	public void resolveInstance(RowProcessingState rowProcessingState) {
		super.resolveInstance( rowProcessingState );
	}

	@Override
	public void initializeInstance(EntitySelectFetchInitializerData data) {
		throw LOG.nonReactiveMethodCall( "reactiveInitializeInstance" );
	}

	@Override
	protected void initialize(EntitySelectFetchInitializerData data) {
		throw LOG.nonReactiveMethodCall( "reactiveInitialize" );
	}

	@Override
	public CompletionStage<Void> forEachReactiveSubInitializer(
			BiFunction<ReactiveInitializer<?>, RowProcessingState, CompletionStage<Void>> consumer,
			InitializerData data) {
		Initializer<?> initializer = getKeyAssembler().getInitializer();
		if ( initializer != null ) {
			return consumer.apply( (ReactiveInitializer<?>) initializer, data.getRowProcessingState() );
		}
		return voidFuture();
	}

	protected CompletionStage<Void> reactiveInitialize(EntitySelectFetchInitializerData ormData) {
		ReactiveEntitySelectFetchInitializerData data = (ReactiveEntitySelectFetchInitializerData) ormData;
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		final SharedSessionContractImplementor session = rowProcessingState.getSession();
		final EntityKey entityKey = new EntityKey( data.getEntityIdentifier(), concreteDescriptor );

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		final EntityHolder holder = persistenceContext.getEntityHolder( entityKey );
		if ( holder != null ) {
			data.setInstance( persistenceContext.proxyFor( holder, concreteDescriptor ) );
			if ( holder.getEntityInitializer() == null ) {
				if ( data.getInstance() != null && Hibernate.isInitialized( data.getInstance() ) ) {
					data.setState( State.INITIALIZED );
					return voidFuture();
				}
			}
			else if ( holder.getEntityInitializer() != this ) {
				// the entity is already being loaded elsewhere
				data.setState( State.INITIALIZED );
				return voidFuture();
			}
			else if ( data.getInstance() == null ) {
				// todo: maybe mark this as resolved instead?
				assert holder.getProxy() == null : "How to handle this case?";
				data.setState( State.INITIALIZED );
				return voidFuture();
			}
		}
		data.setState( State.INITIALIZED );
		final String entityName = concreteDescriptor.getEntityName();

		return ( (ReactiveSession) session ).reactiveInternalLoad(
						entityName,
						data.getEntityIdentifier(),
						true,
						toOneMapping.isInternalLoadNullable()
				)
				.thenAccept( instance -> {
					data.setInstance( instance );

					if ( instance == null ) {
						if ( toOneMapping.getNotFoundAction() != NotFoundAction.IGNORE ) {
							if ( affectedByFilter ) {
								throw new EntityFilterException(
										entityName,
										data.getEntityIdentifier(),
										toOneMapping.getNavigableRole().getFullPath()
								);
							}
							if ( toOneMapping.getNotFoundAction() == NotFoundAction.EXCEPTION ) {
								throw new FetchNotFoundException( entityName, data.getEntityIdentifier() );
							}
						}
						rowProcessingState.getSession().getPersistenceContextInternal().claimEntityHolderIfPossible(
								new EntityKey( data.getEntityIdentifier(), concreteDescriptor ),
								instance,
								rowProcessingState.getJdbcValuesSourceProcessingState(),
								this
						);
					}

					final boolean unwrapProxy = toOneMapping.isUnwrapProxy() && isEnhancedForLazyLoading;
					final LazyInitializer lazyInitializer = HibernateProxy.extractLazyInitializer( data.getInstance() );
					if ( lazyInitializer != null ) {
						lazyInitializer.setUnwrap( unwrapProxy );
					}
				} );
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(Data original) {
		if ( original.getState() != State.KEY_RESOLVED ) {
			return voidFuture();
		}

		ReactiveEntitySelectFetchInitializerData data = (ReactiveEntitySelectFetchInitializerData) original;
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		data.setEntityIdentifier( keyAssembler.assemble( rowProcessingState ) );

		if ( data.getEntityIdentifier() == null ) {
			data.setState( State.MISSING );
			data.setInstance( null );
			return voidFuture();
		}

		data.setState( State.INITIALIZED );
		return reactiveInitialize( data );
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(Data data) {
		if ( data.getState() != State.RESOLVED ) {
			return voidFuture();
		}
		data.setState( State.INITIALIZED );
		Hibernate.initialize( data.getInstance() );
		return voidFuture();
	}
}
