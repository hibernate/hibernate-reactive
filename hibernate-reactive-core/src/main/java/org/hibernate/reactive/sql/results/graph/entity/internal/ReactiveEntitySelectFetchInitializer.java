/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.FetchNotFoundException;
import org.hibernate.annotations.NotFoundAction;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.impl.ReactiveQueryExecutorLookup;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.spi.EntityIdentifierNavigablePath;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.entity.EntityLoadingLogging;
import org.hibernate.sql.results.graph.entity.LoadingEntityEntry;
import org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchInitializer;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
import static org.hibernate.internal.log.LoggingHelper.toLoggableString;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.whileLoop;
import static org.hibernate.sql.results.graph.entity.EntityLoadingLogging.DEBUG_ENABLED;
import static org.hibernate.sql.results.graph.entity.EntityLoadingLogging.ENTITY_LOADING_LOGGER;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchInitializer
 */
public class ReactiveEntitySelectFetchInitializer extends EntitySelectFetchInitializer implements ReactiveInitializer {

	private static final String CONCRETE_NAME = ReactiveEntitySelectFetchInitializer.class.getSimpleName();

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final boolean isEnhancedForLazyLoading;

	public ReactiveEntitySelectFetchInitializer(
			FetchParentAccess parentAccess,
			ToOneAttributeMapping toOneMapping,
			NavigablePath fetchedNavigable,
			EntityPersister concreteDescriptor,
			DomainResultAssembler<?> keyAssembler) {
		super( parentAccess, toOneMapping, fetchedNavigable, concreteDescriptor, keyAssembler );
		this.isEnhancedForLazyLoading = concreteDescriptor.getBytecodeEnhancementMetadata().isEnhancedForLazyLoading();
	}

	@Override
	public void resolveInstance(RowProcessingState rowProcessingState) {
		super.resolveInstance( rowProcessingState );
	}

	@Override
	public void initializeInstance(RowProcessingState rowProcessingState) {
		throw LOG.nonReactiveMethodCall( "reactiveInitializeInstance" );
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(ReactiveRowProcessingState rowProcessingState) {
		NavigablePath[] np = { getNavigablePath().getParent() };
		if ( np[0] == null ) {
			return voidFuture();
		}
		return whileLoop( () -> {
			CompletionStage<Void> loop = voidFuture();
			// Defer the select by default to the initialize phase
			// We only need to select in this phase if this is part of an identifier or foreign key
			if ( np[0] instanceof EntityIdentifierNavigablePath
					|| ForeignKeyDescriptor.PART_NAME.equals( np[0].getLocalName() )
					|| ForeignKeyDescriptor.TARGET_PART_NAME.equals( np[0].getLocalName() ) ) {
				loop = reactiveInitializeInstance( rowProcessingState );
			}
			return loop.thenApply( v -> {
				np[0] = np[0].getParent();
				return np[0] != null;
			} );
		} );
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(ReactiveRowProcessingState rowProcessingState) {
		if ( getEntityInstance() != null || isEntityInitialized() ) {
			return voidFuture();
		}

		if ( !isAttributeAssignableToConcreteDescriptor() ) {
			return voidFuture();
		}

		final Object entityIdentifier = keyAssembler.assemble( rowProcessingState );
		if ( entityIdentifier == null ) {
			isInitialized = true;
			return voidFuture();
		}

		if ( EntityLoadingLogging.TRACE_ENABLED ) {
			ENTITY_LOADING_LOGGER.tracef(
					"(%s) Beginning Initializer#resolveInstance process for entity (%s) : %s",
					StringHelper.collapse( this.getClass().getName() ),
					getNavigablePath(),
					entityIdentifier
			);
		}

		final SharedSessionContractImplementor session = rowProcessingState.getSession();
		final String entityName = concreteDescriptor.getEntityName();
		final EntityKey entityKey = new EntityKey( entityIdentifier, concreteDescriptor );

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		entityInstance = persistenceContext.getEntity( entityKey );
		if ( entityInstance != null ) {
			if ( isPersistentAttributeInterceptable( entityInstance ) ) {
				final PersistentAttributeInterceptor interceptor = asPersistentAttributeInterceptable( entityInstance ).$$_hibernate_getInterceptor();
				if ( interceptor instanceof EnhancementAsProxyLazinessInterceptor ) {
					( (EnhancementAsProxyLazinessInterceptor) interceptor ).forceInitialize( entityInstance, null );
				}
			}
			isInitialized = true;
			return voidFuture();
		}

		final LoadingEntityEntry existingLoadingEntry = session
				.getPersistenceContext()
				.getLoadContexts()
				.findLoadingEntityEntry( entityKey );

		if ( existingLoadingEntry != null ) {
			if ( DEBUG_ENABLED ) {
				ENTITY_LOADING_LOGGER.debugf(
						"(%s) Found existing loading entry [%s] - using loading instance",
						CONCRETE_NAME,
						toLoggableString( getNavigablePath(), entityIdentifier )
				);
			}
			this.entityInstance = existingLoadingEntry.getEntityInstance();

			if ( existingLoadingEntry.getEntityInitializer() != this ) {
				// the entity is already being loaded elsewhere
				if ( DEBUG_ENABLED ) {
					ENTITY_LOADING_LOGGER.debugf(
							"(%s) Entity [%s] being loaded by another initializer [%s] - skipping processing",
							CONCRETE_NAME,
							toLoggableString( getNavigablePath(), entityIdentifier ),
							existingLoadingEntry.getEntityInitializer()
					);
				}

				// EARLY EXIT!!!
				isInitialized = true;
				return voidFuture();
			}
		}

		if ( DEBUG_ENABLED ) {
			ENTITY_LOADING_LOGGER.debugf(
					"(%s) Invoking session#internalLoad for entity (%s) : %s",
					CONCRETE_NAME,
					toLoggableString( getNavigablePath(), entityIdentifier ),
					entityIdentifier
			);
		}

		return ReactiveQueryExecutorLookup.extract( session )
				.reactiveInternalLoad( entityName, entityIdentifier, true, toOneMapping().isInternalLoadNullable() )
				.thenCompose( instance -> {
					entityInstance = instance;

					if ( entityInstance == null ) {
						if ( toOneMapping().getNotFoundAction() == NotFoundAction.EXCEPTION ) {
							return failedFuture( new FetchNotFoundException( entityName, entityIdentifier ) );
						}
					}

					if ( DEBUG_ENABLED ) {
						ENTITY_LOADING_LOGGER.debugf(
								"(%s) Entity [%s] : %s has being loaded by session.internalLoad.",
								CONCRETE_NAME,
								toLoggableString( getNavigablePath(), entityIdentifier ),
								entityIdentifier
						);
					}

					final boolean unwrapProxy = toOneMapping().isUnwrapProxy() && isEnhancedForLazyLoading;
					final LazyInitializer lazyInitializer = HibernateProxy.extractLazyInitializer( entityInstance );
					if ( lazyInitializer != null ) {
						lazyInitializer.setUnwrap( unwrapProxy );
					}
					isInitialized = true;
					return voidFuture();
				} );
	}

	protected ToOneAttributeMapping toOneMapping() {
		return (ToOneAttributeMapping) getInitializedPart();
	}
}
