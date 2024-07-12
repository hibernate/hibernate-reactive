/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.hibernate.FetchNotFoundException;
import org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer;
import org.hibernate.engine.spi.EntityHolder;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.EntityUniqueKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.session.impl.ReactiveQueryExecutorLookup;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.reactive.sql.results.graph.embeddable.internal.ReactiveEmbeddableForeignKeyResultImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.basic.BasicFetch;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableForeignKeyResultImpl;
import org.hibernate.sql.results.graph.entity.internal.EntityDelayedFetchInitializer;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;
import org.hibernate.type.Type;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.results.graph.entity.internal.EntityInitializerImpl.determineConcreteEntityDescriptor;

public class ReactiveEntityDelayedFetchInitializer extends EntityDelayedFetchInitializer
		implements ReactiveInitializer<EntityDelayedFetchInitializer.EntityDelayedFetchInitializerData> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ToOneAttributeMapping referencedModelPart;

	public ReactiveEntityDelayedFetchInitializer(
			InitializerParent<?> parent,
			NavigablePath fetchedNavigable,
			ToOneAttributeMapping referencedModelPart,
			boolean selectByUniqueKey,
			DomainResult<?> keyResult,
			BasicFetch<?> discriminatorResult,
			AssemblerCreationState creationState) {
		super(
				parent,
				fetchedNavigable,
				referencedModelPart,
				selectByUniqueKey,
				convert(keyResult),
				discriminatorResult,
				creationState
		);
		this.referencedModelPart = referencedModelPart;
	}

	private static DomainResult<?> convert(DomainResult<?> keyResult) {
		return keyResult instanceof EmbeddableForeignKeyResultImpl<?>
				? new ReactiveEmbeddableForeignKeyResultImpl<>( (EmbeddableForeignKeyResultImpl<?>) keyResult )
				: keyResult;
	}

	public static class ReactiveEntityDelayedFetchInitializerData extends EntityDelayedFetchInitializerData {

		public ReactiveEntityDelayedFetchInitializerData(RowProcessingState rowProcessingState) {
			super( rowProcessingState );
		}

		public Object getEntityIdentifier() {
			return entityIdentifier;
		}

		public void setEntityIdentifier(Object entityIdentifier) {
			this.entityIdentifier = entityIdentifier;
		}
	}

	@Override
	protected InitializerData createInitializerData(RowProcessingState rowProcessingState) {
		return new ReactiveEntityDelayedFetchInitializerData( rowProcessingState );
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(EntityDelayedFetchInitializerData initializerData) {
		if ( initializerData.getState() != State.KEY_RESOLVED ) {
			return voidFuture();
		}

		ReactiveEntityDelayedFetchInitializerData data = (ReactiveEntityDelayedFetchInitializerData) initializerData;
		data.setState( State.RESOLVED );

		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		data.setEntityIdentifier( getIdentifierAssembler().assemble( rowProcessingState ) );

		CompletionStage<Void> stage = voidFuture();
		if ( data.getEntityIdentifier() == null ) {
			data.setInstance( null );
			data.setState( State.MISSING );
		}
		else {
			final SharedSessionContractImplementor session = rowProcessingState.getSession();

			final EntityPersister entityPersister = referencedModelPart.getEntityMappingType().getEntityPersister();
			final EntityPersister concreteDescriptor;
			if ( getDiscriminatorAssembler() != null ) {
				concreteDescriptor = determineConcreteEntityDescriptor( rowProcessingState, getDiscriminatorAssembler(), entityPersister );
				if ( concreteDescriptor == null ) {
					// If we find no discriminator it means there's no entity in the target table
					if ( !referencedModelPart.isOptional() ) {
						throw new FetchNotFoundException( entityPersister.getEntityName(), data.getEntityIdentifier() );
					}
					data.setInstance( null );
					data.setState( State.MISSING );
					return voidFuture();
				}
			}
			else {
				concreteDescriptor = entityPersister;
			}

			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			if ( isSelectByUniqueKey() ) {
				final String uniqueKeyPropertyName = referencedModelPart.getReferencedPropertyName();
				final Type uniqueKeyPropertyType = uniqueKeyPropertyName == null
						? concreteDescriptor.getIdentifierType()
						: session.getFactory().getReferencedPropertyType( concreteDescriptor.getEntityName(), uniqueKeyPropertyName );
				final EntityUniqueKey euk = new EntityUniqueKey(
						concreteDescriptor.getEntityName(),
						uniqueKeyPropertyName,
						data.getEntityIdentifier(),
						uniqueKeyPropertyType,
						session.getFactory()
				);
				data.setInstance( persistenceContext.getEntity( euk ) );
				if ( data.getInstance() == null ) {
					// For unique-key mappings, we always use bytecode-laziness if possible,
					// because we can't generate a proxy based on the unique key yet
					if ( referencedModelPart.isLazy() ) {
						data.setInstance( LazyPropertyInitializer.UNFETCHED_PROPERTY );
					}
					else {
						stage = stage
								.thenCompose( v -> ( (ReactiveEntityPersister) concreteDescriptor )
										.reactiveLoadByUniqueKey(
												uniqueKeyPropertyName,
												data.getEntityIdentifier(),
												session
										) )
								.thenAccept( data::setInstance )
								.thenAccept( v -> {
									// If the entity was not in the Persistence Context, but was found now,
									// add it to the Persistence Context
									if ( data.getInstance() != null ) {
										persistenceContext.addEntity( euk, data.getInstance() );
									}
								} );
					}
				}
				stage = stage.thenAccept( v -> {
					if ( data.getInstance() != null ) {
						data.setInstance( persistenceContext.proxyFor( data.getInstance() ) );
					}
				} );
			}
			else {
				final EntityKey entityKey = new EntityKey( data.getEntityIdentifier(), concreteDescriptor );
				final EntityHolder holder = persistenceContext.getEntityHolder( entityKey );
				if ( holder != null && holder.getEntity() != null ) {
					data.setInstance( persistenceContext.proxyFor( holder, concreteDescriptor ) );
				}
				// For primary key based mappings we only use bytecode-laziness if the attribute is optional,
				// because the non-optionality implies that it is safe to have a proxy
				else if ( referencedModelPart.isOptional() && referencedModelPart.isLazy() ) {
					data.setInstance( LazyPropertyInitializer.UNFETCHED_PROPERTY );
				}
				else {
					stage = stage.thenCompose( v -> ReactiveQueryExecutorLookup
							.extract( session )
							.reactiveInternalLoad(
									concreteDescriptor.getEntityName(),
									data.getEntityIdentifier(),
									false,
									false
							)
							.thenAccept( data::setInstance )
					);
				}
				stage = stage
						.thenAccept( v -> {
							final LazyInitializer lazyInitializer = HibernateProxy.extractLazyInitializer( data.getInstance() );
							if ( lazyInitializer != null ) {
								lazyInitializer.setUnwrap( referencedModelPart.isUnwrapProxy() && concreteDescriptor.isInstrumented() );
							}
						} );
			}
		}
		return stage;
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(EntityDelayedFetchInitializerData data) {
		// No-op by default
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> forEachReactiveSubInitializer(
			BiFunction<ReactiveInitializer<?>, RowProcessingState, CompletionStage<Void>> consumer,
			InitializerData data) {
		final ReactiveInitializer<?> initializer = (ReactiveInitializer<?>) getIdentifierAssembler().getInitializer();
		if ( initializer != null ) {
			return consumer.apply( initializer, data.getRowProcessingState() );
		}
		return voidFuture();
	}
}
