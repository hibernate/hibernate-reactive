/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import org.hibernate.FetchNotFoundException;
import org.hibernate.Hibernate;
import org.hibernate.LockMode;
import org.hibernate.annotations.NotFoundAction;
import org.hibernate.internal.log.LoggingHelper;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.reactive.sql.results.graph.entity.ReactiveAbstractEntityInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.entity.EntityLoadingLogging;
import org.hibernate.sql.results.graph.entity.EntityResultGraphNode;
import org.hibernate.sql.results.graph.entity.EntityValuedFetchable;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

/**
 * Basically a copy of {@link org.hibernate.sql.results.graph.entity.internal.EntityJoinedFetchInitializer}.
 *
 * We could delegate but it would require override a huge amount of methods.
 *
 * @see org.hibernate.sql.results.graph.entity.internal.EntityJoinedFetchInitializer
 */
public class ReactiveEntityJoinedFetchInitializer extends ReactiveAbstractEntityInitializer {

	private static final String CONCRETE_NAME = ReactiveEntityJoinedFetchInitializer.class.getSimpleName();

	private final DomainResultAssembler<?> keyAssembler;
	private final NotFoundAction notFoundAction;

	public ReactiveEntityJoinedFetchInitializer(
			EntityResultGraphNode resultDescriptor,
			EntityValuedFetchable referencedFetchable,
			NavigablePath navigablePath,
			LockMode lockMode,
			NotFoundAction notFoundAction,
			DomainResult<?> keyResult,
			DomainResult<Object> rowIdResult,
			Fetch identifierFetch,
			Fetch discriminatorFetch,
			FetchParentAccess parentAccess,
			AssemblerCreationState creationState) {
		super(
				resultDescriptor,
				navigablePath,
				lockMode,
				identifierFetch,
				discriminatorFetch,
				rowIdResult,
				parentAccess,
				creationState
		);
		assert getInitializedPart() == referencedFetchable;
		this.notFoundAction = notFoundAction;

		this.keyAssembler = keyResult == null ? null : keyResult.createResultAssembler( this, creationState );
	}

	@Override
	public void resolveKey(RowProcessingState rowProcessingState) {
		if ( isParentShallowCached() ) {
			state = State.MISSING;
		}
		else if ( state == State.UNINITIALIZED ) {
			if ( shouldSkipInitializer( rowProcessingState ) ) {
				state = State.MISSING;
				return;
			}

			super.resolveKey( rowProcessingState );

			// super processes the foreign-key target column.  here we
			// need to also look at the foreign-key value column to check
			// for a dangling foreign-key

			if ( keyAssembler != null ) {
				final Object fkKeyValue = keyAssembler.assemble( rowProcessingState );
				if ( fkKeyValue != null ) {
					if ( state == State.MISSING ) {
						if ( notFoundAction != NotFoundAction.IGNORE ) {
							throw new FetchNotFoundException(
									getEntityDescriptor().getEntityName(),
									fkKeyValue
							);
						}
						else {
							EntityLoadingLogging.ENTITY_LOADING_LOGGER.debugf(
									"Ignoring dangling foreign-key due to `@NotFound(IGNORE); association will be null - %s",
									getNavigablePath()
							);
						}
					}
				}
			}
		}
	}

	@Override
	public void initializeInstanceFromParent(Object parentInstance, RowProcessingState rowProcessingState) {
		final AttributeMapping attributeMapping = getInitializedPart().asAttributeMapping();
		final Object instance = attributeMapping != null
				? attributeMapping.getValue( parentInstance )
				: parentInstance;
		setEntityInstance( instance );
		setEntityInstanceForNotify( Hibernate.unproxy( instance ) );
		state = State.INITIALIZED;
		initializeSubInstancesFromParent( rowProcessingState );
	}

	@Override
	protected String getSimpleConcreteImplName() {
		return CONCRETE_NAME;
	}

	@Override
	public boolean isResultInitializer() {
		return false;
	}

	@Override
	public String toString() {
		return "ReactiveEntityJoinedFetchInitializer(" + LoggingHelper.toLoggableString( getNavigablePath() ) + ")";
	}
}
