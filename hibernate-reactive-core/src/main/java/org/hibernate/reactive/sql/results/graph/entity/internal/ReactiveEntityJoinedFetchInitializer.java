/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import org.hibernate.FetchNotFoundException;
import org.hibernate.LockMode;
import org.hibernate.annotations.NotFoundAction;
import org.hibernate.reactive.sql.results.graph.entity.ReactiveAbstractEntityInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.entity.EntityLoadingLogging;
import org.hibernate.sql.results.graph.entity.EntityResultGraphNode;
import org.hibernate.sql.results.graph.entity.EntityValuedFetchable;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntityJoinedFetchInitializer
 */
public class ReactiveEntityJoinedFetchInitializer extends ReactiveAbstractEntityInitializer {

	private static final String CONCRETE_NAME = ReactiveEntityJoinedFetchInitializer.class.getSimpleName();

	private final EntityValuedFetchable referencedFetchable;
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
			AssemblerCreationState creationState) {
		super(
				resultDescriptor,
				navigablePath,
				lockMode,
				identifierFetch,
				discriminatorFetch,
				rowIdResult,
				creationState
		);
		this.referencedFetchable = referencedFetchable;
		this.notFoundAction = notFoundAction;
		this.keyAssembler = keyResult == null ? null : keyResult.createResultAssembler( this, creationState );
	}

	@Override
	protected void registerLoadingEntityInstanceFromExecutionContext(RowProcessingState rowProcessingState, Object instance) {
		// we want the EntityResultInitializer to take care of the instance
	}

	@Override
	public void resolveKey(RowProcessingState rowProcessingState) {
		if ( shouldSkipResolveInstance( rowProcessingState ) ) {
			missing = true;
			return;
		}

		super.resolveKey( rowProcessingState );

		// super processes the foreign-key target column.  here we
		// need to also look at the foreign-key value column to check
		// for a dangling foreign-key

		if ( keyAssembler != null ) {
			final Object fkKeyValue = keyAssembler.assemble( rowProcessingState );
			if ( fkKeyValue != null ) {
				if ( isMissing() ) {
					if ( notFoundAction != NotFoundAction.IGNORE ) {
						throw new FetchNotFoundException(
								referencedFetchable.getEntityMappingType().getEntityName(),
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

	@Override
	protected String getSimpleConcreteImplName() {
		return CONCRETE_NAME;
	}

	@Override
	protected boolean isEntityReturn() {
		return false;
	}
}
