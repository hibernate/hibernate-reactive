/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import org.hibernate.annotations.NotFoundAction;
import org.hibernate.engine.FetchTiming;
import org.hibernate.metamodel.mapping.EntityValuedModelPart;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.sql.results.graph.embeddable.internal.ReactiveNonAggregatedIdentifierMappingFetch;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityAssembler;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityInitializerImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.Fetchable;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.embeddable.internal.NonAggregatedIdentifierMappingFetch;
import org.hibernate.sql.results.graph.entity.internal.EntityResultImpl;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

public class ReactiveEntityResultImpl extends EntityResultImpl {
	private static final Log LOG = make( Log.class, lookup() );

	public ReactiveEntityResultImpl(
			NavigablePath navigablePath,
			EntityValuedModelPart entityValuedModelPart,
			TableGroup tableGroup,
			String resultVariable) {
		super( navigablePath, entityValuedModelPart, tableGroup, resultVariable );
	}

	@Override
	public DomainResultAssembler<?> createResultAssembler(
			InitializerParent parent,
			AssemblerCreationState creationState) {
		return new ReactiveEntityAssembler(
				this.getResultJavaType(),
				creationState.resolveInitializer( this, parent, this ).asEntityInitializer()
		);
	}

	@Override
	public Initializer<?> createInitializer(InitializerParent<?> parent, AssemblerCreationState creationState) {
		return new ReactiveEntityInitializerImpl(
				this,
				getSourceAlias(),
				getIdentifierFetch(),
				getDiscriminatorFetch(),
				null,
				getRowIdResult(),
				NotFoundAction.EXCEPTION,
				false,
				null,
				true,
				creationState
		);
	}

	@Override
	public Fetch generateFetchableFetch(
			Fetchable fetchable,
			NavigablePath fetchablePath,
			FetchTiming fetchTiming,
			boolean selected,
			String resultVariable,
			DomainResultCreationState creationState) {
		Fetch fetch = super.generateFetchableFetch(
				fetchable,
				fetchablePath,
				fetchTiming,
				selected,
				resultVariable,
				creationState
		);
		if ( fetch instanceof NonAggregatedIdentifierMappingFetch ) {
			return new ReactiveNonAggregatedIdentifierMappingFetch( (NonAggregatedIdentifierMappingFetch) fetch );
		}
		return fetch;
	}
}
