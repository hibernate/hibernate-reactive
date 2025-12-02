/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.mapping.internal;

import java.util.function.BiConsumer;

import org.hibernate.engine.FetchTiming;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.internal.AbstractCompositeIdentifierMapping;
import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.metamodel.mapping.internal.EmbeddedIdentifierMappingImpl;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.reactive.sql.results.graph.embeddable.internal.ReactiveEmbeddableFetchImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.Fetchable;

public class ReactiveEmbeddedIdentifierMappingImpl extends AbstractCompositeIdentifierMapping {

	private final EmbeddedIdentifierMappingImpl delegate;

	public ReactiveEmbeddedIdentifierMappingImpl(EmbeddedIdentifierMappingImpl delegate) {
		super( delegate );
		this.delegate = delegate;
	}

	@Override
	public Fetch generateFetch(
			FetchParent fetchParent,
			NavigablePath fetchablePath,
			FetchTiming fetchTiming,
			boolean selected,
			String resultVariable,
			DomainResultCreationState creationState) {
		return new ReactiveEmbeddableFetchImpl(
				fetchablePath,
				this,
				fetchParent,
				fetchTiming,
				selected,
				creationState
		);
	}

	@Override
	public EmbeddableMappingType getPartMappingType() {
		return delegate.getPartMappingType();
	}

	@Override
	public void applySqlSelections(
			NavigablePath navigablePath,
			TableGroup tableGroup,
			DomainResultCreationState creationState) {
		delegate.applySqlSelections( navigablePath, tableGroup, creationState );
	}

	@Override
	public void applySqlSelections(
			NavigablePath navigablePath,
			TableGroup tableGroup,
			DomainResultCreationState creationState,
			BiConsumer<SqlSelection, JdbcMapping> selectionConsumer) {
		delegate.applySqlSelections( navigablePath, tableGroup, creationState, selectionConsumer );
	}

	@Override
	public EmbeddableMappingType getMappedIdEmbeddableTypeDescriptor() {
		return delegate.getMappedIdEmbeddableTypeDescriptor();
	}

	@Override
	public Nature getNature() {
		return delegate.getNature();
	}

	@Override
	public String getAttributeName() {
		return delegate.getAttributeName();
	}

	@Override
	public Object getIdentifier(Object entity) {
		return delegate.getIdentifier( entity );
	}

	@Override
	public void setIdentifier(Object entity, Object id, SharedSessionContractImplementor session) {
		delegate.setIdentifier( entity, id, session );
	}

	@Override
	public String getSqlAliasStem() {
		return "";
	}

	@Override
	public String getFetchableName() {
		return delegate.getFetchableName();
	}

	@Override
	public Fetchable getFetchable(int position) {
		Fetchable fetchable = delegate.getFetchable( position );
		if ( fetchable instanceof ToOneAttributeMapping ) {
			return new ReactiveToOneAttributeMapping( (ToOneAttributeMapping) fetchable );
		}
		return fetchable;
	}
}
