/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.mapping.internal;

import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import org.hibernate.cache.MutableCacheKeyBuilder;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.IndexedConsumer;
import org.hibernate.metamodel.mapping.AssociationKey;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.BasicValuedModelPart;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.metamodel.mapping.ManagedMappingType;
import org.hibernate.metamodel.mapping.MappingType;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.metamodel.mapping.SelectableConsumer;
import org.hibernate.metamodel.mapping.SelectableMapping;
import org.hibernate.metamodel.mapping.ValuedModelPart;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.metamodel.mapping.internal.PluralAttributeMappingImpl;
import org.hibernate.metamodel.mapping.internal.SimpleForeignKeyDescriptor;
import org.hibernate.metamodel.model.domain.NavigableRole;
import org.hibernate.reactive.sql.results.graph.collection.internal.ReactiveCollectionDomainResult;
import org.hibernate.reactive.sql.results.graph.collection.internal.ReactiveEagerCollectionFetch;
import org.hibernate.reactive.sql.results.graph.embeddable.internal.ReactiveEmbeddableForeignKeyResultImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.spi.SqlAstCreationState;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableGroupProducer;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.embeddable.internal.EmbeddableForeignKeyResultImpl;
import org.hibernate.type.descriptor.java.JavaType;

public class ReactivePluralAttributeMapping extends PluralAttributeMappingImpl implements PluralAttributeMapping {

	public ReactivePluralAttributeMapping(PluralAttributeMappingImpl original) {
		super( original );
	}

	@Override
	public <T> DomainResult<T> createDomainResult(
			NavigablePath navigablePath,
			TableGroup tableGroup,
			String resultVariable,
			DomainResultCreationState creationState) {
		final TableGroup collectionTableGroup = creationState.getSqlAstCreationState()
				.getFromClauseAccess()
				.getTableGroup( navigablePath );

		assert collectionTableGroup != null;

		// This is only used for collection initialization where we know the owner is available, so we mark it as visited
		// which will cause bidirectional to-one associations to be treated as such and avoid a join
		creationState.registerVisitedAssociationKey( getKeyDescriptor().getAssociationKey() );

		return new ReactiveCollectionDomainResult( navigablePath, this, resultVariable, tableGroup, creationState );
	}

	@Override
	protected Fetch buildEagerCollectionFetch(
			NavigablePath fetchedPath,
			PluralAttributeMapping fetchedAttribute,
			TableGroup collectionTableGroup,
			boolean needsCollectionKeyResult,
			FetchParent fetchParent,
			DomainResultCreationState creationState) {
		return new ReactiveEagerCollectionFetch(
				fetchedPath,
				fetchedAttribute,
				collectionTableGroup,
				needsCollectionKeyResult,
				fetchParent,
				creationState
		);
	}

	@Override
	public ForeignKeyDescriptor getKeyDescriptor() {
		if ( !super.getKeyDescriptor().isEmbedded() ) {
			// Hibernate ORM has a check like this that will fail if ForeignKeyDescriptor is not an instance of
			// SimpleForeignKeyDescriptor: see LoadSelectBuilder#applySubSelectRestriction line 1115
			return new ReactiveSimpleForeignKeyDescriptor( (SimpleForeignKeyDescriptor) super.getKeyDescriptor() );
		}
		return new ReactiveForeignKeyDescriptor( super.getKeyDescriptor() );
	}

	private static DomainResult<?> convert(DomainResult<?> keyDomainResult) {
		if ( keyDomainResult instanceof EmbeddableForeignKeyResultImpl ) {
			return new ReactiveEmbeddableForeignKeyResultImpl<>( (EmbeddableForeignKeyResultImpl<?>) keyDomainResult );
		}
		return keyDomainResult;
	}

	private static class ReactiveSimpleForeignKeyDescriptor extends SimpleForeignKeyDescriptor {

		protected ReactiveSimpleForeignKeyDescriptor(SimpleForeignKeyDescriptor original) {
			super( original );
		}

		@Override
		public DomainResult<?> createKeyDomainResult(
				NavigablePath navigablePath,
				TableGroup targetTableGroup,
				FetchParent fetchParent,
				DomainResultCreationState creationState) {
			return convert( super.createKeyDomainResult( navigablePath, targetTableGroup, fetchParent, creationState ) );
		}

		@Override
		public DomainResult<?> createKeyDomainResult(
				NavigablePath navigablePath,
				TableGroup targetTableGroup,
				Nature fromSide,
				FetchParent fetchParent,
				DomainResultCreationState creationState) {
			return convert( super.createKeyDomainResult( navigablePath, targetTableGroup, fromSide, fetchParent, creationState ) );
		}
	}

	private static class ReactiveForeignKeyDescriptor implements ForeignKeyDescriptor {

		private final ForeignKeyDescriptor delegate;

		public ReactiveForeignKeyDescriptor(ForeignKeyDescriptor delegate) {
			this.delegate = delegate;
		}

		@Override
		public String getPartName() {
			return delegate.getPartName();
		}

		@Override
		public String getKeyTable() {
			return delegate.getKeyTable();
		}

		@Override
		public String getTargetTable() {
			return delegate.getTargetTable();
		}

		@Override
		public ValuedModelPart getKeyPart() {
			return delegate.getKeyPart();
		}

		@Override
		public ValuedModelPart getTargetPart() {
			return delegate.getTargetPart();
		}

		@Override
		public boolean isKeyPart(ValuedModelPart modelPart) {
			return delegate.isKeyPart( modelPart );
		}

		@Override
		public ValuedModelPart getPart(Nature nature) {
			return delegate.getPart( nature );
		}

		@Override
		public Side getKeySide() {
			return delegate.getKeySide();
		}

		@Override
		public Side getTargetSide() {
			return delegate.getTargetSide();
		}

		@Override
		public Side getSide(Nature nature) {
			return delegate.getSide( nature );
		}

		@Override
		public String getContainingTableExpression() {
			return delegate.getContainingTableExpression();
		}

		@Override
		public int compare(Object key1, Object key2) {
			return delegate.compare( key1, key2 );
		}

		@Override
		public DomainResult<?> createKeyDomainResult(
				NavigablePath navigablePath,
				TableGroup targetTableGroup,
				FetchParent fetchParent,
				DomainResultCreationState creationState) {
			return convert( delegate.createKeyDomainResult(
					navigablePath,
					targetTableGroup,
					fetchParent,
					creationState
			) );
		}

		@Override
		public DomainResult<?> createKeyDomainResult(
				NavigablePath navigablePath,
				TableGroup targetTableGroup,
				Nature fromSide,
				FetchParent fetchParent,
				DomainResultCreationState creationState) {
			return convert( delegate.createKeyDomainResult(
					navigablePath,
					targetTableGroup,
					fromSide,
					fetchParent,
					creationState
			) );
		}

		@Override
		public DomainResult<?> createTargetDomainResult(
				NavigablePath navigablePath,
				TableGroup targetTableGroup,
				FetchParent fetchParent,
				DomainResultCreationState creationState) {
			return delegate.createTargetDomainResult( navigablePath, targetTableGroup, fetchParent, creationState );
		}

		@Override
		public <T> DomainResult<T> createDomainResult(
				NavigablePath navigablePath,
				TableGroup targetTableGroup,
				String resultVariable,
				DomainResultCreationState creationState) {
			return delegate.createDomainResult( navigablePath, targetTableGroup, resultVariable, creationState );
		}

		@Override
		public Predicate generateJoinPredicate(
				TableGroup targetSideTableGroup,
				TableGroup keySideTableGroup,
				SqlAstCreationState creationState) {
			return delegate.generateJoinPredicate( targetSideTableGroup, keySideTableGroup, creationState );
		}

		@Override
		public Predicate generateJoinPredicate(
				TableReference targetSideReference,
				TableReference keySideReference,
				SqlAstCreationState creationState) {
			return delegate.generateJoinPredicate( targetSideReference, keySideReference, creationState );
		}

		@Override
		public boolean isSimpleJoinPredicate(Predicate predicate) {
			return delegate.isSimpleJoinPredicate( predicate );
		}

		@Override
		public SelectableMapping getSelectable(int columnIndex) {
			return delegate.getSelectable( columnIndex );
		}

		@Override
		public int forEachSelectable(int offset, SelectableConsumer consumer) {
			return delegate.forEachSelectable( offset, consumer );
		}

		@Override
		public Object getAssociationKeyFromSide(
				Object targetObject,
				Nature nature,
				SharedSessionContractImplementor session) {
			return delegate.getAssociationKeyFromSide( targetObject, nature, session );
		}

		@Override
		public Object getAssociationKeyFromSide(
				Object targetObject,
				Side side,
				SharedSessionContractImplementor session) {
			return delegate.getAssociationKeyFromSide( targetObject, side, session );
		}

		@Override
		public int visitKeySelectables(int offset, SelectableConsumer consumer) {
			return delegate.visitKeySelectables( offset, consumer );
		}

		@Override
		public int visitKeySelectables(SelectableConsumer consumer) {
			return delegate.visitKeySelectables( consumer );
		}

		@Override
		public int visitTargetSelectables(int offset, SelectableConsumer consumer) {
			return delegate.visitTargetSelectables( offset, consumer );
		}

		@Override
		public int visitTargetSelectables(SelectableConsumer consumer) {
			return delegate.visitTargetSelectables( consumer );
		}

		@Override
		public ForeignKeyDescriptor withKeySelectionMapping(
				ManagedMappingType declaringType,
				TableGroupProducer declaringTableGroupProducer,
				IntFunction<SelectableMapping> selectableMappingAccess,
				MappingModelCreationProcess creationProcess) {
			return delegate.withKeySelectionMapping(
					declaringType,
					declaringTableGroupProducer,
					selectableMappingAccess,
					creationProcess
			);
		}

		@Override
		public ForeignKeyDescriptor withTargetPart(ValuedModelPart targetPart) {
			return delegate.withTargetPart( targetPart );
		}

		@Override
		public AssociationKey getAssociationKey() {
			return delegate.getAssociationKey();
		}

		@Override
		public boolean hasConstraint() {
			return delegate.hasConstraint();
		}

		@Override
		public boolean isEmbedded() {
			return delegate.isEmbedded();
		}

		@Override
		public boolean isVirtual() {
			return delegate.isVirtual();
		}

		@Override
		public NavigableRole getNavigableRole() {
			return delegate.getNavigableRole();
		}

		@Override
		public MappingType getPartMappingType() {
			return delegate.getPartMappingType();
		}

		@Override
		public JavaType<?> getJavaType() {
			return delegate.getJavaType();
		}

		@Override
		public boolean isEntityIdentifierMapping() {
			return delegate.isEntityIdentifierMapping();
		}

		@Override
		public boolean hasPartitionedSelectionMapping() {
			return delegate.hasPartitionedSelectionMapping();
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
		public int forEachSelectable(SelectableConsumer consumer) {
			return delegate.forEachSelectable( consumer );
		}

		@Override
		public AttributeMapping asAttributeMapping() {
			return delegate.asAttributeMapping();
		}

		@Override
		public EntityMappingType asEntityMappingType() {
			return delegate.asEntityMappingType();
		}

		@Override
		public BasicValuedModelPart asBasicValuedModelPart() {
			return delegate.asBasicValuedModelPart();
		}

		@Override
		public int breakDownJdbcValues(
				Object domainValue,
				JdbcValueConsumer valueConsumer,
				SharedSessionContractImplementor session) {
			return delegate.breakDownJdbcValues( domainValue, valueConsumer, session );
		}

		@Override
		public <X, Y> int breakDownJdbcValues(
				Object domainValue,
				int offset,
				X x,
				Y y,
				JdbcValueBiConsumer<X, Y> valueConsumer,
				SharedSessionContractImplementor session) {
			return delegate.breakDownJdbcValues( domainValue, offset, x, y, valueConsumer, session );
		}

		@Override
		public int decompose(
				Object domainValue,
				JdbcValueConsumer valueConsumer,
				SharedSessionContractImplementor session) {
			return delegate.decompose( domainValue, valueConsumer, session );
		}

		@Override
		public <X, Y> int decompose(
				Object domainValue,
				int offset,
				X x,
				Y y,
				JdbcValueBiConsumer<X, Y> valueConsumer,
				SharedSessionContractImplementor session) {
			return delegate.decompose( domainValue, offset, x, y, valueConsumer, session );
		}

		@Override
		public EntityMappingType findContainingEntityMapping() {
			return delegate.findContainingEntityMapping();
		}

		@Override
		public boolean areEqual(Object one, Object other, SharedSessionContractImplementor session) {
			return delegate.areEqual( one, other, session );
		}

		@Override
		public int getJdbcTypeCount() {
			return delegate.getJdbcTypeCount();
		}

		@Override
		public int forEachJdbcType(IndexedConsumer<JdbcMapping> action) {
			return delegate.forEachJdbcType( action );
		}

		@Override
		public Object disassemble(Object value, SharedSessionContractImplementor session) {
			return delegate.disassemble( value, session );
		}

		@Override
		public void addToCacheKey(
				MutableCacheKeyBuilder cacheKey,
				Object value,
				SharedSessionContractImplementor session) {
			delegate.addToCacheKey( cacheKey, value, session );
		}

		@Override
		public <X, Y> int forEachDisassembledJdbcValue(
				Object value,
				X x,
				Y y,
				JdbcValuesBiConsumer<X, Y> valuesConsumer,
				SharedSessionContractImplementor session) {
			return delegate.forEachDisassembledJdbcValue( value, x, y, valuesConsumer, session );
		}

		@Override
		public <X, Y> int forEachDisassembledJdbcValue(
				Object value,
				int offset,
				X x,
				Y y,
				JdbcValuesBiConsumer<X, Y> valuesConsumer,
				SharedSessionContractImplementor session) {
			return delegate.forEachDisassembledJdbcValue( value, offset, x, y, valuesConsumer, session );
		}

		@Override
		public int forEachDisassembledJdbcValue(
				Object value,
				JdbcValuesConsumer valuesConsumer,
				SharedSessionContractImplementor session) {
			return delegate.forEachDisassembledJdbcValue( value, valuesConsumer, session );
		}

		@Override
		public int forEachDisassembledJdbcValue(
				Object value,
				int offset,
				JdbcValuesConsumer valuesConsumer,
				SharedSessionContractImplementor session) {
			return delegate.forEachDisassembledJdbcValue( value, offset, valuesConsumer, session );
		}

		@Override
		public <X, Y> int forEachJdbcValue(
				Object value,
				X x,
				Y y,
				JdbcValuesBiConsumer<X, Y> valuesConsumer,
				SharedSessionContractImplementor session) {
			return delegate.forEachJdbcValue( value, x, y, valuesConsumer, session );
		}

		@Override
		public <X, Y> int forEachJdbcValue(
				Object value,
				int offset,
				X x,
				Y y,
				JdbcValuesBiConsumer<X, Y> valuesConsumer,
				SharedSessionContractImplementor session) {
			return delegate.forEachJdbcValue( value, offset, x, y, valuesConsumer, session );
		}

		@Override
		public int forEachJdbcValue(
				Object value,
				JdbcValuesConsumer valuesConsumer,
				SharedSessionContractImplementor session) {
			return delegate.forEachJdbcValue( value, valuesConsumer, session );
		}

		@Override
		public int forEachJdbcValue(
				Object value,
				int offset,
				JdbcValuesConsumer valuesConsumer,
				SharedSessionContractImplementor session) {
			return delegate.forEachJdbcValue( value, offset, valuesConsumer, session );
		}

		@Override
		public JdbcMapping getJdbcMapping(int index) {
			return delegate.getJdbcMapping( index );
		}

		@Override
		public JdbcMapping getSingleJdbcMapping() {
			return delegate.getSingleJdbcMapping();
		}

		@Override
		public int forEachJdbcType(int offset, IndexedConsumer<JdbcMapping> action) {
			return delegate.forEachJdbcType( offset, action );
		}

		@Override
		public void forEachInsertable(SelectableConsumer consumer) {
			delegate.forEachInsertable( consumer );
		}

		@Override
		public void forEachNonFormula(SelectableConsumer consumer) {
			delegate.forEachNonFormula( consumer );
		}

		@Override
		public void forEachUpdatable(SelectableConsumer consumer) {
			delegate.forEachUpdatable( consumer );
		}

		@Override
		public MappingType getMappedType() {
			return delegate.getMappedType();
		}

		@Override
		public JavaType<?> getExpressibleJavaType() {
			return delegate.getExpressibleJavaType();
		}

		@Override
		public <X> X treatAs(Class<X> targetType) {
			return delegate.treatAs( targetType );
		}
	}
}
