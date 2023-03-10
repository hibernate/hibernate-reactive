/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.mapping.internal;

import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.hibernate.annotations.NotFoundAction;
import org.hibernate.engine.FetchStyle;
import org.hibernate.engine.FetchTiming;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.Generator;
import org.hibernate.internal.util.IndexedConsumer;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.AttributeMetadata;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.metamodel.mapping.ManagedMappingType;
import org.hibernate.metamodel.mapping.MappingType;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.metamodel.mapping.SelectableConsumer;
import org.hibernate.metamodel.mapping.SelectableMapping;
import org.hibernate.metamodel.mapping.internal.EmbeddedAttributeMapping;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.metamodel.model.domain.NavigableRole;
import org.hibernate.property.access.spi.PropertyAccess;
import org.hibernate.reactive.sql.results.graph.entity.internal.ReactiveEntityFetchSelectImpl;
import org.hibernate.reactive.sql.results.internal.domain.ReactiveCircularFetchImpl;
import org.hibernate.spi.DotIdentifierSequence;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.SqlAstJoinType;
import org.hibernate.sql.ast.spi.SqlAliasBase;
import org.hibernate.sql.ast.spi.SqlAstCreationState;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.ast.tree.from.LazyTableGroup;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableGroupJoin;
import org.hibernate.sql.ast.tree.from.TableGroupProducer;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchOptions;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.Fetchable;
import org.hibernate.sql.results.graph.entity.EntityFetch;
import org.hibernate.sql.results.graph.entity.internal.EntityFetchSelectImpl;
import org.hibernate.sql.results.internal.domain.CircularFetchImpl;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.java.MutabilityPlan;

public class ReactiveToOneAttributeMapping extends ToOneAttributeMapping {

	private final ToOneAttributeMapping delegate;

	public ReactiveToOneAttributeMapping(ToOneAttributeMapping delegate) {
		super( delegate );
		this.delegate = delegate;
	}

	@Override
	public EntityFetch generateFetch(
			FetchParent fetchParent,
			NavigablePath fetchablePath,
			FetchTiming fetchTiming,
			boolean selected,
			String resultVariable,
			DomainResultCreationState creationState) {
		EntityFetch entityFetch = delegate.generateFetch(
				fetchParent,
				fetchablePath,
				fetchTiming,
				selected,
				resultVariable,
				creationState
		);
		if (entityFetch instanceof EntityFetchSelectImpl) {
			return new ReactiveEntityFetchSelectImpl( (EntityFetchSelectImpl) entityFetch );
		}
		return entityFetch;
	}

	@Override
	public Fetch resolveCircularFetch(NavigablePath fetchablePath, FetchParent fetchParent, FetchTiming fetchTiming, DomainResultCreationState creationState) {
		Fetch fetch = delegate.resolveCircularFetch( fetchablePath, fetchParent, fetchTiming, creationState );
		if ( fetch instanceof CircularFetchImpl ) {
			return new ReactiveCircularFetchImpl( (CircularFetchImpl) fetch );
		}
		return fetch;
	}

	@Override
	public ReactiveToOneAttributeMapping copy(
			ManagedMappingType declaringType,
			TableGroupProducer declaringTableGroupProducer) {
		return new ReactiveToOneAttributeMapping( delegate.copy( declaringType, declaringTableGroupProducer ) );
	}

	@Override
	public void setForeignKeyDescriptor(ForeignKeyDescriptor foreignKeyDescriptor) {
		delegate.setForeignKeyDescriptor( foreignKeyDescriptor );
	}

	@Override
	public String getIdentifyingColumnsTableExpression() {
		return delegate.getIdentifyingColumnsTableExpression();
	}

	@Override
	public void setIdentifyingColumnsTableExpression(String tableExpression) {
		delegate.setIdentifyingColumnsTableExpression( tableExpression );
	}

	@Override
	public ForeignKeyDescriptor getForeignKeyDescriptor() {
		return delegate.getForeignKeyDescriptor();
	}

	@Override
	public ForeignKeyDescriptor.Nature getSideNature() {
		return delegate.getSideNature();
	}

	@Override
	public boolean isReferenceToPrimaryKey() {
		return delegate.isReferenceToPrimaryKey();
	}

	@Override
	public boolean isFkOptimizationAllowed() {
		return delegate.isFkOptimizationAllowed();
	}

	@Override
	public boolean hasPartitionedSelectionMapping() {
		return delegate.hasPartitionedSelectionMapping();
	}

	@Override
	public String getReferencedPropertyName() {
		return delegate.getReferencedPropertyName();
	}

	@Override
	public String getTargetKeyPropertyName() {
		return delegate.getTargetKeyPropertyName();
	}

	@Override
	public Set<String> getTargetKeyPropertyNames() {
		return delegate.getTargetKeyPropertyNames();
	}

	@Override
	public Cardinality getCardinality() {
		return delegate.getCardinality();
	}

	@Override
	public EntityMappingType getMappedType() {
		return delegate.getMappedType();
	}

	@Override
	public EntityMappingType getEntityMappingType() {
		return delegate.getEntityMappingType();
	}

	@Override
	public NavigableRole getNavigableRole() {
		return delegate.getNavigableRole();
	}

	@Override
	public ModelPart findSubPart(String name) {
		return delegate.findSubPart( name );
	}

	@Override
	public ModelPart findSubPart(String name, EntityMappingType targetType) {
		return delegate.findSubPart( name, targetType );
	}

	@Override
	public boolean isBidirectionalAttributeName(
			NavigablePath parentNavigablePath,
			ModelPart parentModelPart,
			NavigablePath fetchablePath,
			DomainResultCreationState creationState) {
		return super.isBidirectionalAttributeName(
				parentNavigablePath,
				parentModelPart,
				fetchablePath,
				creationState
		);
	}
	@Override
	public <T> DomainResult<T> createSnapshotDomainResult(
			NavigablePath navigablePath,
			TableGroup tableGroup,
			String resultVariable,
			DomainResultCreationState creationState) {
		return delegate.createSnapshotDomainResult( navigablePath, tableGroup, resultVariable, creationState );
	}

	@Override
	public TableGroup createTableGroupInternal(
			boolean canUseInnerJoins,
			NavigablePath navigablePath,
			boolean fetched,
			String sourceAlias,
			SqlAliasBase sqlAliasBase,
			SqlAstCreationState creationState) {
		return delegate.createTableGroupInternal(
				canUseInnerJoins,
				navigablePath,
				fetched,
				sourceAlias,
				sqlAliasBase,
				creationState
		);
	}

	@Override
	public TableGroupJoin createTableGroupJoin(
			NavigablePath navigablePath,
			TableGroup lhs,
			String explicitSourceAlias,
			SqlAliasBase explicitSqlAliasBase,
			SqlAstJoinType requestedJoinType,
			boolean fetched,
			boolean addsPredicate,
			SqlAstCreationState creationState) {
		return delegate.createTableGroupJoin(
				navigablePath,
				lhs,
				explicitSourceAlias,
				explicitSqlAliasBase,
				requestedJoinType,
				fetched,
				addsPredicate,
				creationState
		);
	}

	@Override
	public LazyTableGroup createRootTableGroupJoin(
			NavigablePath navigablePath,
			TableGroup lhs,
			String explicitSourceAlias,
			SqlAliasBase explicitSqlAliasBase,
			SqlAstJoinType requestedJoinType,
			boolean fetched,
			Consumer<Predicate> predicateConsumer,
			SqlAstCreationState creationState) {
		return delegate.createRootTableGroupJoin(
				navigablePath,
				lhs,
				explicitSourceAlias,
				explicitSqlAliasBase,
				requestedJoinType,
				fetched,
				predicateConsumer,
				creationState
		);
	}

	@Override
	public SqlAstJoinType getDefaultSqlAstJoinType(TableGroup parentTableGroup) {
		return delegate.getDefaultSqlAstJoinType( parentTableGroup );
	}

	@Override
	public boolean isSimpleJoinPredicate(Predicate predicate) {
		return delegate.isSimpleJoinPredicate( predicate );
	}

	@Override
	public int getNumberOfFetchables() {
		return delegate.getNumberOfFetchables();
	}

	@Override
	public Fetchable getFetchable(int position) {
		return delegate.getFetchable( position );
	}

	@Override
	public String getSqlAliasStem() {
		return delegate.getSqlAliasStem();
	}

	@Override
	public boolean isNullable() {
		return delegate.isNullable();
	}

	@Override
	public boolean isOptional() {
		return delegate.isOptional();
	}

	@Override
	public boolean isInternalLoadNullable() {
		return delegate.isInternalLoadNullable();
	}

	@Override
	public NotFoundAction getNotFoundAction() {
		return delegate.getNotFoundAction();
	}

	@Override
	public boolean isIgnoreNotFound() {
		return delegate.isIgnoreNotFound();
	}

	@Override
	public boolean hasNotFoundAction() {
		return delegate.hasNotFoundAction();
	}

	@Override
	public boolean isUnwrapProxy() {
		return delegate.isUnwrapProxy();
	}

	@Override
	public EntityMappingType getAssociatedEntityMappingType() {
		return delegate.getAssociatedEntityMappingType();
	}

	@Override
	public ModelPart getKeyTargetMatchPart() {
		return delegate.getKeyTargetMatchPart();
	}

	@Override
	public String toString() {
		return delegate.toString();
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
	public int forEachSelectable(int offset, SelectableConsumer consumer) {
		return delegate.forEachSelectable( offset, consumer );
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
	public String getContainingTableExpression() {
		return delegate.getContainingTableExpression();
	}

	@Override
	public int getJdbcTypeCount() {
		return delegate.getJdbcTypeCount();
	}

	@Override
	public SelectableMapping getSelectable(int columnIndex) {
		return delegate.getSelectable( columnIndex );
	}

	@Override
	public int forEachJdbcType(int offset, IndexedConsumer<JdbcMapping> action) {
		return delegate.forEachJdbcType( offset, action );
	}

	@Override
	public Object disassemble(Object value, SharedSessionContractImplementor session) {
		return delegate.disassemble( value, session );
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
	public <X, Y> int forEachJdbcValue(
			Object value,
			int offset,
			X x,
			Y y,
			JdbcValuesBiConsumer<X, Y> consumer,
			SharedSessionContractImplementor session) {
		return delegate.forEachJdbcValue( value, offset, x, y, consumer, session );
	}

	@Override
	public PropertyAccess getPropertyAccess() {
		return delegate.getPropertyAccess();
	}

	@Override
	public Generator getGenerator() {
		return delegate.getGenerator();
	}

	@Override
	public int getStateArrayPosition() {
		return delegate.getStateArrayPosition();
	}

	@Override
	public AttributeMetadata getAttributeMetadata() {
		return delegate.getAttributeMetadata();
	}

	@Override
	public String getFetchableName() {
		return delegate.getFetchableName();
	}

	@Override
	public FetchOptions getMappedFetchOptions() {
		return delegate.getMappedFetchOptions();
	}

	@Override
	public FetchStyle getStyle() {
		return delegate.getStyle();
	}

	@Override
	public FetchTiming getTiming() {
		return delegate.getTiming();
	}

	@Override
	public ManagedMappingType getDeclaringType() {
		return delegate.getDeclaringType();
	}

	@Override
	public String getAttributeName() {
		return delegate.getAttributeName();
	}

	@Override
	public int getFetchableKey() {
		return delegate.getFetchableKey();
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
	public String getPartName() {
		return delegate.getPartName();
	}

	@Override
	public Object getValue(Object container) {
		return delegate.getValue( container );
	}

	@Override
	public void setValue(Object container, Object value) {
		delegate.setValue( container, value );
	}

	@Override
	public EntityMappingType findContainingEntityMapping() {
		return delegate.findContainingEntityMapping();
	}

	@Override
	public MutabilityPlan<?> getExposedMutabilityPlan() {
		return delegate.getExposedMutabilityPlan();
	}

	@Override
	public int compare(Object value1, Object value2) {
		return delegate.compare( value1, value2 );
	}

	@Override
	public AttributeMapping asAttributeMapping() {
		return delegate.asAttributeMapping();
	}

	@Override
	public PluralAttributeMapping asPluralAttributeMapping() {
		return delegate.asPluralAttributeMapping();
	}

	@Override
	public boolean isPluralAttributeMapping() {
		return delegate.isPluralAttributeMapping();
	}

	@Override
	public EmbeddedAttributeMapping asEmbeddedAttributeMapping() {
		return delegate.asEmbeddedAttributeMapping();
	}

	@Override
	public boolean isEmbeddedAttributeMapping() {
		return delegate.isEmbeddedAttributeMapping();
	}

	@Override
	public List<JdbcMapping> getJdbcMappings() {
		return delegate.getJdbcMappings();
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
	public int forEachSelectable(SelectableConsumer consumer) {
		return delegate.forEachSelectable( consumer );
	}

	@Override
	public void forEachInsertable(SelectableConsumer consumer) {
		delegate.forEachInsertable( consumer );
	}

	@Override
	public void forEachUpdatable(SelectableConsumer consumer) {
		delegate.forEachUpdatable( consumer );
	}

	@Override
	public boolean isVirtual() {
		return delegate.isVirtual();
	}

	@Override
	public boolean isEntityIdentifierMapping() {
		return delegate.isEntityIdentifierMapping();
	}

	@Override
	public <T> DomainResult<T> createDomainResult(
			NavigablePath navigablePath,
			TableGroup tableGroup,
			String resultVariable,
			DomainResultCreationState creationState) {
		return delegate.createDomainResult( navigablePath, tableGroup, resultVariable, creationState );
	}

	@Override
	public int breakDownJdbcValues(
			Object domainValue,
			JdbcValueConsumer valueConsumer,
			SharedSessionContractImplementor session) {
		return delegate.breakDownJdbcValues( domainValue, valueConsumer, session );
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
	public boolean areEqual(Object one, Object other, SharedSessionContractImplementor session) {
		return delegate.areEqual( one, other, session );
	}

	@Override
	public int forEachJdbcType(IndexedConsumer<JdbcMapping> action) {
		return delegate.forEachJdbcType( action );
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
	public JavaType<?> getExpressibleJavaType() {
		return delegate.getExpressibleJavaType();
	}

	@Override
	public <X> X treatAs(Class<X> targetType) {
		return delegate.treatAs( targetType );
	}

	@Override
	public boolean incrementFetchDepth() {
		return delegate.incrementFetchDepth();
	}

	@Override
	public void forEachSubPart(IndexedConsumer<ModelPart> consumer, EntityMappingType treatTarget) {
		delegate.forEachSubPart( consumer, treatTarget );
	}

	@Override
	public void visitSubParts(Consumer<ModelPart> consumer, EntityMappingType targetType) {
		delegate.visitSubParts( consumer, targetType );
	}

	@Override
	public int getNumberOfKeyFetchables() {
		return delegate.getNumberOfKeyFetchables();
	}

	@Override
	public int getNumberOfFetchableKeys() {
		return delegate.getNumberOfFetchableKeys();
	}

	@Override
	public Fetchable getKeyFetchable(int position) {
		return delegate.getKeyFetchable( position );
	}

	@Override
	public void visitKeyFetchables(Consumer<? super Fetchable> fetchableConsumer, EntityMappingType treatTargetType) {
		delegate.visitKeyFetchables( fetchableConsumer, treatTargetType );
	}

	@Override
	public void visitKeyFetchables(
			IndexedConsumer<? super Fetchable> fetchableConsumer,
			EntityMappingType treatTargetType) {
		delegate.visitKeyFetchables( fetchableConsumer, treatTargetType );
	}

	@Override
	public void visitKeyFetchables(
			int offset,
			IndexedConsumer<? super Fetchable> fetchableConsumer,
			EntityMappingType treatTargetType) {
		delegate.visitKeyFetchables( offset, fetchableConsumer, treatTargetType );
	}

	@Override
	public void visitFetchables(Consumer<? super Fetchable> fetchableConsumer, EntityMappingType treatTargetType) {
		delegate.visitFetchables( fetchableConsumer, treatTargetType );
	}

	@Override
	public void visitFetchables(
			IndexedConsumer<? super Fetchable> fetchableConsumer,
			EntityMappingType treatTargetType) {
		delegate.visitFetchables( fetchableConsumer, treatTargetType );
	}

	@Override
	public void visitFetchables(
			int offset,
			IndexedConsumer<? super Fetchable> fetchableConsumer,
			EntityMappingType treatTargetType) {
		delegate.visitFetchables( offset, fetchableConsumer, treatTargetType );
	}

	@Override
	public int getSelectableIndex(String selectableName) {
		return delegate.getSelectableIndex( selectableName );
	}

	@Override
	public void forEachSubPart(IndexedConsumer<ModelPart> consumer) {
		delegate.forEachSubPart( consumer );
	}

	@Override
	public ModelPart findByPath(String path) {
		return delegate.findByPath( path );
	}

	@Override
	public ModelPart findByPath(DotIdentifierSequence path) {
		return delegate.findByPath( path );
	}

	@Override
	public boolean containsTableReference(String tableExpression) {
		return delegate.containsTableReference( tableExpression );
	}
}
