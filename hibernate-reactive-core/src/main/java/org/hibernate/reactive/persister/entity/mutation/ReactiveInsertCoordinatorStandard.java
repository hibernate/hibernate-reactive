/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.Internal;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.batch.internal.BasicBatchKey;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.internal.NoBatchKeyAccess;
import org.hibernate.engine.jdbc.mutation.spi.BatchKeyAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.Generator;
import org.hibernate.generator.OnExecutionGenerator;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.AttributeMappingsList;
import org.hibernate.metamodel.mapping.BasicEntityIdentifierMapping;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.AbstractMutationCoordinator;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.persister.entity.mutation.InsertCoordinatorStandard;
import org.hibernate.persister.entity.mutation.InsertCoordinatorStandard.InsertValuesAnalysis;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.model.ast.builder.MutationGroupBuilder;
import org.hibernate.sql.model.ast.builder.TableInsertBuilder;
import org.hibernate.sql.model.ast.builder.TableInsertBuilderStandard;
import org.hibernate.sql.model.ast.builder.TableMutationBuilder;
import org.hibernate.tuple.entity.EntityMetamodel;

import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.reactive.persister.entity.mutation.GeneratorValueUtil.generateValue;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see InsertCoordinatorStandard
 */
@Internal
public class ReactiveInsertCoordinatorStandard extends AbstractMutationCoordinator implements ReactiveInsertCoordinator,
		InsertCoordinator {
	private final MutationOperationGroup staticInsertGroup;
	private final BasicBatchKey batchKey;

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveInsertCoordinatorStandard(AbstractEntityPersister entityPersister, SessionFactoryImplementor factory) {
		super( entityPersister, factory );

		if ( entityPersister.isIdentifierAssignedByInsert() || entityPersister.hasInsertGeneratedProperties() ) {
			// disable batching in case of insert generated identifier or properties
			batchKey = null;
		}
		else {
			batchKey = new BasicBatchKey( entityPersister.getEntityName() + "#INSERT" );
		}

		if ( entityPersister.getEntityMetamodel().isDynamicInsert() ) {
			// the entity specified dynamic-insert - skip generating the
			// static inserts as we will create them every time
			staticInsertGroup = null;
		}
		else {
			staticInsertGroup = generateStaticOperationGroup();
		}

	}

	@Override
	public GeneratedValues insert(
			Object entity,
			Object[] values,
			SharedSessionContractImplementor session) {
		return insert( entity, null, values, session );
	}

	@Override
	public GeneratedValues insert(
			Object entity,
			Object id,
			Object[] values,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveInsert" );
	}

	@Override
	public CompletionStage<GeneratedValues> reactiveInsert(
			Object entity,
			Object[] values,
			SharedSessionContractImplementor session) {
		return reactiveInsert( entity, null, values, session );
	}

	@Override
	public CompletionStage<GeneratedValues> reactiveInsert(
			Object entity,
			Object id,
			Object[] values,
			SharedSessionContractImplementor session) {
		return coordinateReactiveInsert( entity, id, values, session, true );
	}

	public CompletionStage<GeneratedValues> coordinateReactiveInsert(
			Object entity,
			Object id,
			Object[] values,
			SharedSessionContractImplementor session,
			boolean isIdentityInsert) {
		return reactivePreInsertInMemoryValueGeneration( values, entity, session )
				.thenCompose( needsDynamicInsert -> {
					final boolean forceIdentifierBinding = entityPersister().getGenerator().generatedOnExecution() && id != null;
					return entityPersister().getEntityMetamodel().isDynamicInsert() || needsDynamicInsert || forceIdentifierBinding
						? doDynamicInserts( id, values, entity, session, forceIdentifierBinding, isIdentityInsert )
						: doStaticInserts( id, values, entity, session, isIdentityInsert );
				} );
	}

	private CompletionStage<Boolean> reactivePreInsertInMemoryValueGeneration(Object[] currentValues, Object entity, SharedSessionContractImplementor session) {
		final EntityMetamodel entityMetamodel = entityPersister().getEntityMetamodel();
		CompletionStage<Boolean> stage = falseFuture();
		if ( entityMetamodel.hasPreInsertGeneratedValues() ) {
			final Generator[] generators = entityMetamodel.getGenerators();
			for ( int i = 0; i < generators.length; i++ ) {
				final int index = i;
				final Generator generator = generators[i];
				if ( generator != null
						&& !generator.generatedOnExecution()
						&& generator.generatesOnInsert() ) {
					final Object currentValue = currentValues[i];
					final BeforeExecutionGenerator beforeGenerator = (BeforeExecutionGenerator) generator;
					stage = stage
							.thenCompose( foundStateDependentGenerator -> generateValue(
												  session,
												  entity,
												  currentValue,
												  beforeGenerator,
												  INSERT
										  )
												  .thenApply( generatedValue -> {
													  currentValues[index] = generatedValue;
													  entityPersister().setValue( entity, index, generatedValue );
													  return foundStateDependentGenerator || beforeGenerator.generatedOnExecution();
												  } )
							);
				}
			}
		}

		return stage;
	}

	protected CompletionStage<Void> decomposeForReactiveInsert(
			MutationExecutor mutationExecutor,
			Object id,
			Object[] values,
			MutationOperationGroup mutationGroup,
			boolean[] propertyInclusions,
			TableInclusionChecker tableInclusionChecker,
			SharedSessionContractImplementor session) {
		final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();
		mutationGroup.forEachOperation( (position, operation) -> {
			final EntityTableMapping tableDetails = (EntityTableMapping) operation.getTableDetails();
			if ( tableInclusionChecker.include( tableDetails ) ) {
				final int[] attributeIndexes = tableDetails.getAttributeIndexes();
				for ( final int attributeIndex : attributeIndexes ) {
					if ( propertyInclusions[attributeIndex] ) {
						final AttributeMapping mapping = entityPersister().getAttributeMappings().get( attributeIndex );
						decomposeAttribute( values[attributeIndex], session, jdbcValueBindings, mapping );
					}
				}
			}
		} );

		mutationGroup.forEachOperation( (position, jdbcOperation) -> {
			if ( id == null )  {
				assert entityPersister().getIdentityInsertDelegate() != null;
			}
			else {
				final EntityTableMapping tableDetails = (EntityTableMapping) jdbcOperation.getTableDetails();
				breakDownJdbcValue( id, session, jdbcValueBindings, tableDetails );
			}
		} );
		return voidFuture();
	}

	protected CompletionStage<GeneratedValues> doDynamicInserts(
			Object id,
			Object[] values,
			Object object,
			SharedSessionContractImplementor session,
			boolean forceIdentifierBinding,
			boolean isIdentityInsert) {
		final boolean[] insertability = getPropertiesToInsert( values );
		final MutationOperationGroup insertGroup = generateDynamicInsertSqlGroup( insertability, object, session, forceIdentifierBinding );
		final ReactiveMutationExecutor mutationExecutor = getReactiveMutationExecutor( session, insertGroup, true );

		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );
		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );
		return decomposeForReactiveInsert( mutationExecutor, id, values, insertGroup, insertability, tableInclusionChecker, session )
				.thenCompose( v -> mutationExecutor.executeReactive(
								object,
								insertValuesAnalysis,
								tableInclusionChecker,
								(statementDetails, affectedRowCount, batchPosition) -> {
									statementDetails.getExpectation()
											.verifyOutcome(
													affectedRowCount,
													statementDetails.getStatement(),
													batchPosition,
													statementDetails.getSqlString()
											);
									return true;
								},
								session,
								isIdentityInsert,
								entityPersister().getIdentifierColumnNames()
						)
						.whenComplete( (o, t) -> mutationExecutor.release() ) );
	}

	protected CompletionStage<GeneratedValues> doStaticInserts(Object id, Object[] values, Object object, SharedSessionContractImplementor session, boolean isIdentityInsert) {
		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );
		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );
		final ReactiveMutationExecutor mutationExecutor = getReactiveMutationExecutor( session, staticInsertGroup, false );

		return decomposeForReactiveInsert( mutationExecutor, id, values, staticInsertGroup, entityPersister().getPropertyInsertability(), tableInclusionChecker, session )
				.thenCompose( v -> mutationExecutor.executeReactive(
						object,
						insertValuesAnalysis,
						tableInclusionChecker,
						(statementDetails, affectedRowCount, batchPosition) -> {
							statementDetails.getExpectation().verifyOutcome( affectedRowCount, statementDetails.getStatement(), batchPosition, statementDetails.getSqlString() );
							return true;
						},
						session,
						isIdentityInsert,
						entityPersister().getIdentifierColumnNames()
				) )
				.whenComplete( (generatedValues, throwable) -> mutationExecutor.release() );
	}


	protected static TableInclusionChecker getTableInclusionChecker(InsertValuesAnalysis insertValuesAnalysis) {
		return tableMapping -> !tableMapping.isOptional() || insertValuesAnalysis.hasNonNullBindings( tableMapping );
	}

	private ReactiveMutationExecutor getReactiveMutationExecutor(SharedSessionContractImplementor session, MutationOperationGroup operationGroup, boolean dynamicUpdate) {
		return (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor( resolveBatchKeyAccess( dynamicUpdate, session ), operationGroup, session );
	}

	@Override
	protected BatchKeyAccess resolveBatchKeyAccess(boolean dynamicUpdate, SharedSessionContractImplementor session) {
		if ( !dynamicUpdate
				&& !entityPersister().optimisticLockStyle().isAllOrDirty()
				&& session.getTransactionCoordinator() != null
//				&& session.getTransactionCoordinator().isTransactionActive()
		) {
			return this::getBatchKey;
		}

		return NoBatchKeyAccess.INSTANCE;
	}

	/*
	 * BEGIN Copied from InsertCoordinatorStandard
 	 */
	@Override
	public BasicBatchKey getBatchKey() {
		return batchKey;
	}

	@Override
	@Deprecated
	public MutationOperationGroup getStaticMutationOperationGroup() {
		return staticInsertGroup;
	}

	protected void decomposeAttribute(
			Object value,
			SharedSessionContractImplementor session,
			JdbcValueBindings jdbcValueBindings,
			AttributeMapping mapping) {
		if ( !(mapping instanceof PluralAttributeMapping ) ) {
			mapping.decompose(
					value,
					0,
					jdbcValueBindings,
					null,
					(valueIndex, bindings, noop, jdbcValue, selectableMapping) -> {
						if ( selectableMapping.isInsertable() ) {
							bindings.bindValue(
									jdbcValue,
									entityPersister().physicalTableNameForMutation( selectableMapping ),
									selectableMapping.getSelectionExpression(),
									ParameterUsage.SET
							);
						}
					},
					session
			);
		}
	}

	/**
	 * Transform the array of property indexes to an array of booleans,
	 * true when the property is insertable and non-null
	 */
	public boolean[] getPropertiesToInsert(Object[] fields) {
		boolean[] notNull = new boolean[fields.length];
		boolean[] insertable = entityPersister().getPropertyInsertability();
		for ( int i = 0; i < fields.length; i++ ) {
			notNull[i] = insertable[i] && fields[i] != null;
		}
		return notNull;
	}

	protected MutationOperationGroup generateDynamicInsertSqlGroup(
			boolean[] insertable,
			Object object,
			SharedSessionContractImplementor session,
			boolean forceIdentifierBinding) {
		final MutationGroupBuilder insertGroupBuilder = new MutationGroupBuilder( MutationType.INSERT, entityPersister() );
		entityPersister().forEachMutableTable(
				(tableMapping) -> insertGroupBuilder.addTableDetailsBuilder( createTableInsertBuilder( tableMapping, forceIdentifierBinding ) )
		);
		applyTableInsertDetails( insertGroupBuilder, insertable, object, session, forceIdentifierBinding );
		return createOperationGroup( null, insertGroupBuilder.buildMutationGroup() );
	}

	public MutationOperationGroup generateStaticOperationGroup() {
		final MutationGroupBuilder insertGroupBuilder = new MutationGroupBuilder( MutationType.INSERT, entityPersister() );
		entityPersister().forEachMutableTable(
				(tableMapping) -> insertGroupBuilder.addTableDetailsBuilder( createTableInsertBuilder( tableMapping, false ) )
		);
		applyTableInsertDetails( insertGroupBuilder, entityPersister().getPropertyInsertability(), null, null, false );
		return createOperationGroup( null, insertGroupBuilder.buildMutationGroup() );
	}

	private TableMutationBuilder<?> createTableInsertBuilder(EntityTableMapping tableMapping, boolean forceIdentifierBinding) {
		final GeneratedValuesMutationDelegate delegate = entityPersister().getInsertDelegate();
		if ( tableMapping.isIdentifierTable() && delegate != null && !forceIdentifierBinding ) {
			return delegate.createTableMutationBuilder( tableMapping.getInsertExpectation(), factory() );
		}
		else {
			return new TableInsertBuilderStandard( entityPersister(), tableMapping, factory() );
		}
	}

	private void applyTableInsertDetails(
			MutationGroupBuilder insertGroupBuilder,
			boolean[] attributeInclusions,
			Object object,
			SharedSessionContractImplementor session,
			boolean forceIdentifierBinding) {
		final AttributeMappingsList attributeMappings = entityPersister().getAttributeMappings();

		insertGroupBuilder.forEachTableMutationBuilder( (builder) -> {
			final EntityTableMapping tableMapping = (EntityTableMapping) builder.getMutatingTable().getTableMapping();
			assert !tableMapping.isInverse();

			// `attributeIndexes` represents the indexes (relative to `attributeMappings`) of
			// the attributes mapped to the table
			final int[] attributeIndexes = tableMapping.getAttributeIndexes();
			for ( int i = 0; i < attributeIndexes.length; i++ ) {
				final int attributeIndex = attributeIndexes[ i ];
				final AttributeMapping attributeMapping = attributeMappings.get( attributeIndex );
				if ( attributeInclusions[attributeIndex] ) {
					attributeMapping.forEachInsertable( insertGroupBuilder );
				}
				else {
					final Generator generator = attributeMapping.getGenerator();
					if ( isValueGenerated( generator ) ) {
						if ( session != null && !generator.generatedOnExecution( object, session ) ) {
							attributeInclusions[attributeIndex] = true;
							attributeMapping.forEachInsertable( insertGroupBuilder );
						}
						else if ( isValueGenerationInSql( generator, factory().getJdbcServices().getDialect() ) ) {
							handleValueGeneration( attributeMapping, insertGroupBuilder, (OnExecutionGenerator) generator );
						}
					}
				}
			}
		} );

		// add the discriminator
		entityPersister().addDiscriminatorToInsertGroup( insertGroupBuilder );
		entityPersister().addSoftDeleteToInsertGroup( insertGroupBuilder );

		// add the keys
		insertGroupBuilder.forEachTableMutationBuilder( (tableMutationBuilder) -> {
			final TableInsertBuilder tableInsertBuilder = (TableInsertBuilder) tableMutationBuilder;
			final EntityTableMapping tableMapping = (EntityTableMapping) tableInsertBuilder.getMutatingTable().getTableMapping();
			if ( tableMapping.isIdentifierTable() && entityPersister().isIdentifierAssignedByInsert() && !forceIdentifierBinding ) {
				assert entityPersister().getInsertDelegate() != null;
				final OnExecutionGenerator generator = (OnExecutionGenerator) entityPersister().getGenerator();
				if ( generator.referenceColumnsInSql( dialect() ) ) {
					final BasicEntityIdentifierMapping identifierMapping = (BasicEntityIdentifierMapping) entityPersister().getIdentifierMapping();
					final String[] columnValues = generator.getReferencedColumnValues( dialect );
					tableMapping.getKeyMapping().forEachKeyColumn( (i, column) -> tableInsertBuilder.addKeyColumn(
							column.getColumnName(),
							columnValues[i],
							identifierMapping.getJdbcMapping()
					) );
				}
			}
			else {
				tableMapping.getKeyMapping().forEachKeyColumn( tableInsertBuilder::addKeyColumn );
			}
		} );
	}

	protected void breakDownJdbcValue(
			Object id,
			SharedSessionContractImplementor session,
			JdbcValueBindings jdbcValueBindings,
			EntityTableMapping tableDetails) {
		final String tableName = tableDetails.getTableName();
		tableDetails.getKeyMapping().breakDownKeyJdbcValues(
				id,
				(jdbcValue, columnMapping) -> {
					jdbcValueBindings.bindValue(
							jdbcValue,
							tableName,
							columnMapping.getColumnName(),
							ParameterUsage.SET
					);
				},
				session
		);
	}

	private static boolean isValueGenerated(Generator generator) {
		return generator != null
				&& generator.generatesOnInsert()
				&& generator.generatedOnExecution();
	}

	private static boolean isValueGenerationInSql(Generator generator, Dialect dialect) {
		assert isValueGenerated( generator );
		return ( (OnExecutionGenerator) generator ).referenceColumnsInSql(dialect);
	}

	/*
	 * END Copied from InsertCoordinatorStandard
	 */
}
