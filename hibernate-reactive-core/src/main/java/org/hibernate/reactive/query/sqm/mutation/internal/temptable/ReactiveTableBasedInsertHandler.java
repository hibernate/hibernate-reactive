/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.Generator;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.id.insert.Binder;
import org.hibernate.internal.util.MutableObject;
import org.hibernate.metamodel.mapping.BasicEntityIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityIdentifierMapping;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.query.sqm.mutation.internal.temptable.TableBasedInsertHandler;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.id.insert.ReactiveInsertGeneratedIdentifierDelegate;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.exec.internal.JdbcParameterBindingImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.type.descriptor.ValueBinder;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;

public class ReactiveTableBasedInsertHandler extends TableBasedInsertHandler implements ReactiveHandler {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveTableBasedInsertHandler(
			SqmInsertStatement<?> sqmInsert,
			DomainParameterXref domainParameterXref,
			TemporaryTable entityTable,
			TemporaryTableStrategy temporaryTableStrategy,
			boolean forceDropAfterUse,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		super( sqmInsert, domainParameterXref, entityTable, temporaryTableStrategy, forceDropAfterUse, sessionUidAccess, context, firstJdbcParameterBindingsConsumer );
	}

	@Override
	public CompletionStage<Integer> reactiveExecute(
			JdbcParameterBindings jdbcParameterBindings,
			DomainQueryExecutionContext context) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracef(
					"Starting multi-table insert execution - %s",
					getSqmStatement().getTarget().getModel().getName()
			);
		}

		final SqmJdbcExecutionContextAdapter executionContext = SqmJdbcExecutionContextAdapter.omittingLockingAndPaging( context );
		// NOTE: we could get rid of using a temporary table if the expressions in Values are "stable".
		// But that is a non-trivial optimization that requires more effort
		// as we need to split out individual inserts if we have a non-bulk capable optimizer
		return ReactiveExecuteWithTemporaryTableHelper.performBeforeTemporaryTableUseActions(
				getEntityTable(),
				getTemporaryTableStrategy(),
				executionContext
		).thenCompose( createdTable ->
			ReactiveExecuteWithTemporaryTableHelper.saveIntoTemporaryTable(
					getTemporaryTableInsert().jdbcOperation(),
					jdbcParameterBindings,
					executionContext
			).thenCompose( rows -> {
				if ( rows != 0 ) {
					final JdbcParameterBindings sessionUidBindings = new JdbcParameterBindingsImpl( 1 );
					final JdbcParameter sessionUidParameter = getSessionUidParameter();
					if ( sessionUidParameter != null ) {
						sessionUidBindings.addBinding(
								sessionUidParameter,
								new JdbcParameterBindingImpl(
										sessionUidParameter.getExpressionType().getSingleJdbcMapping(),
										UUID.fromString( getSessionUidAccess().apply( executionContext.getSession() ) )
								)
						);
					}
					return insertRootTable(	rows, createdTable, sessionUidBindings, executionContext )
							.thenCompose( insertedRows -> CompletionStages
									.loop(
											getNonRootTableInserts(), nonRootTableInsert ->
													insertTable( nonRootTableInsert, sessionUidBindings, executionContext )
									).thenApply( v -> insertedRows )
					 );
				}
				return CompletionStages.completedFuture( rows );
			} ) )
				.handle( CompletionStages::handle )
				.thenCompose( handler -> ReactiveExecuteWithTemporaryTableHelper
						.performAfterTemporaryTableUseActions(
								getEntityTable(),
								getSessionUidAccess(),
								getAfterUseAction(),
								executionContext
						)
						.thenCompose( v -> handler.getResultAsCompletionStage() )
				);
	}

	private CompletionStage<Void> insertTable(
			JdbcOperationQueryMutation nonRootTableInsert,
			JdbcParameterBindings sessionUidBindings,
			ExecutionContext executionContext) {
		return StandardReactiveJdbcMutationExecutor.INSTANCE
				.executeReactive(
						nonRootTableInsert,
						sessionUidBindings,
						sql -> executionContext.getSession()
								.getJdbcCoordinator()
								.getStatementPreparer()
								.prepareStatement( sql ),
						(integer, preparedStatement) -> {
						},
						executionContext
				).thenCompose( unused -> CompletionStages.voidFuture() );
	}

	private CompletionStage<Integer> insertRootTable(
			int rows,
			boolean rowNumberStartsAtOne,
			JdbcParameterBindings sessionUidBindings,
			SqmJdbcExecutionContextAdapter executionContext) {
		final EntityPersister entityPersister = getEntityDescriptor().getEntityPersister();
		final Generator generator = entityPersister.getGenerator();
		final EntityIdentifierMapping identifierMapping = entityPersister.getIdentifierMapping();

		final SharedSessionContractImplementor session = executionContext.getSession();
		final RootTableInserter rootTableInserter = getRootTableInserter();

		if ( rootTableInserter.temporaryTableIdentitySelect() != null ) {
			return StandardReactiveSelectExecutor.INSTANCE.list(
					rootTableInserter.temporaryTableIdentitySelect(),
					sessionUidBindings,
					executionContext,
					null,
					null,
					ReactiveListResultsConsumer.UniqueSemantic.NONE,
					rows
			).thenApply( list -> {
				Map<Object, Object> entityTableToRootIdentity = new LinkedHashMap<>( list.size() );
				for ( Object o : list ) {
					entityTableToRootIdentity.put( o, null );
				}
				return entityTableToRootIdentity;
			} ).thenCompose( entityTableToRootIdentity -> insertRootTable(
					sessionUidBindings,
					executionContext,
					rootTableInserter,
					entityPersister,
					identifierMapping,
					entityTableToRootIdentity,
					session
			) );
		}
		else {
			final Map<Object, Object> entityTableToRootIdentity = null;

			if ( rootTableInserter.temporaryTableIdUpdate() != null ) {
				final BeforeExecutionGenerator beforeExecutionGenerator = (BeforeExecutionGenerator) generator;
				final JdbcParameterBindings updateBindings = new JdbcParameterBindingsImpl( 3 );
				final JdbcParameter sessionUidParameter = getSessionUidParameter();
				if ( sessionUidParameter != null ) {
					updateBindings.addBinding(
							sessionUidParameter,
							new JdbcParameterBindingImpl(
									sessionUidParameter.getExpressionType().getSingleJdbcMapping(),
									UUID.fromString( getSessionUidAccess().apply( session ) )
							)
					);
				}
				final List<JdbcParameterBinder> parameterBinders = rootTableInserter.temporaryTableIdUpdate().getParameterBinders();
				final JdbcParameter rootIdentity = (JdbcParameter) parameterBinders.get( 0 );
				final JdbcParameter rowNumber = (JdbcParameter) parameterBinders.get( 1 );
				final BasicEntityIdentifierMapping basicIdentifierMapping = (BasicEntityIdentifierMapping) identifierMapping;

				if ( !rowNumberStartsAtOne ) {
					return ReactiveExecuteWithTemporaryTableHelper.loadInsertedRowNumbers(
							rootTableInserter.temporaryTableRowNumberSelectSql(),
							getEntityTable(),
							getSessionUidAccess(),
							rows,
							executionContext
					).thenCompose( rowNumbers ->
						forEachRow(
								rowNumbers,
								executionContext,
								updateBindings,
								rowNumber,
								rootIdentity,
								basicIdentifierMapping,
								beforeExecutionGenerator,
								session,
								rootTableInserter
						).thenCompose( v -> insertRootTable(
								sessionUidBindings,
								executionContext,
								rootTableInserter,
								entityPersister,
								identifierMapping,
								entityTableToRootIdentity,
								session )
						)
					);
				}
				else {
					final Integer[] rowNumbers = IntStream.range( 1, rows + 1 ).boxed()
							.toArray(Integer[]::new);
					return forEachRow(
							rowNumbers,
							executionContext,
							updateBindings,
							rowNumber,
							rootIdentity,
							basicIdentifierMapping,
							beforeExecutionGenerator,
							session,
							rootTableInserter
					).thenCompose( v -> insertRootTable(
							sessionUidBindings,
							executionContext,
							rootTableInserter,
							entityPersister,
							identifierMapping,
							entityTableToRootIdentity,
							session)
					);
				}
			}
			return insertRootTable(
					sessionUidBindings,
					executionContext,
					rootTableInserter,
					entityPersister,
					identifierMapping,
					entityTableToRootIdentity,
					session
			);
		}
	}

	private static CompletionStage<Void> forEachRow(
			Integer[] rowNumbers,
			SqmJdbcExecutionContextAdapter executionContext,
			JdbcParameterBindings updateBindings,
			JdbcParameter rowNumber,
			JdbcParameter rootIdentity,
			BasicEntityIdentifierMapping basicIdentifierMapping,
			BeforeExecutionGenerator beforeExecutionGenerator,
			SharedSessionContractImplementor session,
			RootTableInserter rootTableInserter) {
		return loop( rowNumbers, rowNumberValue -> {
			updateBindings.addBinding(
					rowNumber,
					new JdbcParameterBindingImpl(
							rowNumber.getExpressionType().getSingleJdbcMapping(),
							rowNumberValue
					)
			);
			updateBindings.addBinding(
					rootIdentity,
					new JdbcParameterBindingImpl(
							basicIdentifierMapping.getJdbcMapping(),
							beforeExecutionGenerator.generate( session, null, null, INSERT )
					)
			);
			return StandardReactiveJdbcMutationExecutor.INSTANCE.executeReactive(
					rootTableInserter.temporaryTableIdUpdate(),
					updateBindings,
					sql -> session
							.getJdbcCoordinator()
							.getStatementPreparer()
							.prepareStatement( sql ),
					(integer, preparedStatement) -> {
					},
					executionContext
			).thenApply( updateCount -> {
				assert updateCount == 1;
				return updateCount;
			} );
	} );
	}

	private CompletionStage<Integer> insertRootTable(
			JdbcParameterBindings sessionUidBindings,
			SqmJdbcExecutionContextAdapter executionContext,
			RootTableInserter rootTableInserter,
			EntityPersister entityPersister,
			EntityIdentifierMapping identifierMapping,
			Map<Object, Object> entityTableToRootIdentity,
			SharedSessionContractImplementor session ) {
		if ( rootTableInserter.rootTableInsertWithReturningSql() != null ) {
			final GeneratedValuesMutationDelegate insertDelegate = entityPersister.getEntityPersister().getInsertDelegate();
			final BasicEntityIdentifierMapping basicIdentifierMapping = (BasicEntityIdentifierMapping) identifierMapping;
			// todo 7.0 : InsertGeneratedIdentifierDelegate will be removed once we're going to handle
			//            generated values within the jdbc insert operaetion itself
			final ReactiveInsertGeneratedIdentifierDelegate identifierDelegate = (ReactiveInsertGeneratedIdentifierDelegate) insertDelegate;
			final ValueBinder jdbcValueBinder = basicIdentifierMapping.getJdbcMapping().getJdbcValueBinder();
			return loop(entityTableToRootIdentity.entrySet() , entry ->
				identifierDelegate.reactivePerformInsertReturning(
						rootTableInserter.rootTableInsertWithReturningSql(),
						session,
						new Binder() {
							@Override
							public void bindValues(PreparedStatement ps) throws SQLException {
								jdbcValueBinder.bind( ps, entry.getKey(), 1, session );
								final JdbcParameter sessionUidParameter = getSessionUidParameter();
								if ( sessionUidParameter != null ) {
									sessionUidParameter.getParameterBinder().bindParameterValue(
											ps,
											2,
											sessionUidBindings,
											executionContext
									);
								}
							}

							@Override
							public Object getEntity() {
								return null;
							}
						}
				).thenAccept( generatedValues -> {
					entry.setValue( generatedValues.getGeneratedValue( identifierMapping ) );
				} )
			).thenCompose( unused -> {
				final JdbcParameterBindings updateBindings = new JdbcParameterBindingsImpl( 2 );

				final List<JdbcParameterBinder> parameterBinders = rootTableInserter.temporaryTableIdentityUpdate()
						.getParameterBinders();
				final JdbcParameter rootIdentity = (JdbcParameter) parameterBinders.get( 0 );
				final JdbcParameter entityIdentity = (JdbcParameter) parameterBinders.get( 1 );
				return loop(entityTableToRootIdentity.entrySet(), entry -> {
					JdbcMapping jdbcMapping = basicIdentifierMapping.getJdbcMapping();
					updateBindings.addBinding(
							entityIdentity,
							new JdbcParameterBindingImpl( jdbcMapping, entry.getKey() )
					);
					updateBindings.addBinding(
							rootIdentity,
							new JdbcParameterBindingImpl( jdbcMapping, entry.getValue() )
					);
					return StandardReactiveJdbcMutationExecutor.INSTANCE.executeReactive(
							rootTableInserter.temporaryTableIdentityUpdate(),
							updateBindings,
							sql -> session
									.getJdbcCoordinator()
									.getStatementPreparer()
									.prepareStatement( sql ),
							(integer, preparedStatement) -> {
							},
							executionContext
					);
				}).thenApply( v ->  entityTableToRootIdentity.size() );
			});
		}
		else {
			return StandardReactiveJdbcMutationExecutor.INSTANCE.executeReactive(
					rootTableInserter.rootTableInsert(),
					sessionUidBindings,
					sql -> session
							.getJdbcCoordinator()
							.getStatementPreparer()
							.prepareStatement( sql ),
					(integer, preparedStatement) -> {
					},
					executionContext
			);
		}
	}

}
