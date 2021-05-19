/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PostLoadEvent;
import org.hibernate.event.spi.PreLoadEvent;
import org.hibernate.loader.entity.plan.AbstractLoadPlanBasedEntityLoader;
import org.hibernate.loader.plan.exec.internal.EntityLoadQueryDetails;
import org.hibernate.loader.plan.exec.process.internal.AbstractRowReader;
import org.hibernate.loader.plan.exec.process.internal.HydratedEntityRegistration;
import org.hibernate.loader.plan.exec.process.internal.ResultSetProcessingContextImpl;
import org.hibernate.loader.plan.exec.process.internal.ResultSetProcessorImpl;
import org.hibernate.loader.plan.exec.process.spi.ReaderCollector;
import org.hibernate.loader.plan.exec.query.internal.QueryBuildingParametersImpl;
import org.hibernate.loader.plan.exec.query.spi.NamedParameterContext;
import org.hibernate.loader.plan.exec.query.spi.QueryBuildingParameters;
import org.hibernate.loader.plan.exec.spi.AliasResolutionContext;
import org.hibernate.loader.plan.spi.LoadPlan;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.loader.ReactiveLoader;
import org.hibernate.reactive.loader.ReactiveResultSetProcessor;
import org.hibernate.reactive.loader.entity.ReactiveUniqueEntityLoader;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * An entity loader that respects the JPA {@link javax.persistence.EntityGraph}
 * in effect.
 *
 * @see AbstractLoadPlanBasedEntityLoader
 *
 * @author Gavin King
 */
public class ReactivePlanEntityLoader extends AbstractLoadPlanBasedEntityLoader
		//can't extend org.hibernate.loader.entity.plan.EntityLoader which has private constructors
		implements ReactiveUniqueEntityLoader, ReactiveLoader {

	private final OuterJoinLoadable persister;
	private final Parameters parameters;
	private final String processedSQL;

	public static class Builder {
		private final OuterJoinLoadable persister;
		private ReactivePlanEntityLoader entityLoaderTemplate;
		private int batchSize = 1;
		private LoadQueryInfluencers influencers = LoadQueryInfluencers.NONE;
		private LockMode lockMode = LockMode.NONE;
		private LockOptions lockOptions;

		public Builder(OuterJoinLoadable persister) {
			this.persister = persister;
		}

		public Builder withEntityLoaderTemplate(ReactivePlanEntityLoader entityLoaderTemplate) {
			this.entityLoaderTemplate = entityLoaderTemplate;
			return this;
		}

		public Builder withBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public Builder withInfluencers(LoadQueryInfluencers influencers) {
			this.influencers = influencers;
			return this;
		}

		public Builder withLockMode(LockMode lockMode) {
			this.lockMode = lockMode;
			return this;
		}

		public Builder withLockOptions(LockOptions lockOptions) {
			this.lockOptions = lockOptions;
			return this;
		}

		public ReactivePlanEntityLoader byPrimaryKey() {
			return byUniqueKey( persister.getIdentifierColumnNames(), persister.getIdentifierType() );
		}

		public ReactivePlanEntityLoader byUniqueKey(String[] keyColumnNames, Type keyType) {
			// capture current values in a new instance of QueryBuildingParametersImpl
			if ( entityLoaderTemplate == null ) {
				return new ReactivePlanEntityLoader(
						persister.getFactory(),
						persister,
						keyColumnNames,
						keyType,
						new QueryBuildingParametersImpl(
								influencers,
								batchSize,
								lockMode,
								lockOptions
						)
				);
			}
			else {
				return new ReactivePlanEntityLoader(
						persister.getFactory(),
						persister,
						entityLoaderTemplate,
						keyType,
						new QueryBuildingParametersImpl(
								influencers,
								batchSize,
								lockMode,
								lockOptions
						)
				);
			}
		}
	}

	private final ReactiveResultSetProcessor reactiveResultSetProcessor;

	private ReactivePlanEntityLoader(
			SessionFactoryImplementor factory,
			OuterJoinLoadable persister,
			String[] uniqueKeyColumnNames,
			Type uniqueKeyType,
			QueryBuildingParameters buildingParameters) throws MappingException {
		super(
				persister,
				factory,
				uniqueKeyColumnNames,
				uniqueKeyType,
				buildingParameters,
				(loadPlan, aliasResolutionContext, readerCollector, shouldUseOptionalEntityInstance, hadSubselectFetches)
						-> new ReactiveLoadPlanBasedResultSetProcessor(
								loadPlan,
								aliasResolutionContext,
								new ReactiveRowReader( readerCollector ),
								shouldUseOptionalEntityInstance,
								hadSubselectFetches
						)
		);
		this.persister = persister;
		this.reactiveResultSetProcessor = (ReactiveResultSetProcessor) getStaticLoadQuery().getResultSetProcessor();
		this.parameters = Parameters.instance( factory.getJdbcServices().getDialect() );
		this.processedSQL = parameters.process( getStaticLoadQuery().getSqlStatement() );
	}

	private ReactivePlanEntityLoader(
			SessionFactoryImplementor factory,
			OuterJoinLoadable persister,
			ReactivePlanEntityLoader entityLoaderTemplate,
			Type uniqueKeyType,
			QueryBuildingParameters buildingParameters) throws MappingException {
		super(
				persister,
				factory,
				entityLoaderTemplate.getStaticLoadQuery(),
				uniqueKeyType,
				buildingParameters,
				(loadPlan, aliasResolutionContext, readerCollector, shouldUseOptionalEntityInstance, hadSubselectFetches)
						-> new ReactiveLoadPlanBasedResultSetProcessor(
						loadPlan,
						aliasResolutionContext,
						new ReactiveRowReader( readerCollector ),
						shouldUseOptionalEntityInstance,
						hadSubselectFetches
				)
		);
		this.persister = persister;
		this.reactiveResultSetProcessor = (ReactiveResultSetProcessor) getStaticLoadQuery().getResultSetProcessor();
		this.parameters = Parameters.instance( factory.getJdbcServices().getDialect() );
		this.processedSQL = parameters.process( getStaticLoadQuery().getSqlStatement() );
	}

	@Override
	public Parameters parameters() {
		return parameters;
	}

	@Override
	protected EntityLoadQueryDetails getStaticLoadQuery() {
		return (EntityLoadQueryDetails) super.getStaticLoadQuery();
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session) {
		// this form is deprecated!
		return load( id, optionalObject, session, LockOptions.NONE, null );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, Boolean readOnly) {
		// this form is deprecated!
		return load( id, optionalObject, session, LockOptions.NONE, readOnly );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions) {
		return load( id, optionalObject, session, lockOptions, null );
	}

	@Override
	public CompletionStage<Object> load(Object id, SharedSessionContractImplementor session, LockOptions lockOptions) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject,
										SharedSessionContractImplementor session,
										LockOptions lockOptions, Boolean readOnly) {

		final QueryParameters parameters = buildQueryParameters( id, optionalObject, lockOptions, readOnly );

		// Filters get applied just before the query is executed. The parameter processor is not smart enough
		// to count deal with the additional parameters in a second pass and we have to wait until the query
		// is complete before processing it. See: ReactiveLoader#executeReactiveQueryStatement
		String sql = hasFilters( session )
			? getStaticLoadQuery().getSqlStatement()
			: processedSQL;

		return doReactiveQueryAndInitializeNonLazyCollections( sql, session, parameters )
				.thenApply( results -> extractEntityResult( results, id ) )
				.handle( (list, err) -> {
					logSqlException( err,
							() -> "could not load an entity: "
									+ infoString( persister, id, persister.getIdentifierType(), getFactory() ),
							sql
					);
					return returnOrRethrow( err, list) ;
				} );
	}

	private boolean hasFilters(SharedSessionContractImplementor session) {
		return !session.getLoadQueryInfluencers().getEnabledFilters().isEmpty();
	}

	private QueryParameters buildQueryParameters(Serializable id,
												 Object optionalObject,
												 LockOptions lockOptions,
												 Boolean readOnly) {
		//copied from super:
		final QueryParameters qp = new QueryParameters();
		qp.setPositionalParameterTypes( new Type[] { persister.getIdentifierType() } );
		qp.setPositionalParameterValues( new Object[] { id } );
		qp.setOptionalObject( optionalObject );
		qp.setOptionalEntityName( persister.getEntityName() );
		qp.setOptionalId( id );
		qp.setLockOptions( lockOptions );
		if ( readOnly != null ) {
			qp.setReadOnly( readOnly );
		}
		return qp;
	}

	public ReactiveResultSetProcessor getReactiveResultSetProcessor() {
		return reactiveResultSetProcessor;
	}

	@Override
	public String preprocessSQL(String sql,
								QueryParameters queryParameters,
								SessionFactoryImplementor factory,
								List<AfterLoadAction> afterLoadActions) {
		//TODO!!!
		return sql;
	}

	/**
	 * A "reactive" version of hibernate-orm's {@link ResultSetProcessorImpl}
	 */
	private static class ReactiveLoadPlanBasedResultSetProcessor extends ResultSetProcessorImpl
			implements ReactiveResultSetProcessor {

		private final ReactiveRowReader rowReader;

		public ReactiveLoadPlanBasedResultSetProcessor(
				LoadPlan loadPlan,
				AliasResolutionContext aliasResolutionContext,
				ReactiveRowReader rowReader,
				boolean shouldUseOptionalEntityInstance,
				boolean hadSubselectFetches) {
			super(
					loadPlan,
					aliasResolutionContext,
					rowReader,
					shouldUseOptionalEntityInstance,
					hadSubselectFetches
			);
			this.rowReader = rowReader;
		}

		@Override
		public List<Object> extractResults(
				ResultSet resultSet,
				final SharedSessionContractImplementor session,
				QueryParameters queryParameters,
				NamedParameterContext namedParameterContext,
				boolean returnProxies,
				boolean readOnly,
				ResultTransformer forcedResultTransformer,
				List<AfterLoadAction> afterLoadActionList) {
			throw new UnsupportedOperationException( "#reactiveExtractResults should be used instead" );
		}

		/**
		 * This method is based on {@link ResultSetProcessorImpl#extractResults}
		 */
		@Override
		public CompletionStage<List<Object>> reactiveExtractResults(
				ResultSet resultSet,
				final SharedSessionContractImplementor session,
				QueryParameters queryParameters,
				NamedParameterContext namedParameterContext,
				boolean returnProxies,
				boolean readOnly,
				ResultTransformer forcedResultTransformer,
				List<AfterLoadAction> afterLoadActionList) throws SQLException {

			handlePotentiallyEmptyCollectionRootReturns( queryParameters.getCollectionKeys(), resultSet, session );

			final ResultSetProcessingContextImpl context = createResultSetProcessingContext(
					resultSet,
					session,
					queryParameters,
					namedParameterContext,
					returnProxies,
					readOnly
			);

			final List<Object> loadResults = extractRows( resultSet, queryParameters, context );

			return rowReader.reactiveFinishUp( this, context, afterLoadActionList )
					.thenAccept( v -> context.wrapUp() )
					.thenApply( v -> loadResults );
		}
	}

	/**
	 * A "reactive" version of hibernate-orm's {@link AbstractRowReader}
	 */
	private static class ReactiveRowReader extends AbstractRowReader {

		private final ReaderCollector readerCollector;

		public ReactiveRowReader(ReaderCollector readerCollector) {
			super( readerCollector );
			this.readerCollector = readerCollector;
		}

		@Override
		protected Object readLogicalRow(ResultSet resultSet, ResultSetProcessingContextImpl context) throws SQLException {
			return readerCollector.getReturnReader().read( resultSet, context ) ;
		}

		@Override
		public void finishUp(ResultSetProcessingContextImpl context, List<AfterLoadAction> afterLoadActionList) {
			throw new UnsupportedOperationException( "Use #reactiveFinishUp instead." );
		}

		/**
		 * This method is based on {@link AbstractRowReader#finishUp}
		 */
		public CompletionStage<Void> reactiveFinishUp(
				ReactiveLoadPlanBasedResultSetProcessor resultSetProcessor,
				ResultSetProcessingContextImpl context,
				List<AfterLoadAction> afterLoadActionList) {

			// for arrays, we should end the collection load before resolving the entities, since the
			// actual array instances are not instantiated during loading
			finishLoadingArrays( context );

			// IMPORTANT: reuse the same event instances for performance!
			final PreLoadEvent preLoadEvent;
			final PostLoadEvent postLoadEvent;
			if ( context.getSession().isEventSource() ) {
				EventSource session = (EventSource) context.getSession();
				preLoadEvent = new PreLoadEvent(session);
				postLoadEvent = new PostLoadEvent(session);
			}
			else {
				preLoadEvent = null;
				postLoadEvent = null;
			}

			// now finish loading the entities (2-phase load)
			return reactivePerformTwoPhaseLoad( resultSetProcessor, preLoadEvent, context )
					.thenAccept( v -> {
						List<HydratedEntityRegistration> hydratedEntityRegistrations =
								context.getHydratedEntityRegistrationList();

						// now we can finalize loading collections
						finishLoadingCollections( context );

						// and trigger the afterInitialize() hooks
						afterInitialize( context, hydratedEntityRegistrations );

						// finally, perform post-load operations
						postLoad( postLoadEvent, context, hydratedEntityRegistrations, afterLoadActionList );
					} );
		}

		/**
		 * This method is based on {@link AbstractRowReader#performTwoPhaseLoad}
		 */
		public CompletionStage<Void> reactivePerformTwoPhaseLoad(
				ReactiveLoadPlanBasedResultSetProcessor resultSetProcessor,
				PreLoadEvent preLoadEvent,
				ResultSetProcessingContextImpl context) {

			List<HydratedEntityRegistration> hydratedEntityRegistrations = context.getHydratedEntityRegistrationList();
			if ( hydratedEntityRegistrations == null || hydratedEntityRegistrations.isEmpty() ) {
				return voidFuture();
			}

			final SharedSessionContractImplementor session = context.getSession();

			return CompletionStages.loop(
					hydratedEntityRegistrations,
					registration -> resultSetProcessor.initializeEntity(
							registration.getInstance(),
							false,
							session,
							preLoadEvent
					)
			);
		}
	}
}
