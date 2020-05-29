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
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.plan.AbstractLoadPlanBasedEntityLoader;
import org.hibernate.loader.plan.exec.internal.EntityLoadQueryDetails;
import org.hibernate.loader.plan.exec.query.internal.QueryBuildingParametersImpl;
import org.hibernate.loader.plan.exec.query.spi.QueryBuildingParameters;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.loader.ReactiveLoader;
import org.hibernate.reactive.loader.entity.ReactiveUniqueEntityLoader;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.pretty.MessageHelper.infoString;

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

	private ReactivePlanEntityLoader(
			SessionFactoryImplementor factory,
			OuterJoinLoadable persister,
			String[] uniqueKeyColumnNames,
			Type uniqueKeyType,
			QueryBuildingParameters buildingParameters) throws MappingException {
		super( persister, factory, uniqueKeyColumnNames, uniqueKeyType, buildingParameters );
		this.persister = persister;
	}

	private ReactivePlanEntityLoader(
			SessionFactoryImplementor factory,
			OuterJoinLoadable persister,
			ReactivePlanEntityLoader entityLoaderTemplate,
			Type uniqueKeyType,
			QueryBuildingParameters buildingParameters) throws MappingException {
		super( persister, factory, entityLoaderTemplate.getStaticLoadQuery(), uniqueKeyType, buildingParameters );
		this.persister = persister;
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
	public CompletionStage<Object> load(Serializable id, Object optionalObject,
										SharedSessionContractImplementor session,
										LockOptions lockOptions, Boolean readOnly) {

		final QueryParameters parameters = buildQueryParameters( id, optionalObject, lockOptions, readOnly );
		String sql = getStaticLoadQuery().getSqlStatement();

		return doReactiveQueryAndInitializeNonLazyCollections( sql, (SessionImplementor) session, parameters )
				.thenApply( results -> extractEntityResult( results, id ) )
				.handle( (list, err) -> {
					CompletionStages.logSqlException( err,
							() -> "could not load an entity: "
									+ infoString( persister, id, persister.getIdentifierType(), getFactory() ),
							sql
					);
					return CompletionStages.returnOrRethrow( err, list) ;
				} );
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

	@Override
	public SessionFactoryImplementor getFactory() {
		return super.getFactory();
	}

	@Override @SuppressWarnings("unchecked")
	public List<Object> processResultSet(ResultSet resultSet,
										 QueryParameters queryParameters,
										 SharedSessionContractImplementor session,
										 boolean returnProxies,
										 ResultTransformer forcedResultTransformer,
										 int maxRows,
										 List<AfterLoadAction> afterLoadActions) throws SQLException {
		return getStaticLoadQuery()
				.getResultSetProcessor()
				.extractResults(
						resultSet,
						session,
						queryParameters,
						null,
						false,
						false,
						null,
						afterLoadActions
				);
	}

	@Override
	public String preprocessSQL(String sql,
								QueryParameters queryParameters,
								SessionFactoryImplementor factory,
								List<AfterLoadAction> afterLoadActions) {
		//TODO!!!
		return sql;
	}
}
