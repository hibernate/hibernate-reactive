/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.Internal;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.internal.LoaderSelectBuilder;
import org.hibernate.loader.ast.internal.SingleIdEntityLoaderStandardImpl;
import org.hibernate.loader.ast.spi.CascadingFetchProfile;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;

/**
 * Standard implementation of {@link ReactiveSingleIdEntityLoader}.
 *
 * @see SingleIdEntityLoaderStandardImpl
 */
public class ReactiveSingleIdEntityLoaderStandardImpl<T> extends SingleIdEntityLoaderStandardImpl<CompletionStage<T>> implements ReactiveSingleIdEntityLoader<T> {

	private EnumMap<LockMode, ReactiveSingleIdLoadPlan> selectByLockMode = new EnumMap<>( LockMode.class );
	private EnumMap<CascadingFetchProfile, ReactiveSingleIdLoadPlan> selectByInternalCascadeProfile;

	private AtomicInteger nonReusablePlansGenerated = new AtomicInteger();

	public AtomicInteger getNonReusablePlansGenerated() {
		return nonReusablePlansGenerated;
	}

	private DatabaseSnapshotExecutor databaseSnapshotExecutor;

	private final EntityMappingType entityDescriptor;

	protected final SessionFactoryImplementor sessionFactory;

	public ReactiveSingleIdEntityLoaderStandardImpl(
			EntityMappingType entityDescriptor,
			SessionFactoryImplementor sessionFactory) {
		super( entityDescriptor, sessionFactory );
		// todo (6.0) : consider creating a base AST and "cloning" it
		this.entityDescriptor = entityDescriptor;
		this.sessionFactory = sessionFactory;
	}

	@Override
	public EntityMappingType getLoadable() {
		return entityDescriptor;
	}

	@Override
	public CompletionStage<Object[]> reactiveLoadDatabaseSnapshot(Object id, SharedSessionContractImplementor session) {
		if ( databaseSnapshotExecutor == null ) {
			databaseSnapshotExecutor = new DatabaseSnapshotExecutor( entityDescriptor, sessionFactory );
		}

		return databaseSnapshotExecutor.loadDatabaseSnapshot( id, session );
	}

	@Override
	public void prepare() {
		// see `org.hibernate.persister.entity.AbstractEntityPersister#createLoaders`
		//		we should pre-load a few - maybe LockMode.NONE and LockMode.READ
		final LockOptions lockOptions = LockOptions.NONE;
		final LoadQueryInfluencers queryInfluencers = new LoadQueryInfluencers( sessionFactory );
		final ReactiveSingleIdLoadPlan<T> plan = createLoadPlan(
				lockOptions,
				queryInfluencers,
				sessionFactory
		);
		if ( determineIfReusable( lockOptions, queryInfluencers ) ) {
			selectByLockMode.put( lockOptions.getLockMode(), plan );
		}
	}

	private boolean determineIfReusable(LockOptions lockOptions, LoadQueryInfluencers loadQueryInfluencers) {
		if ( getLoadable().isAffectedByEntityGraph( loadQueryInfluencers ) ) {
			return false;
		}

		if ( getLoadable().isAffectedByEnabledFetchProfiles( loadQueryInfluencers ) ) {
			return false;
		}

		//noinspection RedundantIfStatement
		if ( lockOptions.getTimeOut() != LockOptions.WAIT_FOREVER ) {
			return false;
		}

		return true;
	}

	@Override
	public CompletionStage<T> load(Object key, LockOptions lockOptions, Boolean readOnly, SharedSessionContractImplementor session) {
		final ReactiveSingleIdLoadPlan<T> loadPlan = resolveLoadPlan( lockOptions, session.getLoadQueryInfluencers(), session.getFactory() );
		return loadPlan.load( key, readOnly, true, session );
	}

	@Override
	public CompletionStage<T> load(
			Object key,
			Object entityInstance,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		final ReactiveSingleIdLoadPlan<T> loadPlan = resolveLoadPlan(
				lockOptions,
				session.getLoadQueryInfluencers(),
				session.getFactory()
		);

		return loadPlan.load( key, entityInstance, readOnly, false, session );
	}

	@Internal
	public ReactiveSingleIdLoadPlan<T> resolveLoadPlan(
			LockOptions lockOptions,
			LoadQueryInfluencers loadQueryInfluencers,
			SessionFactoryImplementor sessionFactory) {
		if ( getLoadable().isAffectedByEnabledFilters( loadQueryInfluencers ) ) {
			// special case of not-cacheable based on enabled filters effecting this load.
			//
			// This case is special because the filters need to be applied in order to
			// 		properly restrict the SQL/JDBC results.  For this reason it has higher
			// 		precedence than even "internal" fetch profiles.
			nonReusablePlansGenerated.incrementAndGet();
			return createLoadPlan( lockOptions, loadQueryInfluencers, sessionFactory );
		}

		final CascadingFetchProfile enabledCascadingFetchProfile = loadQueryInfluencers.getEnabledCascadingFetchProfile();
		if ( enabledCascadingFetchProfile != null ) {
			if ( LockMode.WRITE.greaterThan( lockOptions.getLockMode() ) ) {
				if ( selectByInternalCascadeProfile == null ) {
					selectByInternalCascadeProfile = new EnumMap<>( CascadingFetchProfile.class );
				}
				else {
					final ReactiveSingleIdLoadPlan existing = selectByInternalCascadeProfile.get( enabledCascadingFetchProfile );
					if ( existing != null ) {
						//noinspection unchecked
						return existing;
					}
				}

				final ReactiveSingleIdLoadPlan<T> plan = createLoadPlan(
						lockOptions,
						loadQueryInfluencers,
						sessionFactory
				);
				selectByInternalCascadeProfile.put( enabledCascadingFetchProfile, plan );
				return plan;
			}
		}

		// otherwise see if the loader for the requested load can be cached - which
		// 		also means we should look in the cache for an existing one

		final boolean reusable = determineIfReusable( lockOptions, loadQueryInfluencers );

		if ( reusable ) {
			final ReactiveSingleIdLoadPlan<T> existing = selectByLockMode.get( lockOptions.getLockMode() );
			if ( existing != null ) {
				//noinspection unchecked
				return existing;
			}

			final ReactiveSingleIdLoadPlan<T> plan = createLoadPlan(
					lockOptions,
					loadQueryInfluencers,
					sessionFactory
			);
			selectByLockMode.put( lockOptions.getLockMode(), plan );

			return plan;
		}

		nonReusablePlansGenerated.incrementAndGet();
		return createLoadPlan( lockOptions, loadQueryInfluencers, sessionFactory );
	}

	private ReactiveSingleIdLoadPlan<T> createLoadPlan(
			LockOptions lockOptions,
			LoadQueryInfluencers queryInfluencers,
			SessionFactoryImplementor sessionFactory) {

		final List<JdbcParameter> jdbcParameters = new ArrayList<>();

		final SelectStatement sqlAst = LoaderSelectBuilder.createSelect(
				getLoadable(),
				// null here means to select everything
				null,
				getLoadable().getIdentifierMapping(),
				null,
				1,
				queryInfluencers,
				lockOptions,
				jdbcParameters::add,
				sessionFactory
		);

		return new ReactiveSingleIdLoadPlan<>(
				(Loadable) getLoadable(),
				getLoadable().getIdentifierMapping(),
				sqlAst,
				jdbcParameters,
				lockOptions,
				sessionFactory
		);
	}
}
