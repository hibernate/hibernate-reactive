/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.lang.reflect.Array;
import java.util.Locale;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.engine.internal.BatchFetchQueueHelper;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.internal.EntityBatchLoaderArrayParam;
import org.hibernate.loader.ast.internal.LoaderSelectBuilder;
import org.hibernate.loader.ast.internal.MultiKeyLoadHelper;
import org.hibernate.loader.ast.internal.Preparable;
import org.hibernate.loader.ast.spi.EntityBatchLoader;
import org.hibernate.loader.ast.spi.SqlArrayMultiKeyLoader;
import org.hibernate.metamodel.mapping.BasicEntityIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.JdbcParameterImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.type.BasicType;
import org.hibernate.type.BasicTypeRegistry;

import static org.hibernate.loader.ast.internal.MultiKeyLoadLogging.MULTI_KEY_LOAD_DEBUG_ENABLED;
import static org.hibernate.loader.ast.internal.MultiKeyLoadLogging.MULTI_KEY_LOAD_LOGGER;

/**
 * @see EntityBatchLoaderArrayParam
 */
public class ReactiveEntityBatchLoaderArrayParam<T> extends ReactiveSingleIdEntityLoaderSupport<T>
		implements EntityBatchLoader<CompletionStage<T>>, ReactiveSingleIdEntityLoader<T>, SqlArrayMultiKeyLoader, Preparable {

	private final int domainBatchSize;

	private BasicEntityIdentifierMapping identifierMapping;
	private JdbcMapping arrayJdbcMapping;
	private JdbcParameter jdbcParameter;
	private SelectStatement sqlAst;
	private JdbcOperationQuerySelect jdbcSelectOperation;

	public ReactiveEntityBatchLoaderArrayParam(
			int domainBatchSize,
			EntityMappingType entityDescriptor,
			SessionFactoryImplementor sessionFactory) {
		super( entityDescriptor, sessionFactory );
		this.domainBatchSize = domainBatchSize;

		if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
			MULTI_KEY_LOAD_LOGGER.debugf(
					"Batch fetching enabled for `%s` (entity) using ARRAY strategy : %s",
					entityDescriptor.getEntityName(),
					domainBatchSize
			);
		}
	}

	@Override
	public int getDomainBatchSize() {
		return domainBatchSize;
	}

	@Override
	public CompletionStage<T> load(Object pkValue, LockOptions lockOptions, Boolean readOnly, SharedSessionContractImplementor session) {
		return load( pkValue, null, lockOptions, readOnly, session );
	}

	@Override
	public final CompletionStage<T> load(
			Object pkValue,
			Object entityInstance,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
			MULTI_KEY_LOAD_LOGGER.debugf( "Batch fetching entity `%s#%s`", getLoadable().getEntityName(), pkValue );
		}

		final Object[] ids = resolveIdsToInitialize( pkValue, session );
		return initializeEntities( ids, pkValue, entityInstance, lockOptions, readOnly, session )
				.thenApply( v -> {
					final EntityKey entityKey = session.generateEntityKey( pkValue, getLoadable().getEntityPersister() );
					//noinspection unchecked
					return (T) session.getPersistenceContext().getEntity( entityKey );
				}  );
	}

	protected Object[] resolveIdsToInitialize(Object pkValue, SharedSessionContractImplementor session) {
		final Object[] idsToLoad = (Object[]) Array.newInstance(
				identifierMapping.getJavaType().getJavaTypeClass(),
				domainBatchSize
		);
		session.getPersistenceContextInternal().getBatchFetchQueue().collectBatchLoadableEntityIds(
				domainBatchSize,
				(index, value) -> idsToLoad[index] = value,
				pkValue,
				getLoadable()
		);
		return idsToLoad;
	}

	private CompletionStage<Void> initializeEntities(
			Object[] idsToInitialize,
			Object pkValue,
			Object entityInstance,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		return ReactiveLoaderHelper.loadByArrayParameter(
						idsToInitialize,
						sqlAst,
						jdbcSelectOperation,
						jdbcParameter,
						arrayJdbcMapping,
						pkValue,
						entityInstance,
						lockOptions,
						readOnly,
						session
				)
				.thenAccept( objects -> {
					//noinspection ForLoopReplaceableByForEach
					for ( int i = 0; i < idsToInitialize.length; i++ ) {
						final Object id = idsToInitialize[i];
						if ( id == null ) {
							// skip any of the null padded ids
							//		- actually we could probably even break here
							continue;
						}
						// found or not, remove the key from the batch-fetch queue
						BatchFetchQueueHelper.removeBatchLoadableEntityKey( id, getLoadable(), session );
					}
				} );
	}

	@Override
	public void prepare() {
		identifierMapping = (BasicEntityIdentifierMapping) getLoadable().getIdentifierMapping();
		final Class<?> arrayClass = Array.newInstance( identifierMapping.getJavaType().getJavaTypeClass(), 0 )
				.getClass();

		final BasicTypeRegistry basicTypeRegistry = sessionFactory.getTypeConfiguration().getBasicTypeRegistry();
		final BasicType<?> arrayBasicType = basicTypeRegistry.getRegisteredType( arrayClass );

		arrayJdbcMapping = MultiKeyLoadHelper.resolveArrayJdbcMapping(
				arrayBasicType,
				identifierMapping.getJdbcMapping(),
				arrayClass,
				sessionFactory
		);

		jdbcParameter = new JdbcParameterImpl( arrayJdbcMapping );
		sqlAst = LoaderSelectBuilder.createSelectBySingleArrayParameter(
				getLoadable(),
				identifierMapping,
				LoadQueryInfluencers.NONE,
				LockOptions.NONE,
				jdbcParameter,
				sessionFactory
		);

		jdbcSelectOperation = sessionFactory.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildSelectTranslator( sessionFactory, sqlAst )
				.translate( JdbcParameterBindings.NO_BINDINGS, QueryOptions.NONE );
	}

	@Override
	public String toString() {
		return String.format(
				Locale.ROOT,
				"EntityBatchLoaderArrayParam(%s [%s])",
				getLoadable().getEntityName(),
				domainBatchSize
		);
	}
}
