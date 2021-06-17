/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.loader.entity.EntityJoinWalker;
import org.hibernate.persister.entity.OuterJoinLoadable;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

/**
 * A {@link ReactiveEntityLoader} whose generated SQL contains a placeholder
 * that is interpolated with a batch of ids at runtime.
 *
 * Used when for {@link org.hibernate.loader.BatchFetchStyle#DYNAMIC} is selected.
 *
 * @see org.hibernate.loader.entity.DynamicBatchingEntityLoaderBuilder.DynamicEntityLoader
 * @see ReactiveDynamicBatchingEntityDelegator
 */
class ReactiveDynamicBatchingEntityLoader extends ReactiveEntityLoader {

	private final String sqlTemplate;
	private final String alias;

	public ReactiveDynamicBatchingEntityLoader(
			OuterJoinLoadable persister,
			int maxBatchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		this( persister, maxBatchSize, lockOptions.getLockMode(), factory, loadQueryInfluencers );
	}

	public ReactiveDynamicBatchingEntityLoader(
			OuterJoinLoadable persister,
			int maxBatchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super( persister, -1, lockMode, factory, loadQueryInfluencers );

		EntityJoinWalker walker = new EntityJoinWalker(
				persister,
				persister.getIdentifierColumnNames(),
				-1,
				lockMode,
				factory,
				loadQueryInfluencers) {
			@Override
			protected StringBuilder whereString(String alias, String[] columnNames, int batchSize) {
				return buildBatchFetchRestrictionFragment(
						alias,
						columnNames,
						getDialect()
				);
			}
		};

		initFromWalker( walker );
		this.sqlTemplate = walker.getSQLString();
		this.alias = walker.getAlias();
		postInstantiate();

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"SQL-template for dynamic entity [%s] batch-fetching [%s] : %s",
					entityName,
					lockMode,
					sqlTemplate
			);
		}
	}

	@Override
	protected boolean isSingleRowLoader() {
		return false;
	}

	@Override
	public boolean isSubselectLoadingEnabled() {
		return persister.hasSubselectLoadableCollections();
	}

	public CompletionStage<List<Object>> doEntityBatchFetch(
			SessionImplementor session,
			QueryParameters queryParameters,
			Serializable[] ids) {

		final String sql = expandBatchIdPlaceholder(
				sqlTemplate,
				ids,
				alias,
				persister.getKeyColumnNames(),
				getDialect()
		);

		// Filters might add additional parameters and our processor is not smart enough, right now, to
		// recognize them if the query has been processed already.
		// So we only process the SQL in advance if filters are disabled.
		final String processedSQL = session.getLoadQueryInfluencers().hasEnabledFilters()
				? sql
				: parameters().process( sql );
		return doReactiveQueryAndInitializeNonLazyCollections( processedSQL, session, queryParameters )
				.handle( (results, err) -> {
					logSqlException( err,
							() -> "could not load an entity batch: " + infoString(
									getEntityPersisters()[0],
									ids,
									session.getFactory()
							),
							sql
					);
					return returnOrRethrow( err, results );
				} );
	}

	private static StringBuilder buildBatchFetchRestrictionFragment(
			String alias,
			String[] columnNames,
			Dialect dialect) {
		// the general idea here is to just insert a placeholder that we can easily find later...
		if ( columnNames.length == 1 ) {
			// non-composite key
			return new StringBuilder( StringHelper.qualify( alias, columnNames[0] ) )
					.append( " in (" ).append( StringHelper.BATCH_ID_PLACEHOLDER ).append( ')' );
		}
		else {
			// composite key - the form to use here depends on what the dialect supports.
			if ( dialect.supportsRowValueConstructorSyntaxInInList() ) {
				// use : (col1, col2) in ( (?,?), (?,?), ... )
				StringBuilder builder = new StringBuilder();
				builder.append( '(' );
				boolean firstPass = true;
				String deliminator = "";
				for ( String columnName : columnNames ) {
					builder.append( deliminator ).append( StringHelper.qualify( alias, columnName ) );
					if ( firstPass ) {
						firstPass = false;
						deliminator = ",";
					}
				}
				builder.append( ") in (" );
				builder.append( StringHelper.BATCH_ID_PLACEHOLDER );
				builder.append( ')' );
				return builder;
			}
			else {
				// use : ( (col1 = ? and col2 = ?) or (col1 = ? and col2 = ?) or ... )
				//		unfortunately most of this building needs to be held off until we know
				//		the exact number of ids :(
				final StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append( '(' )
						.append( StringHelper.BATCH_ID_PLACEHOLDER )
						.append( ')' );
				return stringBuilder;
			}
		}
	}

	static String expandBatchIdPlaceholder(
			String sql,
			Serializable[] ids,
			String alias,
			String[] keyColumnNames,
			Dialect dialect) {
		if ( keyColumnNames.length == 1 ) {
			// non-composite
			return StringHelper.replace( sql, StringHelper.BATCH_ID_PLACEHOLDER, StringHelper.repeat( "?", ids.length, "," ) );
		}
		else {
			// composite
			if ( dialect.supportsRowValueConstructorSyntaxInInList() ) {
				final String tuple = '(' + StringHelper.repeat( "?", keyColumnNames.length, "," ) + ')';
				return StringHelper.replace( sql, StringHelper.BATCH_ID_PLACEHOLDER, StringHelper.repeat( tuple, ids.length, "," ) );
			}
			else {
				final String keyCheck = '(' + StringHelper.joinWithQualifierAndSuffix(
						keyColumnNames,
						alias,
						" = ?",
						" and "
				) + ')';
				return StringHelper.replace( sql, StringHelper.BATCH_ID_PLACEHOLDER, StringHelper.repeat( keyCheck, ids.length, " or " ) );
			}
		}
	}

}
