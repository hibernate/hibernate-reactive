/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.TypedValue;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static org.hibernate.pretty.MessageHelper.collectionInfoString;
import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnNullorRethrow;

/**
 * Implements subselect fetching for a one to many association
 * @author Gavin King
 */
public class ReactiveSubselectOneToManyLoader extends ReactiveOneToManyLoader {

	private final Serializable[] keys;
	private final Type[] types;
	private final Object[] values;
	private final Map<String, TypedValue> namedParameters;
	private final Map<String, int[]> namedParameterLocMap;

	public ReactiveSubselectOneToManyLoader(
			QueryableCollection persister,
			String subquery,
			Collection<EntityKey> entityKeys,
			QueryParameters queryParameters,
			Map<String, int[]> namedParameterLocMap,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		super( persister, 1, subquery, factory, loadQueryInfluencers );

		keys = new Serializable[ entityKeys.size() ];
		int i=0;
		for ( EntityKey key: entityKeys ) {
			keys[i++] = key.getIdentifier();
		}

		this.namedParameters = queryParameters.getNamedParameters();
		this.types = queryParameters.getFilteredPositionalParameterTypes();
		this.values = queryParameters.getFilteredPositionalParameterValues();
		this.namedParameterLocMap = namedParameterLocMap;
	}

	@Override
	public void initialize(Serializable id, SharedSessionContractImplementor session) throws HibernateException {
		loadCollectionSubselect(
				session,
				keys,
				values,
				types,
				namedParameters,
				getKeyType()
		);
	}

	@Override
	public CompletionStage<Void> reactiveInitialize(Serializable id, SharedSessionContractImplementor session)
			throws HibernateException {
		return reactiveLoadCollectionSubselect(
				session,
				keys,
				values,
				types,
				namedParameters,
				getKeyType()
		);
	}

	protected final CompletionStage<Void> reactiveLoadCollectionSubselect(
			final SharedSessionContractImplementor session,
			final Serializable[] ids,
			final Object[] parameterValues,
			final Type[] parameterTypes,
			final Map<String, TypedValue> namedParameters,
			final Type type) throws HibernateException {

		QueryParameters parameters = new QueryParameters(parameterTypes, parameterValues, namedParameters, ids);
		return doReactiveQueryAndInitializeNonLazyCollections( session, parameters, true )
				.handle( (list, err) -> {
					logSqlException( err,
							() -> "could not load collection by subselect: "
									+ collectionInfoString( getCollectionPersisters()[0], ids, getFactory() ),
							getSQLString()
					);
					return returnNullorRethrow( err );
				} );
	}

	@Override
	public int[] getNamedParameterLocs(String name) {
		return namedParameterLocMap.get( name );
	}

}
