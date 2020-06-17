/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.loader.ReactiveLoader;
import org.hibernate.transform.ResultTransformer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * An interface intended for {@link ReactiveLoader} implementations that also
 * extend a {@link org.hibernate.loader.Loader} implementation.
 *
 * @author Gail Badner
 */
public interface ReactiveLoaderBasedLoader extends ReactiveLoader {

	SessionFactoryImplementor getFactory();

	EntityPersister[] getEntityPersisters();

	CollectionPersister[] getCollectionPersisters();

	boolean isSubselectLoadingEnabled();

	List<Object> getRowsFromResultSet(
			final ResultSet rs,
			final QueryParameters queryParameters,
			final SharedSessionContractImplementor session,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer,
			final int maxRows,
			final List<Object> hydratedObjects,
			final List<EntityKey[]> subselectResultKeys) throws SQLException;

	void createSubselects(
			final List keys,
			final QueryParameters queryParameters,
			final SharedSessionContractImplementor session);

	void endCollectionLoad(
			final Object resultSetId,
			final SharedSessionContractImplementor session,
			final CollectionPersister collectionPersister);
}
