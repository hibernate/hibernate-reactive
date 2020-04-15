package org.hibernate.rx.loader.collection;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.*;
import org.hibernate.loader.collection.CollectionInitializer;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.loader.RxOuterJoinLoader;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

public class RxCollectionLoader extends RxOuterJoinLoader implements CollectionInitializer {
	private final QueryableCollection collectionPersister;

	public RxCollectionLoader(
			QueryableCollection collectionPersister,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super(factory, loadQueryInfluencers);
		this.collectionPersister = collectionPersister;
	}

	protected QueryableCollection collectionPersister() {
		return collectionPersister;
	}

	@Override
	protected boolean isSubselectLoadingEnabled() {
		return hasSubselectLoadableCollections();
	}

	/**
	 * @deprecated use {@link #rxInitialize(Serializable, SharedSessionContractImplementor)}
	 */
	@Override
	@Deprecated
	public void initialize(Serializable id, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Use the reactive method instead: rxInitialize");
	}

	public CompletionStage<Void> rxInitialize(Serializable id, SharedSessionContractImplementor session) throws HibernateException {
		return rxLoadCollection( (SessionImplementor) session, id, getKeyType() );
	}

	/**
	 * Called by subclasses that initialize collections
	 */
	public CompletionStage<Void> rxLoadCollection(
			final SessionImplementor session,
			final Serializable id,
			final Type type) throws HibernateException {
		if (LOG.isDebugEnabled()) {
			LOG.debugf(
					"Loading collection: %s",
					MessageHelper.collectionInfoString(collectionPersister(), id, getFactory())
			);
		}

		Serializable[] ids = new Serializable[]{id};
		QueryParameters qp = new QueryParameters(new Type[]{type}, ids, ids);
		return doRxQueryAndInitializeNonLazyCollections(session, qp, true)
				.handle((list, e) -> {
					if (e instanceof SQLException) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
								(SQLException) e,
								"could not initialize a collection: " +
										MessageHelper.collectionInfoString(collectionPersister(), id, getFactory()),
								getSQLString()
						);
					} else if (e != null) {
						RxUtil.rethrow(e);
					}
					LOG.debug("Done loading collection");
					return null;
				});
	}

	protected Type getKeyType() {
		return collectionPersister.getKeyType();
	}

	@Override
	public String toString() {
		return getClass().getName() + '(' + collectionPersister.getRole() + ')';
	}

}
