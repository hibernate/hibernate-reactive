package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.loader.JoinWalker;
import org.hibernate.loader.collection.OneToManyLoader;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.transform.ResultTransformer;
import org.jboss.logging.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ReactiveOneToManyLoader extends ReactiveCollectionLoader {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger(CoreMessageLogger.class, OneToManyLoader.class.getName());

	public ReactiveOneToManyLoader(
			QueryableCollection oneToManyPersister,
			SessionFactoryImplementor session,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(oneToManyPersister, 1, session, loadQueryInfluencers);
	}

	public ReactiveOneToManyLoader(
			QueryableCollection oneToManyPersister,
			int batchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(oneToManyPersister, batchSize, null, factory, loadQueryInfluencers);
	}

	public ReactiveOneToManyLoader(
			QueryableCollection oneToManyPersister,
			int batchSize,
			String subquery,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		super(oneToManyPersister, factory, loadQueryInfluencers);

		JoinWalker walker = new ReactiveOneToManyJoinWalker(
				oneToManyPersister,
				batchSize,
				subquery,
				factory,
				loadQueryInfluencers
		);
		initFromWalker(walker);

		postInstantiate();
		if (LOG.isDebugEnabled()) {
			LOG.debugf("Static select for one-to-many %s: %s", oneToManyPersister.getRole(), getSQLString());
		}
	}

	@Override
	protected Object getResultColumnOrRow(Object[] row, ResultTransformer transformer, ResultSet rs, SharedSessionContractImplementor session) throws SQLException, HibernateException {
		if ( row.length == 1 ) {
			return row[0];
		}
		return row;
	}
}
