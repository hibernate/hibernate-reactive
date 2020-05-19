package org.hibernate.reactive.engine.impl;

import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionFactoryRegistry.ObjectFactoryImpl;
import org.hibernate.reactive.stage.RxSession;
import org.hibernate.reactive.stage.RxSessionFactory;
import org.hibernate.reactive.boot.impl.RxSessionFactoryBuilderImpl;
import org.hibernate.reactive.engine.query.spi.RxHQLQueryPlan;
import org.hibernate.reactive.engine.spi.RxSessionBuilderImplementor;
import org.hibernate.reactive.impl.RxSessionBuilderDelegator;

/**
 * Implementation of {@link RxSessionFactory}.
 *
 * @see RxSessionFactoryBuilderImpl
 */
public class RxSessionFactoryImpl extends SessionFactoryImpl
		implements RxSessionFactory {

	public RxSessionFactoryImpl(
			MetadataImplementor metadata,
			SessionFactoryOptions options) {
		super( metadata, options, RxHQLQueryPlan::new );
	}

	@Override
	public RxSession openRxSession() throws HibernateException {
		return withOptions().openRxSession();
	}

	@Override
	public Session openSession() throws HibernateException {
		return withOptions().openSession();
	}

	@Override
	public RxSessionBuilderImplementor withOptions() {
		return new RxSessionBuilderDelegator( new SessionBuilderImpl<>(this), this );
	}

	@Override
	public Reference getReference() {
		return new Reference(
				getClass().getName(),
				new StringRefAddr( "uuid", getUuid() ),
				ObjectFactoryImpl.class.getName(),
				null
		);
	}

	@Override
	public <T> T unwrap(Class<T> type) {
		if ( type.isAssignableFrom( SessionFactory.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( SessionFactoryImplementor.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( RxSessionFactory.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( EntityManagerFactory.class ) ) {
			return type.cast( this );
		}

		throw new PersistenceException( "Hibernate cannot unwrap EntityManagerFactory as '" + type.getName() + "'" );
	}

}
