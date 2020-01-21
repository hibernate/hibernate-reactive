package org.hibernate.rx.engine.impl;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.engine.jndi.spi.JndiService;
import org.hibernate.engine.spi.SessionFactoryDelegatingImpl;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionFactoryRegistry;
import org.hibernate.internal.SessionFactoryRegistry.ObjectFactoryImpl;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.RxSessionFactory;
import org.hibernate.rx.engine.spi.RxSessionBuilderImplementor;
import org.hibernate.rx.engine.spi.RxSessionFactoryImplementor;
import org.hibernate.rx.impl.RxSessionBuilderDelegator;

import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.persistence.EntityGraph;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;
import java.util.List;

/**
 * Implementation of {@link RxSessionFactory}.
 */
public class RxSessionFactoryImpl extends SessionFactoryDelegatingImpl
		implements RxSessionFactoryImplementor {

	private final String uuid;
	private final SessionFactoryImpl delegate;

	public RxSessionFactoryImpl(SessionFactoryImpl delegate) {
		super( delegate );
		this.delegate = delegate;
		uuid = delegate.getUuid();
		SessionFactoryRegistry.INSTANCE.addSessionFactory(
				delegate.getUuid(),
				delegate.getName(),
				delegate.getSessionFactoryOptions().isSessionFactoryNameAlsoJndiName(),
				this,
				delegate.getServiceRegistry().getService( JndiService.class )
		);
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
		return new RxSessionBuilderDelegator(
				new SessionFactoryImpl.SessionBuilderImpl(delegate),
				delegate
		);
	}

	@Override
	public Reference getReference() {
		return new Reference(
				getClass().getName(),
				new StringRefAddr( "uuid", uuid ),
				ObjectFactoryImpl.class.getName(),
				null
		);
	}

	@Override
	public <T> List<EntityGraph<? super T>> findEntityGraphsByType(Class<T> entityClass) {
		return delegate().findEntityGraphsByType( entityClass );
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

		if ( type.isAssignableFrom( RxSessionFactoryImplementor.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( EntityManagerFactory.class ) ) {
			return type.cast( this );
		}

		throw new PersistenceException( "Hibernate cannot unwrap EntityManagerFactory as '" + type.getName() + "'" );
	}

}
