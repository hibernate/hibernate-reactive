package org.hibernate.rx.engine.impl;

import java.util.List;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.persistence.EntityGraph;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.engine.jndi.spi.JndiService;
import org.hibernate.engine.spi.SessionFactoryDelegatingImpl;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionFactoryRegistry;
import org.hibernate.internal.SessionFactoryRegistry.ObjectFactoryImpl;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxHibernateSessionFactory;
import org.hibernate.rx.engine.spi.RxHibernateSessionBuilderImplementor;
import org.hibernate.rx.engine.spi.RxHibernateSessionFactoryImplementor;
import org.hibernate.rx.impl.RxHibernateSessionBuilderDelegator;
import org.hibernate.rx.impl.RxHibernateSessionImpl;

public class RxHibernateSessionFactoryImpl extends SessionFactoryDelegatingImpl
		implements RxHibernateSessionFactoryImplementor {

	private final String uuid;

	public RxHibernateSessionFactoryImpl(SessionFactoryImplementor delegate) {
		super( delegate );
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
	public RxHibernateSession openRxSession() throws HibernateException {
		return (RxHibernateSession) openSession();
	}

	@Override
	public Session openSession() throws HibernateException {
		return withOptions().openRxSession();
	}

	@Override
	public RxHibernateSessionBuilderImplementor withOptions() {
		return new RxHibernateSessionBuilderDelegator( delegate().withOptions(), this );
	}

	@Override
	public Reference getReference() throws NamingException {
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

		if ( type.isAssignableFrom( SessionFactoryImpl.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( RxHibernateSessionFactory.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( RxHibernateSessionFactoryImplementor.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( RxHibernateSessionFactoryImpl.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( EntityManagerFactory.class ) ) {
			return type.cast( this );
		}

		throw new PersistenceException( "Hibernate cannot unwrap EntityManagerFactory as '" + type.getName() + "'" );
	}

}
