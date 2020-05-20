package org.hibernate.reactive.impl;

import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;

import org.hibernate.HibernateException;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionFactoryRegistry.ObjectFactoryImpl;
import org.hibernate.reactive.engine.query.spi.ReactiveHQLQueryPlan;
import org.hibernate.reactive.stage.Stage;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Implementation of {@link Stage.SessionFactory}.
 *
 * @see org.hibernate.reactive.boot.impl.ReactiveSessionFactoryBuilderImpl
 */
public class StageSessionFactoryImpl extends SessionFactoryImpl
		implements Stage.SessionFactory {

	public StageSessionFactoryImpl(
			MetadataImplementor metadata,
			SessionFactoryOptions options) {
		super( metadata, options, ReactiveHQLQueryPlan::new );
	}

	@Override
	public CompletionStage<Stage.Session> openReactiveSession() throws HibernateException {
		return withOptions().openReactiveSession();
	}

	@Override
	public <T> CompletionStage<T> withReactiveSession(Function<Stage.Session, CompletionStage<T>> work) {
		return openReactiveSession().thenCompose(
				session -> work.apply(session)
						.whenComplete( (r, e) -> session.close() )
		);
	}

	@Override
	public org.hibernate.Session openSession() throws HibernateException {
		return withOptions().openSession();
	}

	@Override
	public StageSessionBuilderImpl withOptions() {
		return new StageSessionBuilderImpl( new SessionBuilderImpl<>(this), this );
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
		if ( type.isAssignableFrom( org.hibernate.SessionFactory.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( SessionFactoryImplementor.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( Stage.SessionFactory.class ) ) {
			return type.cast( this );
		}

		if ( type.isAssignableFrom( EntityManagerFactory.class ) ) {
			return type.cast( this );
		}

		throw new PersistenceException( "Hibernate cannot unwrap EntityManagerFactory as '" + type.getName() + "'" );
	}

}
