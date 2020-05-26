/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.impl.MutinySessionFactoryImpl;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionFactoryImpl;

/**
 * A Hibernate {@link org.hibernate.SessionFactory} that can be
 * unwrapped to produce a {@link Stage.SessionFactory} or a
 * {@link Mutiny.SessionFactory}.
 */
public class ReactiveSessionFactoryImpl extends SessionFactoryImpl {
	public ReactiveSessionFactoryImpl(MetadataImplementor metadata, SessionFactoryOptions options) {
		super( metadata, options, ReactiveHQLQueryPlan::new );
	}

	@Override
	public <T> T unwrap(Class<T> type) {
		if ( type.isAssignableFrom(Stage.SessionFactory.class) ) {
			return type.cast( new StageSessionFactoryImpl( this ) );
		}
		if ( type.isAssignableFrom(Mutiny.SessionFactory.class) ) {
			return type.cast( new MutinySessionFactoryImpl( this ) );
		}
		return super.unwrap(type);
	}
}
