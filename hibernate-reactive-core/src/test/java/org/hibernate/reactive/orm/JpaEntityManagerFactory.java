/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.orm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceUnitInfo;

import org.hibernate.jpa.boot.internal.EntityManagerFactoryBuilderImpl;
import org.hibernate.jpa.boot.internal.PersistenceUnitInfoDescriptor;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;

public class JpaEntityManagerFactory {
	private EntityManager em;
	private EntityManagerFactory emf;
	private Class[] entityClasses;

	public JpaEntityManagerFactory(Class[] entityClasses) {
		this.entityClasses = entityClasses;
	}

	public EntityManager getEntityManager() {
		if( emf == null ) {
			emf = getEntityManagerFactory ( );
			if( em == null ) {
				em = emf.createEntityManager();
			}
		}

		return em;
	}

	protected EntityManagerFactory getEntityManagerFactory() {
		PersistenceUnitInfo persistenceUnitInfo = getPersistenceUnitInfo(
				getClass().getSimpleName());
		Map<String, Object> configuration = new HashMap<String, Object>();
		return new EntityManagerFactoryBuilderImpl(
				new PersistenceUnitInfoDescriptor( persistenceUnitInfo), configuration)
				.build();
	}

	protected HibernatePersistenceUnitInfo getPersistenceUnitInfo(String name) {
		return new HibernatePersistenceUnitInfo(name, getEntityClassNames(), getProperties());
	}

	protected Properties getProperties() {
		Properties properties = new Properties();
		properties.put( Settings.URL, DatabaseConfiguration.getJdbcUrl() );
		return properties;
	}


	private List<String> getEntityClassNames() {
		return new ArrayList<String>( 0 );
	}

	public void close() {
		if( emf != null ) {
			emf.close();
		}
	}
}
