/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.hibernate.cfg.Configuration;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.hibernate.reactive.provider.ReactivePersistenceProvider;
import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import jakarta.persistence.Entity;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Id;
import jakarta.persistence.PersistenceConfiguration;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.BaseReactiveTest.setDefaultProperties;

@Timeout(value = 10, unit = MINUTES)
public class ReactivePersistenceConfigurationTest {

	private final ReactivePersistenceProvider provider = new ReactivePersistenceProvider();

	@Test
	public void createsReactiveFactoryWhenProviderIsReactive() {
		Book book = new Book( 1, "The Book of M" );

		EntityManagerFactory factory = provider.createEntityManagerFactory( configuration(
				ReactivePersistenceProvider.class.getName(),
				defaultProperties()
		) );
		try {
			assertThat( findAfterPersist( factory, book ) ).isEqualTo( book );
		}
		finally {
			factory.close();
		}
	}

	@Test
	public void returnsNullWhenProviderIsNotReactive() {
		assertThat( provider.createEntityManagerFactory( configuration( null, defaultProperties() ) ) )
				.isNull();
		assertThat( provider.createEntityManagerFactory(
				configuration( HibernatePersistenceProvider.class.getName(), defaultProperties() ) ) )
				.isNull();
	}

	private static Book findAfterPersist(EntityManagerFactory factory, Book book) {
		Stage.SessionFactory sessionFactory = factory.unwrap( Stage.SessionFactory.class );
		return sessionFactory.withTransaction( session -> session.persist( book ) )
				.thenCompose( v -> sessionFactory.withSession( session -> session.find( Book.class, book.id ) ) )
				.toCompletableFuture().join();
	}

	private static PersistenceConfiguration configuration(String providerName, Map<String, Object> properties) {
		PersistenceConfiguration configuration = new PersistenceConfiguration( "reactive-configuration" )
				.managedClass( Book.class )
				.properties( properties );
		return providerName == null ? configuration : configuration.provider( providerName );
	}

	private static Map<String, Object> defaultProperties() {
		Configuration configuration = new Configuration();
		setDefaultProperties( configuration );
		Map<String, Object> properties = new HashMap<>();
		configuration.getProperties().forEach( (key, value) -> properties.put( (String) key, value ) );
		return properties;
	}

	@Entity(name = "Book")
	@Table(name = "PersistenceConfigurationBook")
	public static class Book {
		@Id
		Integer id;
		String title;

		public Book() {
		}

		public Book(Integer id, String title) {
			this.id = id;
			this.title = title;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			return Objects.equals( title, ( (Book) o ).title );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( title );
		}

		@Override
		public String toString() {
			return id + ": " + title;
		}
	}
}
