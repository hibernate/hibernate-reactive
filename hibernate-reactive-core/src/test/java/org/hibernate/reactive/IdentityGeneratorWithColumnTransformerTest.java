/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.annotations.ColumnTransformer;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Similar to {@link IdentityGeneratorTest} but enables SQL comments and uses a {@link ColumnTransformer}.
 */
public class IdentityGeneratorWithColumnTransformerTest extends BaseReactiveTest {

	/**
	 * When {@link AvailableSettings#USE_GET_GENERATED_KEYS} is enabled, different
	 * queries will be used for each datastore to get the id
	 */
	public static class EnableUseGetGeneratedKeysTest extends IdentityGeneratorWithColumnTransformerTest {

		@Override
		protected Configuration constructConfiguration() {
			Configuration configuration = super.constructConfiguration();
			configuration.setProperty( AvailableSettings.USE_GET_GENERATED_KEYS, "true" );
			return configuration;
		}
	}

	// The number of entities we want to create
	private static final int ENTITY_NUMBER = 100;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( EntityWithIdentity.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.USE_SQL_COMMENTS, "true" );
		return configuration;
	}

	private CompletionStage<?> populateDb() {
		final List<EntityWithIdentity> identities = new ArrayList<>( ENTITY_NUMBER );
		for ( int i = 0; i < ENTITY_NUMBER; i++ ) {
			identities.add( new EntityWithIdentity( i ) );
		}
		return getSessionFactory()
				.withTransaction( (session, tx) -> session.persist( identities.toArray() ) )
				.thenAccept( ignore -> {
					long assignedId = 0L;
					for ( EntityWithIdentity identity : identities ) {
						assertNotNull( identity.id );
						assertTrue( identity.id > assignedId );
						assignedId = identity.id;
					}
				} );
	}

	@Test
	@Timeout(value = 10, timeUnit = MINUTES)

	public void testIdentityGenerator(VertxTestContext context) {
		test( context, populateDb()
				.thenCompose( v -> openSession() )
				.thenCompose( session ->
					  session.createSelectionQuery( "FROM EntityWithIdentity ORDER BY position ASC", EntityWithIdentity.class )
					  .getResultList() )
				.thenAccept( list -> {
					assertEquals( ENTITY_NUMBER, list.size() );
					int i = 0;
					for ( EntityWithIdentity entity : list ) {
						assertEquals( i * 2, entity.getPosition() );
						i++;
					}
				} ) );
	}

	@Entity(name = "EntityWithIdentity")
	@Table( name = "`Table with default values`")
	private static class EntityWithIdentity {
		private static final String PREFIX = "Entity: ";
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		Long id;

		@Column(unique = true)
		String name;

		@Column(name = "position")
		@ColumnTransformer(forColumn = "position", write = "? * 2")
		private int position;

		public EntityWithIdentity() {
		}

		public EntityWithIdentity(int index) {
			this.name =  PREFIX + index;
			this.position = index;
		}

		public int getPosition() {
			return position;
		}

		public void setPosition(int position) {
			this.position = position;
		}

		@Override
		public String toString() {
			return id + ":" + name + ":" + position;
		}
	}
}
