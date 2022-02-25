/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.annotations.DynamicInsert;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class IdentityGeneratorDynamicInsertTest extends BaseReactiveTest {

	/**
	 * When {@link AvailableSettings#USE_GET_GENERATED_KEYS} is enabled, different
	 * queries will be used for each datastore to get the id
	 */
	public static class EnableUseGetGeneratedKeys extends IdentityGeneratorDynamicInsertTest {

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
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( EntityWithIdentity.class );
		configuration.setProperty( AvailableSettings.USE_GET_GENERATED_KEYS, "false" );
		return configuration;
	}

	private CompletionStage<?> populateDb(TestContext context) {
		final List<EntityWithIdentity> identities = new ArrayList<>( ENTITY_NUMBER );
		for ( int i = 0; i < ENTITY_NUMBER; i++ ) {
			identities.add( new EntityWithIdentity( i ) );
		}
		return getSessionFactory()
				.withTransaction( (session, tx) -> session.persist( identities.toArray() ) )
				.thenAccept( ignore -> {
					Long assignedId = 0L;
					for ( EntityWithIdentity identity : identities ) {
						context.assertNotNull( identity.id );
						context.assertTrue( identity.id > assignedId );
						assignedId = identity.id;
					}
				} );
	}

	@Test
	public void testIdentityGenerator(TestContext context) {
		test( context, populateDb( context )
				.thenCompose( v -> openSession() )
				.thenCompose( session ->
					  session.createQuery( "FROM EntityWithIdentity ORDER BY position ASC", EntityWithIdentity.class )
					  .getResultList() )
				.thenAccept( list -> {
					context.assertEquals( ENTITY_NUMBER, list.size() );
					int i = 0;
					for ( EntityWithIdentity entity : list ) {
						context.assertEquals( i, entity.getPosition() );
						i++;
					}
				} ) );
	}

	@Entity(name = "EntityWithIdentity")
	@DynamicInsert
	private static class EntityWithIdentity {
		private static final String PREFIX = "Entity: ";
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		Long id;

		@Column(unique = true)
		String name;

		@Column
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
