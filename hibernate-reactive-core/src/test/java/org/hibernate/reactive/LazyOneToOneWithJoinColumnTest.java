/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.Hibernate;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class LazyOneToOneWithJoinColumnTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Endpoint.class, EndpointWebhook.class );
	}

	@Test
	public void testLoad(TestContext context) {
		final Endpoint endpoint = new Endpoint();
		final EndpointWebhook webhook = new EndpointWebhook();
		endpoint.webhook = webhook;
		webhook.endpoint = endpoint;

		test( context, openMutinySession()
				.chain( session -> session
						.persist( endpoint )
						.chain( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session
						.find( Endpoint.class, endpoint.id )
						.invoke( optionalAnEntity -> {
							context.assertNotNull( optionalAnEntity );
							context.assertNotNull( optionalAnEntity.webhook );
							// This is eager because the other table contains the reference
							context.assertTrue( Hibernate.isInitialized( optionalAnEntity.webhook ) );
						} ) )
				.chain( this::openMutinySession )
				.chain( session -> session
						.find( EndpointWebhook.class, webhook.id )
						.invoke( optionalAnEntity -> {
							context.assertNotNull( optionalAnEntity );
							context.assertNotNull( optionalAnEntity.endpoint );
							// This i actually lazy
							context.assertFalse( Hibernate.isInitialized( optionalAnEntity.endpoint ) );
						} ) )
		);
	}

	@Test
	public void testQuery(TestContext context) {
		final Endpoint endpoint = new Endpoint();
		endpoint.accountId = "XYZ_123";
		final EndpointWebhook webhook = new EndpointWebhook();
		endpoint.webhook = webhook;
		webhook.endpoint = endpoint;

		String query = "FROM Endpoint WHERE id = :id AND accountId = :accountId";
		test( context, openMutinySession()
				.chain( session -> session
						.persist( endpoint )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session
						.createQuery( query, Endpoint.class )
						.setParameter( "id", endpoint.id )
						.setParameter( "accountId", endpoint.accountId )
						.getSingleResultOrNull()
						.invoke( result -> {
							context.assertNotNull( result );
							context.assertTrue( Hibernate.isInitialized( result.webhook ) );
							context.assertEquals( endpoint.id, result.id );
							context.assertEquals( webhook.id, result.webhook.id );
						} ) )
		);
	}

	@Entity(name = "Endpoint")
	@Table(name = "endpoints")
	public static class Endpoint {

		@Id
		@GeneratedValue
		public Long id;

		@OneToOne(mappedBy = "endpoint", fetch = FetchType.LAZY, cascade = CascadeType.ALL)
		public EndpointWebhook webhook;

		public String accountId;


		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder( "Endpoint{" );
			sb.append( id );
			sb.append( ", " );
			sb.append( accountId );
			sb.append( '}' );
			return sb.toString();
		}
	}

	@Entity
	@Table(name = "endpoint_webhooks")
	public static class EndpointWebhook {

		@Id
		@GeneratedValue
		public Long id;

		@OneToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "endpoint_id")
		public Endpoint endpoint;

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder( "EndpointWebhook{" );
			sb.append( id );
			sb.append( '}' );
			return sb.toString();
		}
	}
}
