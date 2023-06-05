/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.Hibernate;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)

public class LazyOneToOneWithJoinColumnTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Endpoint.class, EndpointWebhook.class );
	}

	@Test
	public void testLoad(VertxTestContext context) {
		final Endpoint endpoint = new Endpoint();
		final EndpointWebhook webhook = new EndpointWebhook();
		endpoint.setWebhook( webhook );
		webhook.setEndpoint( endpoint );

		test( context, openMutinySession()
				.chain( session -> session
						.persist( endpoint )
						.chain( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session
						.find( Endpoint.class, endpoint.getId() )
						.invoke( optionalAnEntity -> {
							assertNotNull( optionalAnEntity );
							assertNotNull( optionalAnEntity.getWebhook() );
							// This is eager because the other table contains the reference
							assertTrue( Hibernate.isInitialized( optionalAnEntity.getWebhook() ) );
						} ) )
				.chain( this::openMutinySession )
				.chain( session -> session
						.find( EndpointWebhook.class, webhook.getId() )
						.invoke( optionalAnEntity -> {
							assertNotNull( optionalAnEntity );
							assertNotNull( optionalAnEntity.getEndpoint() );
							// This is actually lazy
							assertFalse( Hibernate.isInitialized( optionalAnEntity.getEndpoint() ) );
						} ) )
		);
	}

	@Test
	public void testQuery(VertxTestContext context) {
		final Endpoint endpoint = new Endpoint();
		endpoint.setAccountId( "XYZ_123"  );
		final EndpointWebhook webhook = new EndpointWebhook();
		endpoint.setWebhook( webhook );
		webhook.setEndpoint( endpoint );

		String query = "FROM Endpoint WHERE id = :id AND accountId = :accountId";
		test( context, openMutinySession()
				.chain( session -> session
						.persist( endpoint )
						.call( session::flush ) )
				.chain( this::openMutinySession )
				.chain( session -> session
						.createQuery( query, Endpoint.class )
						.setParameter( "id", endpoint.getId() )
						.setParameter( "accountId", endpoint.getAccountId() )
						.getSingleResultOrNull()
						.invoke( result -> {
							assertNotNull( result );
							assertTrue( Hibernate.isInitialized( result.getWebhook() ) );
							assertEquals( endpoint.getId(), result.getId() );
							assertEquals( webhook.getId(), result.getWebhook().getId() );
						} ) )
		);
	}

	@Entity(name = "Endpoint")
	@Table(name = "endpoints")
	public static class Endpoint {

		@Id
		@GeneratedValue
		private Long id;

		@OneToOne(mappedBy = "endpoint", fetch = FetchType.LAZY, cascade = CascadeType.ALL)
		private EndpointWebhook webhook;

		private String accountId;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public EndpointWebhook getWebhook() {
			return webhook;
		}

		public void setWebhook(EndpointWebhook webhook) {
			this.webhook = webhook;
		}

		public String getAccountId() {
			return accountId;
		}

		public void setAccountId(String accountId) {
			this.accountId = accountId;
		}

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
		private Long id;

		@OneToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "endpoint_id")
		private Endpoint endpoint;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public Endpoint getEndpoint() {
			return endpoint;
		}

		public void setEndpoint(Endpoint endpoint) {
			this.endpoint = endpoint;
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder( "EndpointWebhook{" );
			sb.append( id );
			sb.append( '}' );
			return sb.toString();
		}
	}
}
