/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.reactive.it.verticle.Product;
import org.hibernate.reactive.it.verticle.ProductVerticle;
import org.hibernate.reactive.it.verticle.VertxServer;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import junit.framework.AssertionFailedError;

import org.jboss.logging.Logger;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static io.vertx.core.Future.all;
import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.util.concurrent.TimeUnit.MINUTES;


/**
 * Create a certain number of entities and check that they can be found using
 * the Vert.x web client.
 * <p>
 * The actual purpose of this test is to make sure that multiple parallel
 * http requests don't share a session.
 * </p>
 * <p>
 * This test relies on an exception being thrown when a thread gets a session
 * that's been close ahead of time by someone else.
 * Theoretically, everything could happen in the right order because of chance,
 * but it's unlikely and at the moment I don't have a better solution.
 * See <a href="https://github.com/hibernate/hibernate-reactive/issues/1073">the related issue</a>
 * for more details.
 * <p>
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Timeout(value = 10, timeUnit = MINUTES)
public class LocalContextTest {

	private static final Logger LOG = Logger.getLogger( LocalContextTest.class );

	// Number of requests: each request is a product created and then searched
	private static final int REQUEST_NUMBER = 20;

	// Keep this value to 1 or the test won't fail when expected
	private static final int VERTICLE_INSTANCES = 1;

	@Test
	public void testProductsGeneration(VertxTestContext context) {
		final Vertx vertx = Vertx.vertx( VertxServer.vertxOptions() );

		Mutiny.SessionFactory sf = VertxServer
				.createHibernateSessionFactory( VertxServer.USE_DOCKER, vertx )
				.unwrap( Mutiny.SessionFactory.class );

		final WebClient webClient = WebClient.create( vertx );

		final DeploymentOptions deploymentOptions = new DeploymentOptions();
		deploymentOptions.setInstances( VERTICLE_INSTANCES );

		vertx
				.deployVerticle( () -> new ProductVerticle( () -> sf ), deploymentOptions )
				.map( ignore -> webClient )
				.compose( this::createProducts )
				.map( ignore -> webClient )
				.compose( this::findProducts )
				.onSuccess( res -> context.completeNow() )
				.onFailure( context::failNow )
				.eventually( () -> vertx.close() );
	}

	/**
	 * Create several products using http requests
	 *
	 * @see #REQUEST_NUMBER
	 */
	private Future<?> createProducts(WebClient webClient) {
		List<Future<?>> postRequests = new ArrayList<>();
		for ( int i = 0; i < REQUEST_NUMBER; i++ ) {
			final Product product = new Product( i + 1 );

			final Future<HttpResponse<Buffer>> send = webClient
					.post( ProductVerticle.HTTP_PORT, "localhost", "/products" )
					.sendJsonObject( JsonObject.mapFrom( product ) )
					.onComplete( v -> LOG.debugf( "Request sent: %s", product.getId() ) );

			postRequests.add( send );
		}
		return all( postRequests );
	}

	/**
	 * Use http requests to find the products previously created and validate them
	 *
	 * @see #REQUEST_NUMBER
	 */
	private Future<?> findProducts(WebClient webClient) {
		List<Future<?>> requestSent = new ArrayList<>();
		for ( int i = 0; i < REQUEST_NUMBER; i++ ) {
			final Product expected = new Product( i + 1 );

			// Send the request
			final Future<Void> send = webClient
					.get( ProductVerticle.HTTP_PORT, "localhost", "/products/" + expected.getId() )
					.send()
					.compose( event -> handle( expected, event ) );

			requestSent.add( send );
		}
		return all( requestSent );
	}

	/**
	 * Check that the expected product is returned by the response.
	 */
	private Future<Void> handle(Product expected, HttpResponse<Buffer> response) {
		if ( response.statusCode() != 200 ) {
			return failedFuture( new AssertionFailedError( "Expected status code 200 but was " + response.statusCode() ) );
		}

		final Product found = response.bodyAsJson( Product.class );
		LOG.debugf( "Found: %s - Expected: %s [%s]", found, expected, expected.equals( found ) );
		if ( !expected.equals( found ) ) {
			return failedFuture( new AssertionFailedError( "Wrong value returned. Expected " + expected + " but was " + found ) );
		}

		return succeededFuture();
	}
}
