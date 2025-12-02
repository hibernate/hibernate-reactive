/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.techempower;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.reactive.it.techempower.VertxServer;
import org.hibernate.reactive.it.techempower.WorldVerticle;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import junit.framework.AssertionFailedError;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
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
 * Test that the Rest API is correct.
 * <p>
 *     <dl>
 *         <dt><a href="http://localhost:8088/createData">http://localhost:8080/createData</a></dt>
 *         <dd>Populate the database with an initial dataset</dd>
 *         <dt><a href="http://localhost:8088/updates?queries=20">http://localhost:8088/updates?queries=20</a></dt>
 *         <dd>Update 20 random entries</dd>
 *     </dl>
 * </p>
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Timeout(value = 5, timeUnit = MINUTES)
public class TechEmpowerTest {

	private static final int VERTICLE_INSTANCES = 10;

	private static final int REQUEST_NUMBER = 500;

	@Test
	public void testWorldRepository(VertxTestContext context) {
		final Vertx vertx = Vertx.vertx( VertxServer.vertxOptions() );

		Mutiny.SessionFactory sf = VertxServer
				.createHibernateSessionFactory( VertxServer.USE_DOCKER, vertx )
				.unwrap( Mutiny.SessionFactory.class );

		final WebClient webClient = WebClient.create( vertx );

		final DeploymentOptions deploymentOptions = new DeploymentOptions();
		deploymentOptions.setInstances( VERTICLE_INSTANCES );

		vertx
				.deployVerticle( () -> new WorldVerticle( () -> sf ), deploymentOptions )
				.map( ignore -> webClient )
				.compose( this::createData )
				.map( ignore -> webClient )
				.compose( this::updates )
				.onSuccess( res -> context.completeNow() )
				.onFailure( context::failNow )
				.eventually( unused -> vertx.close() );
	}

	/**
	 * Create several products using http requests
	 */
	private Future<?> createData(WebClient webClient) {
		return webClient
				.get( WorldVerticle.HTTP_PORT, "localhost", "/createData" )
				.send()
				.compose( this::handle );
	}

	/**
	 * Use http requests to find the products previously created and validate them
	 */
	private Future<?> updates(WebClient webClient) {
		List<Future<Void>> sentRequests = new ArrayList<>();
		for ( int i = 0; i < REQUEST_NUMBER; i++ ) {
			// Send the request
			Future<Void> send = webClient
					.get( WorldVerticle.HTTP_PORT, "localhost", "/updates?queries=20" )
					.send()
					.compose( this::handle );

			sentRequests.add( send );
		}
		return all( sentRequests );
	}

	private Future<Void> handle(HttpResponse<Buffer> response) {
		switch ( response.statusCode() ) {
			case 200:
			case 204:
				return succeededFuture();
			default:
				return failedFuture( new AssertionFailedError( "Expected status code 200 or 204, but was " + response.statusCode() ) );
		}
	}
}
