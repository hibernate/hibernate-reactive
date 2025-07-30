/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.verticle;

import java.util.List;
import java.util.function.Supplier;

import org.hibernate.reactive.mutiny.Mutiny;

import org.jboss.logging.Logger;

import io.smallrye.mutiny.Uni;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import static io.vertx.core.Future.all;

public class ProductVerticle extends AbstractVerticle {

	private static final Logger LOG = Logger.getLogger( ProductVerticle.class );

	private final Supplier<Mutiny.SessionFactory> emfSupplier;
	private Mutiny.SessionFactory emf;
	private HttpServer httpServer;

	/**
	 * The port to use to listen to requests
	 */
	public static final int HTTP_PORT = 8088;

	public ProductVerticle(Supplier<Mutiny.SessionFactory> emfSupplier) {
		this.emfSupplier = emfSupplier;
	}

	@Override
	public void start(Promise<Void> startPromise) {
		final Future<Mutiny.SessionFactory> startHibernate = vertx
				.executeBlocking( this::startHibernate )
				.onSuccess( s -> LOG.infof( "✅ Hibernate Reactive is ready" ) );

		Router router = Router.router( vertx );
		BodyHandler bodyHandler = BodyHandler.create();
		router.post().handler( bodyHandler );

		router.get( "/products" ).respond( this::listProducts );
		router.get( "/products/:id" ).respond( this::getProduct );
		router.post( "/products" ).respond( this::createProduct );

		this.httpServer = vertx.createHttpServer();
		final Future<HttpServer> startHttpServer = httpServer
				.requestHandler( router )
				.listen( HTTP_PORT )
				.onSuccess( v -> LOG.infof( "✅ HTTP server listening on port %s", HTTP_PORT ) );

		all( startHibernate, startHttpServer )
				.onSuccess( s -> startPromise.complete() )
				.onFailure( startPromise::fail );
	}

	private Mutiny.SessionFactory startHibernate() {
		this.emf = emfSupplier.get();
		return this.emf;
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		httpServer.close()
				.onComplete( unused -> emf.close() )
				.onSuccess( s -> stopPromise.complete() )
				.onFailure( stopPromise::fail );
	}

	private static void logFound(Product product) {
		LOG.tracef( "Product found: %s", product );
	}

	private static void logCreated(Product product) {
		LOG.tracef( "Product added: %s", product );
	}

	private Future<List<Product>> listProducts(RoutingContext ctx) {
		return toFuture( emf.withSession( session -> session
				.createQuery( "from Product", Product.class ).getResultList()
		) );
	}

	private Future<Product> getProduct(RoutingContext ctx) {
		long id = Long.parseLong( ctx.pathParam( "id" ) );
		return toFuture( emf.withSession( session -> session.find( Product.class, id ) )
								 .invoke( ProductVerticle::logFound )
								 .onItem().ifNull().continueWith( Product::new )
		);
	}

	private Future<Product> createProduct(RoutingContext ctx) {
		Product product = ctx.body().asPojo( Product.class );
		return toFuture( emf.withTransaction( session -> session.persist( product ) )
								 .map( v -> product )
								 .invoke( ProductVerticle::logCreated )
								 .replaceWith( product )
		);
	}

	private static <U> Future<U> toFuture(Uni<U> uni) {
		return Future.fromCompletionStage( uni.convert().toCompletionStage() );
	}
}
