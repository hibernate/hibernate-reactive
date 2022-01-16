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
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.mutiny.core.http.HttpServer;
import io.vertx.mutiny.ext.web.Router;
import io.vertx.mutiny.ext.web.RoutingContext;
import io.vertx.mutiny.ext.web.handler.BodyHandler;

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

	private Uni<Mutiny.SessionFactory> startHibernate() {
		return Uni.createFrom().item( emfSupplier );
	}

	@Override
	public Uni<Void> asyncStart() {
		final Uni<Mutiny.SessionFactory> startHibernate = vertx
				.executeBlocking( this.startHibernate() )
				.invoke( factory -> {
					this.emf = factory;
					LOG.infof( "✅ Hibernate Reactive is ready" );
				} );

		Router router = Router.router( vertx );
		BodyHandler bodyHandler = BodyHandler.create();
		router.post().handler( bodyHandler::handle );

		router.get( "/products" ).respond( this::listProducts );
		router.get( "/products/:id" ).respond( this::getProduct );
		router.post( "/products" ).respond( this::createProduct );

		this.httpServer = vertx.createHttpServer();
		Uni<HttpServer> startHttpServer = httpServer
				.requestHandler( router::handle )
				.listen( HTTP_PORT )
				.onItem().invoke( () -> LOG.infof( "✅ HTTP server listening on port %s", HTTP_PORT ) );

		return Uni.combine().all().unis( startHibernate, startHttpServer ).discardItems();
	}

	@Override
	public Uni<Void> asyncStop() {
		return httpServer.close().invoke( emf::close );
	}

	private static Product logFound(Product product) {
		LOG.tracef( "Product found: %s", product );
		return product;
	}

	private static Product logCreated(Product product) {
		LOG.tracef( "Product added: %s", product );
		return product;
	}

	private Uni<List<Product>> listProducts(RoutingContext ctx) {
		return emf.withSession( session -> session.createQuery( "from Product", Product.class ).getResultList() );
	}

	private Uni<Product> getProduct(RoutingContext ctx) {
		long id = Long.parseLong( ctx.pathParam( "id" ) );
		return emf.withSession( session -> session.find( Product.class, id ).invoke( ProductVerticle::logFound ) )
				.onItem().ifNull().continueWith( Product::new );
	}

	private Uni<Product> createProduct(RoutingContext ctx) {
		Product product = ctx.getBodyAsJson().mapTo( Product.class );
		return emf.withTransaction( session -> session.persist( product ) )
				.map( v -> product )
				.invoke( ProductVerticle::logCreated )
				.replaceWith( product );
	}
}
