/**
 * Hibernate Reactive is an adaptation of Hibernate ORM to the
 * world of reactive programming, and replaces JDBC for database
 * access with a non-blocking database client.
 * <p>
 * By default, non-blocking access to the database is provided
 * by the {@link io.vertx.sqlclient.SqlClient Vert.x SQL client}.
 * <p>
 * Two parallel APIs are available:
 * <ul>
 *     <li>{@link org.hibernate.reactive.stage.Stage} is an API
 *     designed around Java's {@link java.util.concurrent.CompletionStage}.
 *     <li>{@link org.hibernate.reactive.mutiny.Mutiny} is an API
 *     designed around Mutiny's {@link io.smallrye.mutiny.Uni}.
 * </ul>
 * To get started, see either
 * {@link org.hibernate.reactive.stage.Stage.SessionFactory} or
 * {@link org.hibernate.reactive.mutiny.Mutiny.SessionFactory}.
 */
package org.hibernate.reactive;
