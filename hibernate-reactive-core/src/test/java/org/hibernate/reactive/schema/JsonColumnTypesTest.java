/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.java.DataHelper;

import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.containers.DatabaseConfiguration.getDatatypeQuery;
import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

/**
 * Check that the JSON properties are mapped as the expected type in the database.
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = DB2, reason = "JSON is not supported")
@DisabledFor(value = SQLSERVER, reason = "JSON is not supported with Vert.x 5.1")
@DisabledFor(value = ORACLE, reason = "JSON is not supported with Vert.x 5.1")
public class JsonColumnTypesTest extends BaseReactiveTest {

	private static final String TABLE_NAME = "JsonColumnTypes";

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( JsonColumns.class );
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		// We don't insert any value, so there's nothing to delete
		return voidFuture();
	}

	@Test
	public void testJsonObject(VertxTestContext context) {
		testDatatype( context, "jsonObject", expectedJsonObjectDatatype() );
	}

	@Test
	public void testMap(VertxTestContext context) {
		testDatatype( context, "jsonMap", expectedJsonDatatype() );
	}

	@Test
	public void testList(VertxTestContext context) {
		testDatatype( context, "jsonList", expectedJsonDatatype() );
	}

	@Test
	public void testEmbeddable(VertxTestContext context) {
		testDatatype( context, "jsonEmbeddable", expectedJsonDatatype() );
	}

	private void testDatatype(VertxTestContext context, String columnName, String expected) {
		test( context, getSessionFactory()
				.withTransaction( s -> s
						.createNativeQuery( getDatatypeQuery( TABLE_NAME, columnName ), String.class )
						.getSingleResult()
						.thenAccept( typeOnTheDb -> assertThat( toString( typeOnTheDb ).toLowerCase() )
								.isEqualTo( expected ) )
				)
		);
	}

	/**
	 * The type chosen by Hibernate Reactive for {@link JsonObject}
	 */
	private static String expectedJsonObjectDatatype() {
		return switch ( dbType() ) {
			case POSTGRESQL, MYSQL -> "json";
			// CockroachDB stores json as jsonb
			case COCKROACHDB -> "jsonb";
			// MariaDB defines json as an alias of longtext
			case MARIA -> "longtext";
			default -> throw new IllegalStateException( "JSON is not supported on " + dbType() );
		};
	}

	/**
	 * The type chosen by Hibernate ORM for {@link SqlTypes#JSON}
	 */
	private static String expectedJsonDatatype() {
		return switch ( dbType() ) {
			case POSTGRESQL, COCKROACHDB -> "jsonb";
			case MYSQL -> "json";
			case MARIA -> "longtext";
			default -> throw new IllegalStateException( "JSON is not supported on " + dbType() );
		};
	}

	private static String toString(Object result) {
		try {
			// MySQL returns a Blob
			return result instanceof Blob
					? new String( DataHelper.extractBytes( ( (Blob) result ).getBinaryStream() ) )
					: (String) result;
		}
		catch (SQLException sqlException) {
			throw new RuntimeException( sqlException );
		}
	}

	@Entity(name = "JsonColumns")
	@Table(name = TABLE_NAME)
	public static class JsonColumns {
		@Id
		Integer id;

		JsonObject jsonObject;

		@JdbcTypeCode(SqlTypes.JSON)
		Map<String, Object> jsonMap;

		@JdbcTypeCode(SqlTypes.JSON)
		List<Author> jsonList;

		@JdbcTypeCode(SqlTypes.JSON)
		Author jsonEmbeddable;
	}

	@Embeddable
	public static class Author {
		String name;
		String surname;
	}
}
