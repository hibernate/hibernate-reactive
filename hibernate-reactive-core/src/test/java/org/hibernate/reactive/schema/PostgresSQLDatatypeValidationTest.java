/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

import java.math.BigDecimal;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.annotations.Type;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class PostgresSQLDatatypeValidationTest extends SchemaBaseReactiveTest {

	protected Class[] getAnnotatedClasses() {
		Class[] classes = new Class[1];
		classes[0] = TestEntity.class;
		return classes;
	}

	@Rule
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	@Test
	public void testValidationSucceed(TestContext context) {
		Configuration configuration = constructConfiguration( "validate" );
		configuration.addAnnotatedClass( TestEntity.class );

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory()
								.withTransaction(
										(session, t) -> session.createQuery( "FROM TestDatatypesEntity", TestEntity.class )
												.getResultList() ) )
						.thenAccept( results -> context.assertTrue( results.isEmpty() ) )
		);
	}

	@Test
	public void testDataTypesSql(TestContext context) {
		final TestEntity testEntity = new TestEntity();
		testEntity.id = 9;
		testEntity.numberColumnValue = new BigDecimal( 3.14159 );
		testEntity.textColumnValue = "textValue";

		String columnNumberTypeQuery =
				"select data_type from information_schema.columns " +
						"where table_name = 'testdatatypesentity' and column_name = 'numbercolumnname';";
		String columnTextTypeQuery =
				"select data_type from information_schema.columns " +
						"where table_name = 'testdatatypesentity' and column_name = 'textcolumnname';";

		final Configuration configuration = constructConfiguration( "update" );
		configuration.addAnnotatedClass( TestEntity.class );

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, t) -> session.persist( testEntity ) ) )
						.thenCompose( v1 -> openSession()
								.thenCompose( s -> s
										.find( TestEntity.class, testEntity.id )
										.thenAccept( result -> {
													context.assertNotNull( result );
													context.assertEquals( testEntity.numberColumnValue, testEntity.numberColumnValue );
													context.assertEquals( testEntity.textColumnValue, testEntity.textColumnValue );
												}
										).thenCompose( v -> s.createNativeQuery( columnNumberTypeQuery, String.class )
												.getResultList()
												.thenAccept( list -> {
													context.assertEquals( 1, list.size() );
													context.assertTrue( list.get( 0 ).equals( "numeric" ) );
												} )
										).thenCompose( v -> s.createNativeQuery( columnTextTypeQuery, String.class )
												.getResultList()
												.thenAccept( list -> {
													context.assertEquals( 1, list.size() );
													context.assertTrue( list.get( 0 ).equals( "text" ) );
												} )
										)
								)
						)
		);
	}

	@Entity(name = "TestDatatypesEntity")
	public static class TestEntity {
		@Id
		public Integer id;

		@Column(name = "numberColumnName")
		BigDecimal numberColumnValue;

		@Column(name = "textColumnName")
		@Type(type = "text")
		String textColumnValue;
	}
}
