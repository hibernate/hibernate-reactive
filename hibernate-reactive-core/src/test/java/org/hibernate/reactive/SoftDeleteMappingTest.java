/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.SoftDelete;
import org.hibernate.annotations.SoftDeleteType;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.type.NumericBooleanConverter;
import org.hibernate.type.TrueFalseConverter;
import org.hibernate.type.YesNoConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.junit.jupiter.api.Assertions.assertEquals;

/*
 * Tests validity of @SoftDelete annotation converter types
 * as well as verifying logged 'create table' and 'update' queries for each database
 *
 * @see org.hibernate.annotations.SoftDelete
 * @see org.hibernate.orm.test.softdelete.MappingTest
 */

public class SoftDeleteMappingTest extends BaseReactiveTest {

	enum ENTITY_TYPE {
		BOOLEAN,
		NUMERIC,
		YES_NO,
		NO_YES,
		TRUE_FALSE,
		MAPPED_COLUMN
	}
	private static SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		sqlTracker = new SqlStatementTracker( SoftDeleteMappingTest::filter, configuration.getProperties() );
		return configuration;
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	protected static boolean filter(String s) {
		String[] accepted = { "create table ", "update " };
		for ( String valid : accepted ) {
			if ( s.toLowerCase().startsWith( valid ) ) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of(
				BooleanEntity.class,
				NumericEntity.class,
				YesNoEntity.class,
				TrueFalseEntity.class,
				ReversedYesNoEntity.class,
				MappedColumnEntity.class
		);
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {

		Object[] entities = {
				new BooleanEntity( 1, "first" ),
				new BooleanEntity( 2, "second" ),
				new NumericEntity( 1, "first" ),
				new NumericEntity( 2, "second" ),
				new TrueFalseEntity( 1, "first" ),
				new TrueFalseEntity( 2, "second" ),
				new YesNoEntity( 1, "first" ),
				new YesNoEntity( 2, "second" ),
				new ReversedYesNoEntity( 1, "first" ),
				new ReversedYesNoEntity( 2, "second" ),
				new MappedColumnEntity( 1, "first" ),
				new MappedColumnEntity( 2, "second" )
		};

		test( context, getSessionFactory().withTransaction( session -> session.persist( entities ) ) );
	}

	@Test
	public void verifyTableCreationQuery(VertxTestContext context) {
		test( context, getSessionFactory().withSession( s ->
				s.find( MappedColumnEntity.class, 1 )
						.thenAccept( result -> {
							assertEquals( "first", result.name );
							assertLoggedQuery( getExpectedCreateQuery( ENTITY_TYPE.YES_NO ) );
							assertLoggedQuery( getExpectedCreateQuery( ENTITY_TYPE.NO_YES ) );
							assertLoggedQuery( getExpectedCreateQuery( ENTITY_TYPE.BOOLEAN ) );
							assertLoggedQuery( getExpectedCreateQuery( ENTITY_TYPE.TRUE_FALSE ) );
							assertLoggedQuery( getExpectedCreateQuery( ENTITY_TYPE.NUMERIC ) );
							assertLoggedQuery( getExpectedCreateQuery( ENTITY_TYPE.MAPPED_COLUMN ) );
						} ) ) );


	}

	private void testEntity(
			VertxTestContext context,
			Class entityClass,
			String updateQuery,
			String selectionQuery,
			String expectedQueryStart) {
		test( context, getSessionFactory().withSession( s ->
				s.createMutationQuery( updateQuery ).executeUpdate() )
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createQuery( selectionQuery, entityClass ).getSingleResult()
										.thenAccept( result -> assertLoggedQuery( expectedQueryStart ) ) ) )
		);
	}

	@Test
	public void testBooleanEntity(VertxTestContext context) {
		String expectedQuery = "update boolean_entity set name=null where id=1 and deleted=false";
		switch ( dbType() ) {
			case ORACLE:
				expectedQuery = "update boolean_entity be1_0 set be1_0.name=null where id=1 and deleted=0";
				break;
			case SQLSERVER:
			case MYSQL:
			case MARIA:
				expectedQuery = "update boolean_entity set name=null where id=1 and deleted=0";
				break;
			default:
				break;
		}

		testEntity( context, BooleanEntity.class,
					"update BooleanEntity set name = null where id = 1",
					"from BooleanEntity where id = 1",
					expectedQuery
		);
	}

	@Test
	public void testNumericEntity(VertxTestContext context) {
		String expectedQuery = isDbType( ORACLE ) ?
				"update numeric_entity ne1_0 set ne1_0.name=null where id=1 and deleted=0" :
				"update numeric_entity set name=null where id=1 and deleted=0";
				;

		testEntity( context, NumericEntity.class,
					"update NumericEntity set name = null where id = 1",
					"from NumericEntity where id = 1",
					expectedQuery
		);
	}

	@Test
	public void testTrueFalseEntity(VertxTestContext context) {
		String expectedQuery = isDbType( ORACLE ) ?
				"update true_false_entity tfe1_0 set tfe1_0.name=null where id=1 and deleted='F'" :
				"update true_false_entity set name=null where id=1 and deleted='F'";

		testEntity( context, TrueFalseEntity.class,
					"update TrueFalseEntity set name = null where id = 1",
					"from TrueFalseEntity where id = 1",
					expectedQuery
		);
	}

	@Test
	public void testYesNoEntity(VertxTestContext context) {
		String expectedQuery = isDbType( ORACLE ) ?
				"update yes_no_entity yne1_0 set yne1_0.name=null where id=1 and deleted='N'" :
				"update yes_no_entity set name=null where id=1 and deleted='N'";

		testEntity( context,
					TrueFalseEntity.class,
					"update YesNoEntity set name = null where id = 1",
					"from YesNoEntity where id = 1",
					expectedQuery
		);
	}

	@Test
	public void testReversedYesNoEntity(VertxTestContext context) {
		String expectedQuery = isDbType( ORACLE ) ?
				"update reversed_yes_no_entity ryne1_0 set ryne1_0.name=null where id=1 and active='Y'" :
				"update reversed_yes_no_entity set name=null where id=1 and active='Y'";

		testEntity( context,
					TrueFalseEntity.class,
					"update ReversedYesNoEntity set name = null where id = 1",
					"from ReversedYesNoEntity where id = 1",
					expectedQuery
		);
	}

	@Test
	public void testMappedColumnEntity(VertxTestContext context) {
		String expectedQuery = "update MappedColumnEntity set name=null where id=1 and is_deleted=false";

		switch ( dbType() ) {
			case MYSQL:
			case MARIA:
			case SQLSERVER:
				expectedQuery = "update MappedColumnEntity set name=null where id=1 and is_deleted=0";
				break;
			case ORACLE:
				expectedQuery = "update MappedColumnEntity mce1_0 set mce1_0.name=null where id=1 and is_deleted=0";
				break;
			default:
				break;
		}

		testEntity( context, TrueFalseEntity.class,
					"update MappedColumnEntity set name = null where id = 1",
					"from MappedColumnEntity where id = 1",
					expectedQuery
		);
	}

	public static boolean isDbType(DatabaseConfiguration.DBType dbType) {
		return dbType() == dbType;
	}

	private void assertLoggedQuery(String expectedQuery) {
		String foundQuery = null;
		for ( String loggedQuery : sqlTracker.getLoggedQueries() ) {
			if ( loggedQuery.startsWith( expectedQuery ) ) {
				foundQuery = loggedQuery;
				if ( foundQuery != null ) {
					return;
				}
			}
		}
		Assertions.assertEquals( expectedQuery, foundQuery );
	}

	private String getExpectedCreateQuery(ENTITY_TYPE entityType ) {
		switch ( dbType() ) {
			case MYSQL:
				switch( entityType ) {
					case YES_NO: 		return "create table yes_no_entity (deleted char(1) not null comment 'Soft-delete indicator' check (deleted in ('N','Y')), id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case NO_YES: 		return "create table reversed_yes_no_entity (active char(1) not null comment 'Soft-delete indicator' check (active in ('Y','N')), id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case BOOLEAN: 		return "create table boolean_entity (deleted bit not null comment 'Soft-delete indicator', id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case TRUE_FALSE: 	return "create table true_false_entity (deleted char(1) not null comment 'Soft-delete indicator' check (deleted in ('F','T')), id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case NUMERIC: 		return "create table numeric_entity (deleted integer not null comment 'Soft-delete indicator' check (deleted in (0,1)), id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case MAPPED_COLUMN: return "create table MappedColumnEntity (id integer not null, is_deleted bit, name varchar(255), primary key (id)) engine=InnoDB";
				}
				break;
			case ORACLE:
				switch( entityType ) {
					case YES_NO: 		return "create table yes_no_entity (deleted char(1) not null check (deleted in ('N','Y')), id number(10,0) not null, name varchar2(255 char), primary key (id))";
					case NO_YES: 		return "create table reversed_yes_no_entity (active char(1) not null check (active in ('Y','N')), id number(10,0) not null, name varchar2(255 char), primary key (id))";
					case TRUE_FALSE: 	return "create table true_false_entity (deleted char(1) not null check (deleted in ('F','T')), id number(10,0) not null, name varchar2(255 char), primary key (id))";
					case BOOLEAN: 		return "create table boolean_entity (deleted number(1,0) not null, id number(10,0) not null, name varchar2(255 char), primary key (id))";
					case NUMERIC: 		return "create table numeric_entity (deleted number(10,0) not null check (deleted in (0,1)), id number(10,0) not null, name varchar2(255 char), primary key (id))";
					case MAPPED_COLUMN: return "create table MappedColumnEntity (id number(10,0) not null, is_deleted number(1,0) check (is_deleted in (0,1)), name varchar2(255 char), primary key (id))";
				}
				break;
			case MARIA:
				switch ( entityType ) {
					case YES_NO: 		return "create table yes_no_entity (deleted char(1) not null comment 'Soft-delete indicator' check (deleted in ('N','Y')), id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case NO_YES: 		return "create table reversed_yes_no_entity (active char(1) not null comment 'Soft-delete indicator' check (active in ('Y','N')), id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case TRUE_FALSE: 	return "create table true_false_entity (deleted char(1) not null comment 'Soft-delete indicator' check (deleted in ('F','T')), id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case BOOLEAN: 		return "create table boolean_entity (deleted bit not null comment 'Soft-delete indicator', id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case NUMERIC: 		return "create table numeric_entity (deleted integer not null comment 'Soft-delete indicator' check (deleted in (0,1)), id integer not null, name varchar(255), primary key (id)) engine=InnoDB";
					case MAPPED_COLUMN: return "create table MappedColumnEntity (id integer not null, is_deleted bit, name varchar(255), primary key (id)) engine=InnoDB";
				}
				break;
			case DB2:
			case POSTGRESQL:
				switch( entityType ) {
					case YES_NO: 		return "create table yes_no_entity (deleted char(1) not null check (deleted in ('N','Y')), id integer not null, name varchar(255), primary key (id))";
					case NO_YES: 		return "create table reversed_yes_no_entity (active char(1) not null check (active in ('Y','N')), id integer not null, name varchar(255), primary key (id))";
					case TRUE_FALSE: 	return "create table true_false_entity (deleted char(1) not null check (deleted in ('F','T')), id integer not null, name varchar(255), primary key (id))";
					case BOOLEAN: 		return "create table boolean_entity (deleted boolean not null, id integer not null, name varchar(255), primary key (id))";
					case NUMERIC: 		return "create table numeric_entity (deleted integer not null check (deleted in (0,1)), id integer not null, name varchar(255), primary key (id))";
					case MAPPED_COLUMN: return "create table MappedColumnEntity (id integer not null, is_deleted boolean, name varchar(255), primary key (id))";
				}
				break;

			case COCKROACHDB:
				switch( entityType ) {
					case YES_NO: 		return "create table yes_no_entity (deleted char(1) not null check (deleted in ('N','Y')), id int4 not null, name varchar(255), primary key (id))";
					case NO_YES: 		return "create table reversed_yes_no_entity (active char(1) not null check (active in ('Y','N')), id int4 not null, name varchar(255), primary key (id))";
					case TRUE_FALSE: 	return "create table true_false_entity (deleted char(1) not null check (deleted in ('F','T')), id int4 not null, name varchar(255), primary key (id))";
					case BOOLEAN: 		return "create table boolean_entity (deleted boolean not null, id int4 not null, name varchar(255), primary key (id))";
					case NUMERIC: 		return "create table numeric_entity (deleted int4 not null check (deleted in (0,1)), id int4 not null, name varchar(255), primary key (id))";
					case MAPPED_COLUMN: return "create table MappedColumnEntity (id int4 not null, is_deleted boolean, name varchar(255), primary key (id))";
				}

			default:
			return "";
		}
		return "";
	}

	@Entity(name="BooleanEntity")
	@Table(name="boolean_entity")
	@SoftDelete()
	public static class BooleanEntity {
		@Id
		private Integer id;
		private String name;

		public BooleanEntity() {
		}

		public BooleanEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity(name="NumericEntity")
	@Table(name="numeric_entity")
	@SoftDelete(converter = NumericBooleanConverter.class)
	public static class NumericEntity {
		@Id
		private Integer id;
		private String name;

		public NumericEntity() {
		}

		public NumericEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity(name="TrueFalseEntity")
	@Table(name="true_false_entity")
	@SoftDelete(converter = TrueFalseConverter.class)
	public static class TrueFalseEntity {
		@Id
		private Integer id;
		private String name;

		public TrueFalseEntity() {
		}

		public TrueFalseEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity(name="YesNoEntity")
	@Table(name="yes_no_entity")
	@SoftDelete(converter = YesNoConverter.class)
	public static class YesNoEntity {
		@Id
		private Integer id;
		private String name;

		public YesNoEntity() {
		}

		public YesNoEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity(name="ReversedYesNoEntity")
	@Table(name="reversed_yes_no_entity")
	@SoftDelete(converter = YesNoConverter.class, strategy = SoftDeleteType.ACTIVE)
	public static class ReversedYesNoEntity {
		@Id
		private Integer id;
		private String name;

		public ReversedYesNoEntity() {
		}

		public ReversedYesNoEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity( name = "MappedColumnEntity" )
	@SoftDelete( columnName = "is_deleted" )
	public static class MappedColumnEntity {
		@Id
		private Integer id;

		private String name;

		// Note that this column's name is the same as the @SoftDelete columnName.
		// ORM Treats them different as long as the additional value parameters are used
		@Column( name = "is_deleted", insertable = false, updatable = false )
		private boolean deleted;

		public MappedColumnEntity() {
		}

		public MappedColumnEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}
}
