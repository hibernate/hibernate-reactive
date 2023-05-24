/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;

/**
 * Define which db is enabled for the tests in a class.
 * <p>
 * Examples of use:
 * <p>
 * {@code
 *
 * @RegisterExtension public DBSelectionExtension dbSelection = DBSelectionExtension.skipTestsFor( DBType.POSTGRESQL );
 * }
 * <p>
 * {@code
 * @RegisterExtension public DBSelectionExtension dbSelection = DBSelectionExtension.runOnlyFor( DBType.POSTGRESQL );
 * }
 * </p>
 * @see DBType
 */
public class DBSelectionExtension implements ExecutionCondition {

	/**
	 * The selected db for running the tests
	 */
	private final DBType selectedDb = dbType();

	/**
	 * Skip the tests for these dbs
	 */
	private final DBType[] skippable;
	private final String description;

	private DBSelectionExtension(DBType[] skippable, String description) {
		this.skippable = skippable;
		this.description = description;
	}

	/**
	 * Create an extension that will skip the tests for the selected dbs
	 *
	 * @param dbTypes the dbs we want to skip
	 *
	 * @return an instance of {@link DBSelectionExtension}
	 */
	public static DBSelectionExtension skipTestsFor(DBType... dbTypes) {
		return new DBSelectionExtension( dbTypes, "Skip tests for " + Arrays.toString( dbTypes ) );
	}

	/**
	 * Create an extension that will run the tests only for the selected dbs
	 *
	 * @param dbTypes the dbs we want to use for running the tests
	 *
	 * @return an instance of {@link DBSelectionExtension}
	 */
	public static DBSelectionExtension runOnlyFor(DBType... dbTypes) {
		DBType[] skippable = Arrays.stream( DBType.values() )
				.filter( dbType -> !Arrays.asList( dbTypes ).contains( dbType ) )
				.collect( Collectors.toList() )
				.toArray( new DBType[dbTypes.length] );
		return new DBSelectionExtension( skippable, "Run only for " + Arrays.toString( dbTypes ) );
	}

	private boolean isSkippable(DBType dbType) {
		for ( DBType db : dbType ) {
			if ( db == dbType ) {
				return true;
			}
		}
		return false;
	}

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		return isSkippable( selectedDb )
				? disabled( "Test is not applicable for " + dbType().toString() )
				: enabled( "" );
	}
}
