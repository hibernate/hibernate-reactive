/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.hibernate.reactive.containers.DatabaseConfiguration;

import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;

/**
 * Define which db is enabled for the tests in a class.
 * <p>
 *     Examples of use:
 *
 * {@code
 *     @Rule
 *     public DatabaseSelectionRule rule = DatabaseSelectionRule.skipTestsFor( DBType.POSTGRESQL );
 * }
 *
 * {@code
 *     @Rule
 *     public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( DBType.POSTGRESQL );
 * }
 * </p>
 *
 * @see DBType
 */
public class DatabaseSelectionRule implements TestRule {

    /**
     * The selected db for running the tests
     */
    private final DBType selectedDb = DatabaseConfiguration.dbType();

    /**
     * Skip the tests for these dbs
     */
    private final DBType[] skippable;
    private final String description;

    private DatabaseSelectionRule(DBType[] skippable, String description) {
        this.skippable = skippable;
        this.description = description;
    }

    /**
     * Create a rule that will skip the tests for the selected dbs
     *
     * @param dbTypes the dbs we want to skip
     *
     * @return an instance of {@link DatabaseSelectionRule}
     */
    public static DatabaseSelectionRule skipTestsFor(DBType... dbTypes) {
        return new DatabaseSelectionRule( dbTypes, "Skip tests for " + Arrays.toString( dbTypes ) );
    }

    /**
     * Create a rule that will run the tests only for the selected dbs
     *
     * @param dbTypes the dbs we want to use for running the tests
     *
     * @return an instance of {@link DatabaseSelectionRule}
     */
    public static DatabaseSelectionRule runOnlyFor(DBType... dbTypes) {
        DBType[] skippable = Arrays.stream( DBType.values() )
                .filter( dbType -> !Arrays.asList( dbTypes ).contains( dbType ) )
                .collect( Collectors.toList() )
                .toArray( new DBType[dbTypes.length] );
        return new DatabaseSelectionRule( skippable, "Run only for " + Arrays.toString( dbTypes ) );
    }

    @Override
    public Statement apply(Statement base, Description description) {
        if ( isSkippable( selectedDb ) ) {
            return new SkipDB( selectedDb, description );
        }
        return base;
    }

    private boolean isSkippable(DBType dbType) {
        for ( DBType db : skippable ) {
            if ( db == selectedDb ) {
                return true;
            }
        }
        return false;
    }

    private static class SkipDB extends Statement {
        private final DBType db;
        private Description description;

        private SkipDB(DBType db, Description description) {
            this.db = db;
            this.description = description;
        }

        @Override
        public void evaluate() {
            Assume.assumeTrue( "Skipping test for " + db + ", rule: " + description, false );
        }
    }
}
