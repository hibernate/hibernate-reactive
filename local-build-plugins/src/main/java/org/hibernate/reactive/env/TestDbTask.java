package org.hibernate.reactive.env;

import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestResult;

import groovy.lang.Closure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@CacheableTask
public abstract class TestDbTask extends Test {

	@NotNull
	private String dbName = "PostgreSQL";
	private boolean dockerEnabled = false;
	@Nullable
	private String includeTests = null;
	private boolean showStandardStreams = false;

	@Input
	@Optional
	@NotNull
	public String getDbName() {
		return dbName;
	}

	public void setDbName(final @NotNull String dbName) {
		this.dbName = dbName;
	}

	@Input
	public boolean isDockerEnabled() {
		return dockerEnabled;
	}

	public void setDockerEnabled(final boolean dockerEnabled) {
		this.dockerEnabled = dockerEnabled;
	}

	@Input
	@Optional
	@Nullable
	public String getIncludeTests() {
		return includeTests;
	}

	public void setIncludeTests(final @Nullable String includeTests) {
		this.includeTests = includeTests;
	}

	@Input
	public boolean isShowStandardStreams() {
		return showStandardStreams;
	}

	public void setShowStandardStreams(boolean showStandardStreams) {
		this.showStandardStreams = showStandardStreams;
	}

	public TestDbTask() {
		// Default logging configuration
		setDefaultCharacterEncoding( "UTF-8" );
		useJUnitPlatform();
		final var testLogging = getTestLogging();
		testLogging.setShowStandardStreams( showStandardStreams );
		testLogging.setShowStackTraces( true );
		testLogging.setExceptionFormat( "full" );
		testLogging.setDisplayGranularity( 1 );
		testLogging.events( "PASSED", "FAILED", "SKIPPED" );

		// enforcing Hibernate internal state
		systemProperty( "org.hibernate.reactive.common.InternalStateAssertions.ENFORCE", "true" );

		// Add afterSuite hook
		afterSuite( new Closure<Object>( this, this ) {
			public Object doCall(TestDescriptor desc, TestResult result) {
				logSummary( desc, result );
				return null;
			}
		} );
	}

	@Override
	public void executeTests() {
		// Apply system properties before running
		systemProperty( "db", dbName );
		systemProperty( "docker", dockerEnabled ? "true" : "false" );
		getTestLogging().setShowStandardStreams( showStandardStreams );

		if ( includeTests != null && !includeTests.isEmpty() ) {
			getFilter().includeTestsMatching( includeTests );
		}

		super.executeTests();
	}

	/**
	 * Print a summary of the results of the tests (number of failures, successes and skipped)
	 *
	 * @param desc the test descriptor
	 * @param result the test result
	 */
	private void logSummary(final @NotNull TestDescriptor desc, final @NotNull TestResult result) {
		if ( desc.getParent() == null ) { // outermost suite
			final var output = String.format(
					"%s results: %s (%d tests, %d passed, %d failed, %d skipped)",
					dbName,
					result.getResultType(),
					result.getTestCount(),
					result.getSuccessfulTestCount(),
					result.getFailedTestCount(),
					result.getSkippedTestCount()
			);
			final var line = "-".repeat( output.length() + 1 );
			getLogger().lifecycle( "\n{}\n{}\n{}", line, output, line );
		}
	}
}
