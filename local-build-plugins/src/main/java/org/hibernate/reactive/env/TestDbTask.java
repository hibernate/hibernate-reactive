package org.hibernate.reactive.env;

import java.util.HashMap;
import java.util.Map;

import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestResult;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

	@Input
	public Map<String, String> getCustomSystemProperties() {
		final Map<String, String> props = new HashMap<>();
		props.put("db", dbName);
		props.put("docker", dockerEnabled ? "true" : "false");
		return props;
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

		addTestListener( new TestListener() {

			@Override
			public void beforeSuite(TestDescriptor suite) {
				/* Do nothing */
			}

			// Add afterSuite hook
			@Override
			public void afterSuite(TestDescriptor suite, TestResult result) {
				logSummary( suite, result );
			}

			@Override
			public void beforeTest(TestDescriptor testDescriptor) {
				/* Do nothing */
			}

			@Override
			public void afterTest(TestDescriptor testDescriptor, TestResult result) {
				/* Do nothing */
			}
		} );
	}

	@Override
	public void executeTests() {
		// Apply system properties before running
		getCustomSystemProperties().forEach(this::systemProperty);
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
