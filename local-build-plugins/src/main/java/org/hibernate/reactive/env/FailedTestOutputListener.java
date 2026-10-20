package org.hibernate.reactive.env;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.api.tasks.testing.TestOutputListener;
import org.gradle.api.tasks.testing.TestResult;

import org.jetbrains.annotations.NotNull;

/**
 * Buffers the standard output and error of each test and logs it only if the test fails.
 * <p>
 * Output produced outside a test (for example, in a {@code @BeforeAll} method)
 * belongs to the enclosing suite and is logged only if that suite fails.
 */
public class FailedTestOutputListener implements TestListener, TestOutputListener {

	private final Logger logger;
	private final Map<String, StringBuilder> outputs = new ConcurrentHashMap<>();

	public FailedTestOutputListener(final @NotNull Logger logger) {
		this.logger = logger;
	}

	@Override
	public void onOutput(TestDescriptor descriptor, TestOutputEvent event) {
		outputs.computeIfAbsent( key( descriptor ), k -> new StringBuilder() )
				.append( event.getMessage() );
	}

	@Override
	public void beforeSuite(TestDescriptor suite) {
		/* Do nothing */
	}

	@Override
	public void afterSuite(TestDescriptor suite, TestResult result) {
		logIfFailed( suite, result );
	}

	@Override
	public void beforeTest(TestDescriptor testDescriptor) {
		/* Do nothing */
	}

	@Override
	public void afterTest(TestDescriptor testDescriptor, TestResult result) {
		logIfFailed( testDescriptor, result );
	}

	private void logIfFailed(TestDescriptor descriptor, TestResult result) {
		final StringBuilder output = outputs.remove( key( descriptor ) );
		if ( output != null && result.getResultType() == TestResult.ResultType.FAILURE ) {
			logger.lifecycle( "\n{} OUTPUT:\n{}", path( descriptor ), output.toString().stripTrailing() );
		}
	}

	/**
	 * The display names from the outermost suite to the descriptor,
	 * using the same format as Gradle (e.g. {@code Gradle Test Executor 1 > MyTest > myMethod()}).
	 */
	private static String path(TestDescriptor descriptor) {
		final StringBuilder path = new StringBuilder( descriptor.getDisplayName() );
		// The root descriptor ("Gradle Test Run ...") isn't shown
		for ( TestDescriptor parent = descriptor.getParent();
				parent != null && parent.getParent() != null;
				parent = parent.getParent() ) {
			path.insert( 0, parent.getDisplayName() + " > " );
		}
		return path.toString();
	}

	private static String key(TestDescriptor descriptor) {
		return descriptor.getClassName() + "#" + descriptor.getName();
	}
}
