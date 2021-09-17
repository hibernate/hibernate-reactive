/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.async.impl;

import java.util.concurrent.CompletionStage;

/**
 * Copy of com.ibm.asyncutil.util.AsyncClosable in com.ibm.async:asyncutil:0.1.0
 * with minor changes to the java doc.
 * <p>
 * An object that may hold resources that must be explicitly released, where the release may be
 * performed asynchronously.
 *
 * <p>
 * Examples of such resources are manually managed memory, open file handles, socket descriptors
 * etc. While similar to {@link AutoCloseable}, this interface should be used when the resource
 * release operation may possibly be async. For example, if an object is thread-safe and has many
 * consumers, an implementation may require all current ongoing operations to complete before
 * resources are relinquished.
 *
 * @author Renar Narubin
 * @author Ravi Khadiwala
 */
@FunctionalInterface
public interface AsyncCloseable {
	/**
	 * Relinquishes any resources associated with this object.
	 *
	 * @return a {@link CompletionStage} that completes when all resources associated with this object
	 * have been released, or with an exception if the resources cannot be released.
	 */
	CompletionStage<Void> close();

}
