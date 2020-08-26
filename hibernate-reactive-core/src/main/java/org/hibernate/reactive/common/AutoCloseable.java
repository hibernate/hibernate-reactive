/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

/**
 * A Java {@link java.lang.AutoCloseable} which can be inspected
 * to determine if it has been closed.
 *
 * @author Gavin King
 */
public interface AutoCloseable extends java.lang.AutoCloseable {

    @Override
    void close();

    boolean isOpen();

}
