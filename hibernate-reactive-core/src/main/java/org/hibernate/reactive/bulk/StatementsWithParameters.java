/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bulk;

import org.hibernate.param.ParameterSpecification;

/**
 * @author Gavin King
 */
public interface StatementsWithParameters {
    String[] getStatements();
    ParameterSpecification[][] getParameterSpecifications();
}
