/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

public class OracleParameters extends Parameters {

	public static final OracleParameters INSTANCE = new OracleParameters();

	private OracleParameters() {
		super( ":" );
	}
}
