/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

public class SQLServerParameters extends Parameters {

	public static final SQLServerParameters INSTANCE = new SQLServerParameters();

	private SQLServerParameters() {
		super( "@P" );
	}
}
