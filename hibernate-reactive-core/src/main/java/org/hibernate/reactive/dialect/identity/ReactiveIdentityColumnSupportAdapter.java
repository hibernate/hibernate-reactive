/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.dialect.identity;

import org.hibernate.MappingException;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.id.PostInsertIdentityPersister;
import org.hibernate.id.insert.GetGeneratedKeysDelegate;
import org.hibernate.reactive.id.insert.ReactiveGetGeneratedKeysDelegate;

public class ReactiveIdentityColumnSupportAdapter implements IdentityColumnSupport {

	private final IdentityColumnSupport delegate;

	public ReactiveIdentityColumnSupportAdapter(IdentityColumnSupport delegate) {
		this.delegate = delegate;
	}

	@Override
	public GetGeneratedKeysDelegate buildGetGeneratedKeysDelegate(PostInsertIdentityPersister persister, Dialect dialect) {
		return new ReactiveGetGeneratedKeysDelegate( persister, dialect );
	}

	@Override
	public boolean supportsIdentityColumns() {
		return delegate.supportsIdentityColumns();
	}

	@Override
	public boolean supportsInsertSelectIdentity() {
		return delegate.supportsInsertSelectIdentity();
	}

	@Override
	public boolean hasDataTypeInIdentityColumn() {
		return delegate.hasDataTypeInIdentityColumn();
	}

	@Override
	public String appendIdentitySelectToInsert(String insertString) {
		return delegate.appendIdentitySelectToInsert( insertString );
	}

	@Override
	public String appendIdentitySelectToInsert(String identityColumnName, String insertString) {
		return delegate.appendIdentitySelectToInsert( identityColumnName, insertString );
	}

	@Override
	public String getIdentitySelectString(String table, String column, int type) throws MappingException {
		return delegate.getIdentitySelectString( table, column, type );
	}

	@Override
	public String getIdentityColumnString(int type) throws MappingException {
		return delegate.getIdentityColumnString( type );
	}

	@Override
	public String getIdentityInsertString() {
		return delegate.getIdentityInsertString();
	}

	@Override
	public boolean hasIdentityInsertKeyword() {
		return delegate.hasIdentityInsertKeyword();
	}
}
