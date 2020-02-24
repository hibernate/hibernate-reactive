package org.hibernate.rx.collection;

import org.hibernate.HibernateException;
import org.hibernate.collection.internal.PersistentList;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.CollectionAliases;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.rx.util.impl.RxUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A {@link PersistentList} that makes sure that {@link CompletionStage} values complete before adding them
 * to the underlying list;
 */
public class RxPersistentList extends PersistentList {

	public RxPersistentList(SharedSessionContractImplementor session, List delegate ) {
		super( session, delegate);
	}

	@Override
	public Object readFrom(ResultSet rs, CollectionPersister persister, CollectionAliases descriptor, Object owner) throws HibernateException, SQLException {
		// FIXME: Create a method for reactive reading. For example: rxReadElement
		final Object element = persister.readElement( rs, owner, descriptor.getSuffixedElementAliases(), getSession() );
		if ( element instanceof CompletionStage ) {
			CompletionStage<?> elementStage = (CompletionStage<?>) element;
			return elementStage.thenApply( obj ->  {
				if ( obj != null) {
					add( obj );
				}
				return obj;
			} );
		}
		else {
			return super.readFrom( rs, persister, descriptor, owner );
		}
	}
}
