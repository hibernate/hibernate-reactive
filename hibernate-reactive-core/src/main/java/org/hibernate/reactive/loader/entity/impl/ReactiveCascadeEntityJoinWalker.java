package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.CascadingAction;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.entity.CascadeEntityJoinWalker;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.sql.impl.Parameters;
import org.hibernate.sql.ConditionFragment;
import org.hibernate.sql.DisjunctionFragment;
import org.hibernate.sql.InFragment;

import java.util.function.Supplier;

/**
 * A {@link org.hibernate.loader.entity.EntityJoinWalker} that generates
 * SQL with the database-native bind variable syntax.
 */
public class ReactiveCascadeEntityJoinWalker extends CascadeEntityJoinWalker {

	public ReactiveCascadeEntityJoinWalker(OuterJoinLoadable persister, CascadingAction action, SessionFactoryImplementor factory)
			throws MappingException {
		super(persister, action, factory);
	}

	@Override
	protected StringBuilder whereString(String alias, String[] columnNames, int batchSize) {

		Supplier<String> nextParameter = Parameters.createDialectParameterGenerator(getFactory());

		if ( columnNames.length == 1 ) {
			// if not a composite key, use "foo in (?, ?, ?)" for batching
			// if no batch, and not a composite key, use "foo = ?"
			InFragment in = new InFragment().setColumn( alias, columnNames[0] );
			for ( int i = 0; i < batchSize; i++ ) {
				in.addValue( nextParameter.get() );
			}
			return new StringBuilder( in.toFragmentString() );
		}
		else {
			String[] rhs = new String[columnNames.length];
			for ( int i = 0; i < columnNames.length; i++ ) {
				rhs[i] = nextParameter.get();
			}
			//a composite key
			ConditionFragment byId = new ConditionFragment()
					.setTableAlias( alias )
					.setCondition( columnNames, rhs );

			StringBuilder whereString = new StringBuilder();
			if ( batchSize == 1 ) {
				// if no batch, use "foo = ? and bar = ?"
				whereString.append( byId.toFragmentString() );
			}
			else {
				// if a composite key, use "( (foo = ? and bar = ?) or (foo = ? and bar = ?) )" for batching
				whereString.append( '(' ); //TODO: unnecessary for databases with ANSI-style joins
				DisjunctionFragment df = new DisjunctionFragment();
				for ( int i = 0; i < batchSize; i++ ) {
					df.addCondition( byId );
				}
				whereString.append( df.toFragmentString() );
				whereString.append( ')' ); //TODO: unnecessary for databases with ANSI-style joins
			}
			return whereString;
		}
	}

}
