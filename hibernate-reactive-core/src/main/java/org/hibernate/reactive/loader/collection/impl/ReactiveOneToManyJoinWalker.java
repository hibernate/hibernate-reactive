package org.hibernate.reactive.loader.collection.impl;

import java.util.function.Supplier;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.collection.OneToManyJoinWalker;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.reactive.sql.impl.Parameters;
import org.hibernate.sql.ConditionFragment;
import org.hibernate.sql.DisjunctionFragment;
import org.hibernate.sql.InFragment;

import static org.hibernate.reactive.sql.impl.Parameters.createDialectParameterGenerator;

public class ReactiveOneToManyJoinWalker extends OneToManyJoinWalker {
	public ReactiveOneToManyJoinWalker(QueryableCollection oneToManyPersister, int batchSize, String subquery, SessionFactoryImplementor factory, LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		super(oneToManyPersister, batchSize, subquery, factory, loadQueryInfluencers);
	}

	/**
	 * Render the where condition for a (batch) load by identifier / collection key
	 */
	protected StringBuilder whereString(String alias, String[] columnNames, int batchSize) {
		Supplier<String> nextParameter = createDialectParameterGenerator( getDialect() );

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
			//a composite key
			ConditionFragment byId = new ConditionFragment()
					.setTableAlias( alias )
					.setCondition( columnNames, nextParameter.get() );

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
