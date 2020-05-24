package org.hibernate.reactive.loader;

import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.OuterJoinLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.transform.ResultTransformer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Reactive version of {@link OuterJoinLoader}
 * <p>
 *     This class follows the same structure of {@link OuterJoinLoader} with the methods signature change so that
 *     it's possible to return a {@link CompletionStage}
 * </p>
 */
public class ReactiveOuterJoinLoader extends OuterJoinLoader implements ReactiveLoader {

	public ReactiveOuterJoinLoader(SessionFactoryImplementor factory, LoadQueryInfluencers loadQueryInfluencers) {
		super(factory, loadQueryInfluencers);
	}

	public CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies) {
		return doReactiveQueryAndInitializeNonLazyCollections( getSQLString(), session, queryParameters, returnProxies, null );
	}

	@Override @SuppressWarnings("unchecked")
	public List<Object> processResultSet(ResultSet rs,
										 QueryParameters queryParameters,
										 SharedSessionContractImplementor session,
										 boolean returnProxies,
										 ResultTransformer forcedResultTransformer,
										 int maxRows, List<AfterLoadAction> afterLoadActions) throws SQLException {
		return super.processResultSet(rs, queryParameters, session, returnProxies,
				forcedResultTransformer, maxRows, afterLoadActions);
	}

	// Unnecessary override, I believe
	@Override
	public String preprocessSQL(String sql,
								QueryParameters queryParameters,
								SessionFactoryImplementor factory,
								List<AfterLoadAction> afterLoadActions) {
		return super.preprocessSQL(sql, queryParameters, factory, afterLoadActions);
	}


}
