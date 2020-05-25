package org.hibernate.reactive.session.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.query.criteria.internal.CriteriaBuilderImpl;
import org.hibernate.query.criteria.internal.CriteriaQueryImpl;
import org.hibernate.query.criteria.internal.QueryStructure;
import org.hibernate.query.criteria.internal.compile.ImplicitParameterBinding;
import org.hibernate.query.criteria.internal.compile.RenderingContext;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveSession;

import javax.persistence.TypedQuery;
import java.lang.reflect.Field;

/**
 * A reactific {@link CriteriaQueryImpl}, providing the implementation
 * of {@link javax.persistence.criteria.CriteriaQuery}.
 *
 * @author Gavin King
 */
public class ReactiveCriteriaQueryImpl<T> extends CriteriaQueryImpl<T> implements Criteria<T> {

	//TODO: expose this field in ORM!
	private static Field queryStructureField;
	static {
		try {
			queryStructureField = CriteriaQueryImpl.class.getDeclaredField("queryStructure");
			queryStructureField.setAccessible(true);
		}
		catch (Exception e) {
			throw new AssertionFailure("missing field", e);
		}
	}

	private final QueryStructure<T> queryStructure;

	@SuppressWarnings("unchecked")
	public ReactiveCriteriaQueryImpl(CriteriaBuilderImpl criteriaBuilder, Class<T> returnType) {
		super(criteriaBuilder, returnType);
		try {
			this.queryStructure = (QueryStructure<T>) queryStructureField.get(this);
		}
		catch (Exception e) {
			throw new AssertionFailure("missing field", e);
		}
	}

	private String renderQuery(RenderingContext renderingContext) {
		StringBuilder jpaql = new StringBuilder();
		queryStructure.render( jpaql, renderingContext );
		renderOrderByClause( renderingContext, jpaql );
		return jpaql.toString();
	}

	public ReactiveQuery<T> build(CriteriaQueryRenderingContext context, ReactiveSession session) {

		ReactiveQuery<T> query = session.createReactiveQuery(
				renderQuery( context ),
				getResultType(),
				getSelection(),
				new CriteriaQueryOptions(
						queryStructure.getSelection(),
						context.implicitParameterBindings()
				)
		);

		for ( ImplicitParameterBinding implicitParameterBinding: context.implicitParameterBindings() ) {
			implicitParameterBinding.bind( (TypedQuery<?>) query );
		}

//		return new CriteriaQueryTypeQueryAdapter(
//				session,
//				jpaqlQuery,
//				parameterMetadata.explicitParameterInfoMap()
//		);

		return query;
	}


}
