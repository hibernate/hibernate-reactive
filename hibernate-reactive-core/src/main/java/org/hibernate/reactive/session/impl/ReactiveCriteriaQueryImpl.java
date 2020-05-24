package org.hibernate.reactive.session.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.jpa.spi.HibernateEntityManagerImplementor.QueryOptions;
import org.hibernate.query.criteria.internal.CriteriaBuilderImpl;
import org.hibernate.query.criteria.internal.CriteriaQueryImpl;
import org.hibernate.query.criteria.internal.QueryStructure;
import org.hibernate.query.criteria.internal.SelectionImplementor;
import org.hibernate.query.criteria.internal.ValueHandlerFactory;
import org.hibernate.query.criteria.internal.compile.ImplicitParameterBinding;
import org.hibernate.query.criteria.internal.compile.InterpretedParameterMetadata;
import org.hibernate.query.criteria.internal.compile.RenderingContext;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.type.Type;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.Selection;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A reactific {@link CriteriaQueryImpl}, providing the implementation
 * of {@link javax.persistence.criteria.CriteriaQuery}.
 *
 * @author Gavin King
 */
public class ReactiveCriteriaQueryImpl<T> extends CriteriaQueryImpl<T> {

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
		StringBuilder jpaqlBuffer = new StringBuilder();
		queryStructure.render( jpaqlBuffer, renderingContext );
		renderOrderByClause( renderingContext, jpaqlBuffer );
		return jpaqlBuffer.toString();
	}

	public ReactiveQuery<T> build(RenderingContext renderingContext,
								  ReactiveSession session,
								  InterpretedParameterMetadata parameterMetadata) {

		ReactiveQuery<T> query = session.createReactiveQuery(
				renderQuery( renderingContext ),
				getResultType(),
				getSelection(),
				new ReactiveQueryOptions(
						queryStructure.getSelection(),
						extractTypeMap( parameterMetadata.implicitParameterBindings() )
				)
		);

		for ( ImplicitParameterBinding implicitParameterBinding: parameterMetadata.implicitParameterBindings() ) {
			implicitParameterBinding.bind( (TypedQuery<?>) query );
		}

//		return new CriteriaQueryTypeQueryAdapter(
//				session,
//				jpaqlQuery,
//				parameterMetadata.explicitParameterInfoMap()
//		);

		return query;
	}

	@SuppressWarnings("rawtypes")
	private static Map<String, Class> extractTypeMap(List<ImplicitParameterBinding> implicitParameterBindings) {
		final HashMap<String,Class> map = new HashMap<>();
		for ( ImplicitParameterBinding implicitParameter : implicitParameterBindings ) {
			map.put( implicitParameter.getParameterName(), implicitParameter.getJavaType() );
		}
		return map;
	}

	private static void validate(Type[] returnTypes, SelectionImplementor<?> selection) {
		if (selection != null) {
			if (selection.isCompoundSelection()) {
				if (returnTypes.length != selection.getCompoundSelectionItems().size()) {
					throw new IllegalStateException(
							"Number of return values [" + returnTypes.length +
									"] did not match expected [" +
									selection.getCompoundSelectionItems().size() + "]"
					);
				}
			} else {
				if (returnTypes.length > 1) {
					throw new IllegalStateException(
							"Number of return values [" + returnTypes.length +
									"] did not match expected [1]"
					);
				}
			}
		}
	}

	private static class ReactiveQueryOptions implements QueryOptions {
		private final SelectionImplementor<?> selection;
		private final Map<String, Class> implicitParameterTypes;

		public ReactiveQueryOptions(Selection<?> selection, Map<String, Class> implicitParameterTypes) {
			this.selection = (SelectionImplementor<?>) selection;
			this.implicitParameterTypes = implicitParameterTypes;
		}

		@Override @SuppressWarnings("rawtypes")
		public List<ValueHandlerFactory.ValueHandler> getValueHandlers() {
			return selection == null ? null : selection.getValueHandlers();
		}

		@Override @SuppressWarnings("rawtypes")
		public Map<String, Class> getNamedParameterExplicitTypes() {
			return implicitParameterTypes;
		}

		@Override
		public ResultMetadataValidator getResultMetadataValidator() {
			return returnTypes -> validate( returnTypes, selection);
		}
	}
}
