//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/try_operator_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

//! This binder is used for the TRY expression
class TryOperatorBinder : public ExpressionBinder {
	friend class SelectBinder;

public:
	TryOperatorBinder(Binder &binder, ClientContext &context);

	bool TryResolveAliasReference(ColumnRefExpression &colref, idx_t depth, bool root_expression, BindResult &result,
	                              unique_ptr<ParsedExpression> &expr_ptr) override {
		if (!stored_binder) {
			return false;
		}
		return stored_binder->TryResolveAliasReference(colref, depth, root_expression, result, expr_ptr);
	}

	bool DoesColumnAliasExist(const ColumnRefExpression &colref) override {
		if (!stored_binder) {
			return false;
		}
		return stored_binder->DoesColumnAliasExist(colref);
	}

protected:
	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth) override;
};

} // namespace duckdb
