#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"

namespace duckdb {

namespace {

static int32_t GetDaysInMonth(int32_t year, int32_t month) {
	return Date::MonthDays(year, month);
}

static int32_t GetDaysInMonthFromDate(date_t date) {
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	return GetDaysInMonth(year, month);
}

static int32_t GetDaysInMonthFromTimestamp(timestamp_t timestamp) {
	date_t date = Timestamp::GetDate(timestamp);
	return GetDaysInMonthFromDate(date);
}

static void DaysInMonthBinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto &year_vector = args.data[0];
	auto &month_vector = args.data[1];

	BinaryExecutor::Execute<int32_t, int32_t, int32_t>(
	    year_vector, month_vector, result, args.size(),
	    [&](int32_t year, int32_t month) { return GetDaysInMonth(year, month); });
}

static void DaysInMonthDateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	auto &date_vector = args.data[0];

	UnaryExecutor::Execute<date_t, int32_t>(date_vector, result, args.size(),
	                                        [&](date_t date) { return GetDaysInMonthFromDate(date); });
}

static void DaysInMonthTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	auto &timestamp_vector = args.data[0];

	UnaryExecutor::Execute<timestamp_t, int32_t>(timestamp_vector, result, args.size(), [&](timestamp_t timestamp) {
		return GetDaysInMonthFromTimestamp(timestamp);
	});
}

static void DaysInMonthTimeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InvalidInputException("days_in_month cannot be used with TIME type - TIME does not contain date information");
}

} // namespace

ScalarFunctionSet DaysInMonthFun::GetFunctions() {
	ScalarFunctionSet set("days_in_month");

	set.AddFunction(
	    ScalarFunction({LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::INTEGER, DaysInMonthBinaryFunction));

	set.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::INTEGER, DaysInMonthDateFunction));

	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::INTEGER, DaysInMonthTimestampFunction));

	set.AddFunction(ScalarFunction({LogicalType::TIME}, LogicalType::INTEGER, DaysInMonthTimeFunction));

	return set;
}

} // namespace duckdb
