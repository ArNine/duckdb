#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include <algorithm>
#include <vector>

namespace duckdb {

namespace {

struct MaxIntersectionsState {
	std::vector<std::pair<int64_t, int64_t>> intervals;
	
	// Constructor for proper initialization
	MaxIntersectionsState() {
		intervals.reserve(64); // Reserve space for better performance
	}
	
	// Move constructor for efficient state transfers
	MaxIntersectionsState(MaxIntersectionsState &&other) noexcept 
		: intervals(std::move(other.intervals)) {}
	
	// Move assignment operator
	MaxIntersectionsState& operator=(MaxIntersectionsState &&other) noexcept {
		if (this != &other) {
			intervals = std::move(other.intervals);
		}
		return *this;
	}
	
	// Disable copy operations to prevent unnecessary copying
	MaxIntersectionsState(const MaxIntersectionsState&) = delete;
	MaxIntersectionsState& operator=(const MaxIntersectionsState&) = delete;
};

struct MaxIntersectionsFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) MaxIntersectionsState();
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~MaxIntersectionsState();
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &left, const B_TYPE &right, AggregateBinaryInput &input) {
		// Validate interval and add only if valid (left <= right)
		if (left <= right) {
			state.intervals.emplace_back(static_cast<int64_t>(left), static_cast<int64_t>(right));
		}
		// Invalid intervals (left > right) are silently ignored, following DuckDB conventions
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const A_TYPE &left, const B_TYPE &right, 
	                             AggregateBinaryInput &input, idx_t count) {
		// For constant operations, add the interval once per count if valid
		if (left <= right) {
			auto interval = std::make_pair(static_cast<int64_t>(left), static_cast<int64_t>(right));
			state.intervals.reserve(state.intervals.size() + count);
			for (idx_t i = 0; i < count; i++) {
				state.intervals.emplace_back(interval);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		// Efficiently combine intervals from source into target
		if (source.intervals.empty()) {
			return;
		}
		
		// Reserve space to avoid multiple reallocations
		target.intervals.reserve(target.intervals.size() + source.intervals.size());
		
		// Use move semantics when possible, otherwise copy
		target.intervals.insert(target.intervals.end(), 
		                       source.intervals.begin(), 
		                       source.intervals.end());
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.intervals.empty()) {
			target = 0;
			return;
		}

		// Optimized algorithm to find maximum number of overlapping intervals
		// Using sweep line algorithm with event processing
		
		const auto &intervals = state.intervals;
		const size_t num_intervals = intervals.size();
		
		// Fast path for single interval
		if (num_intervals == 1) {
			target = 1;
			return;
		}
		
		// Create events for interval starts and ends
		// Reserve exact space needed for better performance
		std::vector<std::pair<int64_t, int32_t>> events;
		events.reserve(num_intervals * 2);
		
		for (const auto &interval : intervals) {
			events.emplace_back(interval.first, 1);        // Start of interval: +1
			events.emplace_back(interval.second + 1, -1);  // End of interval: -1 (exclusive end)
		}

		// Sort events by position, with ends processed before starts at the same position
		// This ensures correct handling of touching intervals
		std::sort(events.begin(), events.end(), 
		         [](const std::pair<int64_t, int32_t> &a, const std::pair<int64_t, int32_t> &b) {
			if (a.first == b.first) {
				return a.second < b.second; // Process -1 (end) before +1 (start)
			}
			return a.first < b.first;
		});

		// Sweep through events and track maximum concurrent intervals
		int64_t current_count = 0;
		int64_t max_count = 0;
		
		for (const auto &event : events) {
			current_count += event.second;
			// Update max_count only when current_count increases
			if (current_count > max_count) {
				max_count = current_count;
			}
		}

		target = max_count;
	}

	static bool IgnoreNull() {
		return true;
	}
};

} // namespace

AggregateFunction MaxIntersectionsFun::GetFunction() {
	auto function = AggregateFunction::BinaryAggregate<MaxIntersectionsState, int64_t, int64_t, int64_t, MaxIntersectionsFunction>(
		LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT
	);
	
	// Set function properties for optimal performance
	function.name = "max_intersections";
	function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	
	return function;
}

} // namespace duckdb