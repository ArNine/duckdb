add_library_unity(duckdb_func_date OBJECT strftime.cpp days_in_month.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_date>
    PARENT_SCOPE)
