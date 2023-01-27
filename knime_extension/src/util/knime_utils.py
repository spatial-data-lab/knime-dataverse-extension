# import logging
# from typing import Callable
# from typing import List

# import knime.types.geospatial as gt
# import knime_extension as knext


# LOGGER = logging.getLogger(__name__)

# DEFAULT_CRS = "epsg:4326"
# """Default coordinate reference system."""

# DEF_CRS_DESCRIPTION = """Enter the 
#         [Coordinate reference system (CRS)](https://en.wikipedia.org/wiki/Spatial_reference_system) to use.

#         Common [EPSG codes](https://en.wikipedia.org/wiki/EPSG_Geodetic_Parameter_Dataset) that can be universally 
#         used for mapping coordinates everywhere in the world are 
#         [epsg:4326 (WGS 84, Unit: degree)](https://epsg.io/4326) (Latitude/longitude coordinate system based 
#         on the Earth's center of mass;  Used by the Global Positioning System among others) and 
#         [epsg:3857 (Unit: meter)](https://epsg.io/3857) (Web Mercator projection used by many web-based mapping tools,
#         including Google Maps and OpenStreetMap.). 
        
#         There are EPSG codes for specific regions that provide a higher accuracy in these regions, such as 
#         [epsg:4269 (NAD83, Unit: degree)](https://epsg.io/4269) 
#         and [epsg:26918 (NAD83 18N, Unit: meter)](https://epsg.io/26918) for North America, 
#         and [epsg:4490 (CGCS2000, Unit: degree)](https://epsg.io/4490) 
#         and [epsg:4479 (CGCS2000, Unit: meter)](https://epsg.io/4479) for China.
        
#         The input field supports the following input types:
        
#         - An authority string [i.e. 'epsg:4326']
#         - An EPSG integer code [i.e. 4326]
#         - A tuple of ('auth_name': 'auth_code') [i.e ('epsg', '4326')]
#         - [CRS WKT string](https://www.ogc.org/standards/wkt-crs)
#         - [PROJ string](https://proj.org/usage/quickstart.html)
#         - JSON string with [PROJ parameters](https://proj.org/specifications/projjson.html)
#         """


# ############################################
# # Geometry value types
# ############################################

# TYPE_GEO = knext.logical(gt.GeoValue)


# ############################################
# # Column selection helper
# ############################################

# __DEF_GEO_COL_LABEL = "Geometry column"
# __DEF_GEO_COL_DESC = "Select the geometry column to use"




# def is_numeric(column: knext.Column) -> bool:
#     """
#     Checks if column is numeric e.g. int, long or double.
#     @return: True if Column is numeric
#     """
#     return (
#         column.ktype == knext.double()
#         or column.ktype == knext.int32()
#         or column.ktype == knext.int64()
#     )


# def is_string(column: knext.Column) -> bool:
#     """
#     Checks if column is string
#     @return: True if Column is string
#     """
#     return column.ktype == knext.string()


# def is_boolean(column: knext.Column) -> bool:
#     """
#     Checks if column is boolean
#     @return: True if Column is boolean
#     """
#     return column.ktype == knext.boolean()


# def is_numeric_or_string(column: knext.Column) -> bool:
#     """
#     Checks if column is numeric or string
#     @return: True if Column is numeric or string
#     """
#     return (
#         column.ktype == knext.double()
#         or column.ktype == knext.int32()
#         or column.ktype == knext.int64()
#         or column.ktype == knext.string()
#     )


# def is_binary(column: knext.Column) -> bool:
#     """
#     Checks if column is binary
#     @return: True if Column is binary
#     """
#     return column.ktype == knext.blob



# def __is_type_x(column: knext.Column, type: str) -> bool:
#     """
#     Checks if column contains the given type whereas type can be :
#     GeoPointCell, GeoLineCell, GeoPolygonCell, GeoMultiPointCell, GeoMultiLineCell, GeoMultiPolygonCell, ...
#     @return: True if Column Type is a GeoLogical Point
#     """
#     return (
#         isinstance(column.ktype, knext.LogicalType)
#         and type in column.ktype.logical_type
#     )

# ############################################
# # General helper
# ############################################

# __DEF_PLEASE_SELECT_COLUMN = "Please select a column"


# def column_exists_or_preset(
#     context: knext.ConfigurationContext,
#     column: str,
#     schema: knext.Schema,
#     func: Callable[[knext.Column], bool] = None,
#     none_msg: str = "No compatible column found in input table",
# ) -> str:
#     """
#     Checks that the given column is not None and exists in the given schema. If none is selected it returns the
#     first column that is compatible with the provided function. If none is compatible it throws an exception.
#     """
#     if column is None:
#         for c in schema:
#             if func(c):
#                 context.set_warning(f"Preset column to: {c.name}")
#                 return c.name
#         raise knext.InvalidParametersError(none_msg)
#     __check_col_and_type(column, schema, func)
#     return column



# def column_exists(
#     column: str,
#     schema: knext.Schema,
#     func: Callable[[knext.Column], bool] = None,
#     none_msg: str = __DEF_PLEASE_SELECT_COLUMN,
# ) -> None:
#     """
#     Checks that the given column is not None and exists in the given schema otherwise it throws an exception
#     """
#     if column is None:
#         raise knext.InvalidParametersError(none_msg)
#     __check_col_and_type(column, schema, func)


# def __check_col_and_type(
#     column: str,
#     schema: knext.Schema,
#     check_type: Callable[[knext.Column], bool] = None,
# ) -> None:
#     """
#     Checks that the given column exists in the given schema and that it matches the given type_check function.
#     """
#     # Check that the column exists in the schema and that it has a compatible type
#     try:
#         existing_column = schema[column]
#         if check_type is not None and not check_type(existing_column):
#             raise knext.InvalidParametersError(
#                 f"Column '{str(column)}' has incompatible data type"
#             )
#     except IndexError:
#         raise knext.InvalidParametersError(
#             f"Column '{str(column)}' not available in input table"
#         )


# def columns_exist(
#     columns: List[str],
#     schema: knext.Schema,
#     func: Callable[[knext.Column], bool] = lambda c: True,
#     none_msg: str = __DEF_PLEASE_SELECT_COLUMN,
# ) -> None:
#     """
#     Checks that the given columns are not None and exist in the given schema otherwise it throws an exception
#     """
#     for col in columns:
#         column_exists(col, schema, func, none_msg)


# def fail_if_column_exists(
#     column_name: str, input_schema: knext.Schema, msg: str = None
# ):
#     """Checks that the given column name does not exists in the input schema.
#     Can be used to check that a column is not accidentally overwritten."""
#     if column_name in input_schema.column_names:
#         if msg is None:
#             msg = f"Column '{column_name}' exists"
#         raise knext.InvalidParametersError(msg)


# def get_unique_column_name(column_name: str, input_schema: knext.Schema) -> str:
#     """Checks if the column name exists in the given schema and if so appends a number to it to make it unique.
#     The unique name if returned or the original if it was already unique."""
#     if column_name is None:
#         raise knext.InvalidParametersError("Column name must not be None")
#     uniquifier = 1
#     result = column_name
#     while result in input_schema.column_names:
#         result = column_name + f"(#{uniquifier})"
#         uniquifier += 1
#     return result


# def check_canceled(exec_context: knext.ExecutionContext) -> None:
#     """
#     Checks if the user has canceled the execution and if so throws a RuntimeException
#     """
#     if exec_context.is_canceled():
#         raise RuntimeError("Execution canceled")


# def ensure_file_extension(file_name: str, file_extension: str) -> str:
#     """
#     Checks if the given file_name ends with the given file_extension and if not appends it to the returned file_name.
#     """
#     if not file_name:
#         raise knext.InvalidParametersError("Please enter a valid file name")
#     if file_name.lower().endswith(file_extension):
#         return file_name
#     return file_name + file_extension
