"""
Schema Utilities – IntelliSense-friendly Column & Type Descriptors
==================================================================

Turn any PySpark DataFrame schema into a purpose-built class whose
attributes are the column names (as strings) with full type metadata.

Three complementary workflows
------------------------------
1. **Runtime class** – ``schema_class(df, "Name")`` returns a class with
   attributes you can dot-access in notebooks (tab-completion works).
2. **Column helpers** – each attribute also exposes a ``.col`` property
   that returns ``pyspark.sql.Column`` for direct use in expressions.
3. **Source-code generator** – ``generate_schema_source(df, "Name")``
   prints / returns copy-pasteable Python so you get *static* IntelliSense
   in .py files and IDEs that don't introspect runtime objects.

Usage
-----
>>> cols = schema_class(df, "CustomerCols")
>>> df.select(cols.CUSTOMER_ID, cols.NAME)         # strings
>>> df.filter(cols.NAME.col == "Alice")             # Column object
>>> print(cols)                                      # pretty summary
>>> generate_schema_source(df, "CustomerCols")       # paste into .py
"""

from __future__ import annotations

import keyword
import re
import textwrap
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Type

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimestampNTZType,
)


# ─── Spark → Python type mapping ────────────────────────────────────────

_SPARK_TO_PYTHON: Dict[type, str] = {
    StringType: "str",
    IntegerType: "int",
    LongType: "int",
    ShortType: "int",
    ByteType: "int",
    FloatType: "float",
    DoubleType: "float",
    BooleanType: "bool",
    DateType: "datetime.date",
    TimestampType: "datetime.datetime",
    TimestampNTZType: "datetime.datetime",
    BinaryType: "bytes",
    NullType: "None",
}


def _spark_type_str(dt: DataType) -> str:
    """Human-readable string for a Spark DataType, including nested types."""
    if isinstance(dt, DecimalType):
        return f"Decimal({dt.precision},{dt.scale})"
    if isinstance(dt, ArrayType):
        return f"Array<{_spark_type_str(dt.elementType)}>"
    if isinstance(dt, MapType):
        return f"Map<{_spark_type_str(dt.keyType)},{_spark_type_str(dt.valueType)}>"
    if isinstance(dt, StructType):
        inner = ", ".join(f"{f.name}: {_spark_type_str(f.dataType)}" for f in dt.fields)
        return f"Struct<{inner}>"
    return type(dt).__name__.replace("Type", "")


def _python_type_hint(dt: DataType) -> str:
    """Best-effort Python type hint for a Spark DataType."""
    for spark_cls, py_hint in _SPARK_TO_PYTHON.items():
        if isinstance(dt, spark_cls):
            return py_hint
    if isinstance(dt, DecimalType):
        return "decimal.Decimal"
    if isinstance(dt, ArrayType):
        return f"list[{_python_type_hint(dt.elementType)}]"
    if isinstance(dt, MapType):
        return f"dict[{_python_type_hint(dt.keyType)}, {_python_type_hint(dt.valueType)}]"
    if isinstance(dt, StructType):
        return "dict"
    return "Any"


def _safe_attr_name(name: str) -> str:
    """Convert a column name to a valid, uppercase Python attribute name."""
    # Replace non-alphanumeric characters with underscores
    attr = re.sub(r"[^A-Za-z0-9_]", "_", name).upper()
    # Ensure it doesn't start with a digit
    if attr and attr[0].isdigit():
        attr = f"_{attr}"
    # Avoid Python keywords
    if keyword.iskeyword(attr.lower()):
        attr = f"{attr}_"
    return attr


# ─── ColumnDescriptor: the per-column object ────────────────────────────


class ColumnDescriptor(str):
    """
    A **str subclass** that wraps a column name with Spark type metadata.

    Because it *is* a ``str``, it works natively everywhere PySpark
    expects a column-name string — ``df.select()``, ``df.join()``,
    ``df[col]``, ``df.groupBy()``, etc. — with zero friction.

    Extra attributes expose type information and a ``.col`` shortcut.
    """

    _spark_type: DataType
    _python_type: str
    _nullable: bool

    # Use __new__ because str is immutable — must set value at creation
    def __new__(
        cls,
        name: str,
        spark_type: DataType,
        nullable: bool = True,
    ) -> "ColumnDescriptor":
        instance = super().__new__(cls, name)
        instance._spark_type = spark_type  # type: ignore[attr-defined]
        instance._python_type = _python_type_hint(spark_type)  # type: ignore[attr-defined]
        instance._nullable = nullable  # type: ignore[attr-defined]
        return instance

    # ── rich repr (str behaviour is inherited) ───────────────────────
    def __repr__(self) -> str:
        nullable_str = ", nullable" if self._nullable else ", non-null"
        return (
            f"ColumnDescriptor('{str(self)}', "
            f"spark={_spark_type_str(self._spark_type)}, "
            f"python={self._python_type}{nullable_str})"
        )

    # ── properties ───────────────────────────────────────────────────
    @property
    def name(self) -> str:
        """The original DataFrame column name."""
        return str(self)

    @property
    def spark_type(self) -> DataType:
        """The PySpark DataType."""
        return self._spark_type

    @property
    def spark_type_name(self) -> str:
        """Human-readable Spark type string."""
        return _spark_type_str(self._spark_type)

    @property
    def python_type(self) -> str:
        """Approximate Python type hint."""
        return self._python_type

    @property
    def nullable(self) -> bool:
        """Whether the column is nullable."""
        return self._nullable

    @property
    def col(self):
        """Return a ``pyspark.sql.Column`` for use in expressions."""
        return F.col(str(self))


# ─── Factory: build a schema class from a DataFrame ─────────────────────


def schema_class(df: DataFrame, class_name: str = "Cols") -> type:
    """
    Create a class whose attributes are ``ColumnDescriptor`` objects
    for each column in *df*.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Source DataFrame whose schema to capture.
    class_name : str
        Name for the generated class (used in repr / source output).

    Returns
    -------
    type
        A new class with uppercase attributes for every column.

    Example
    -------
    >>> C = schema_class(customers_df, "CustomerCols")
    >>> C.CUSTOMER_ID          # ColumnDescriptor → acts as str "customer_id"
    >>> C.CUSTOMER_ID.col      # pyspark.sql.Column
    >>> C.CUSTOMER_ID.spark_type_name   # e.g. "String"
    >>> C.columns()            # ["customer_id", "name", ...]
    >>> C.dtypes()             # {"customer_id": "String", ...}
    """

    attrs: Dict[str, object] = {}
    _fields_ordered: List[ColumnDescriptor] = []

    for struct_field in df.schema.fields:
        desc = ColumnDescriptor(
            name=struct_field.name,
            spark_type=struct_field.dataType,
            nullable=struct_field.nullable,
        )
        attr_name = _safe_attr_name(struct_field.name)
        attrs[attr_name] = desc
        _fields_ordered.append(desc)

    # ── Class-level helpers ──────────────────────────────────────────

    @classmethod  # type: ignore[misc]
    def columns(cls) -> List[str]:
        """Return all column names in schema order."""
        return [d.name for d in cls._fields_ordered]

    @classmethod  # type: ignore[misc]
    def dtypes(cls) -> Dict[str, str]:
        """Return {column_name: spark_type_string} mapping."""
        return {d.name: d.spark_type_name for d in cls._fields_ordered}

    @classmethod  # type: ignore[misc]
    def descriptors(cls) -> List[ColumnDescriptor]:
        """Return all ColumnDescriptors in schema order."""
        return list(cls._fields_ordered)

    @classmethod  # type: ignore[misc]
    def select_cols(cls, *attr_names: str):
        """
        Return a list of column-name strings for use in ``df.select()``.

        >>> df.select(*C.select_cols("CUSTOMER_ID", "NAME"))
        """
        result = []
        for a in attr_names:
            desc = getattr(cls, a, None)
            if desc is None:
                raise AttributeError(f"{cls.__name__} has no attribute '{a}'")
            result.append(desc.name)
        return result

    @classmethod  # type: ignore[misc]
    def to_source(cls) -> str:
        """Return copy-pasteable Python source for static IntelliSense."""
        return _render_source(cls.__name__, cls._fields_ordered)

    attrs["columns"] = columns
    attrs["dtypes"] = dtypes
    attrs["descriptors"] = descriptors
    attrs["select_cols"] = select_cols
    attrs["to_source"] = to_source
    attrs["_fields_ordered"] = _fields_ordered

    # Pretty __repr__
    def __class_repr__(cls) -> str:
        lines = [f"<{cls.__name__}  ({len(cls._fields_ordered)} columns)>"]
        for d in cls._fields_ordered:
            nullable_flag = "?" if d.nullable else "!"
            lines.append(f"  .{_safe_attr_name(d.name):<30s} → {d.name:<30s}  {d.spark_type_name}{nullable_flag}")
        return "\n".join(lines)

    # Build the class
    namespace = {**attrs, "__repr__": __class_repr__}
    klass = type(class_name, (), namespace)

    # Override __repr__ at the metaclass level so print(Cls) works
    # We use a simple trick: override __str__ on the class itself
    klass.__str__ = __class_repr__  # type: ignore[assignment]

    return klass


# ─── Source-code generator ───────────────────────────────────────────────


def _render_source(class_name: str, descriptors: List[ColumnDescriptor]) -> str:
    """Render a class definition as Python source code."""
    lines = [
        f"class {class_name}:",
        f'    """Auto-generated column constants. {len(descriptors)} columns."""',
        "",
    ]
    for d in descriptors:
        attr = _safe_attr_name(d.name)
        nullable_str = "nullable" if d.nullable else "non-null"
        lines.append(
            f'    {attr}: str = "{d.name}"'
            f"  # Spark: {d.spark_type_name} | Python: {d.python_type} | {nullable_str}"
        )
    return "\n".join(lines)


def generate_schema_source(
    df: DataFrame,
    class_name: str = "Cols",
    *,
    print_output: bool = True,
) -> str:
    """
    Generate copy-pasteable Python source for a column-constants class.

    Parameters
    ----------
    df : DataFrame
        Source DataFrame.
    class_name : str
        Name for the class.
    print_output : bool
        If True (default), also ``print()`` the source code.

    Returns
    -------
    str
        The generated source code.
    """
    descriptors = [
        ColumnDescriptor(
            name=f.name,
            spark_type=f.dataType,
            nullable=f.nullable,
        )
        for f in df.schema.fields
    ]
    source = _render_source(class_name, descriptors)
    if print_output:
        print(source)
    return source
