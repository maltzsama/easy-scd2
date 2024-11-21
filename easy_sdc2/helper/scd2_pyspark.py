from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    row_number,
    lag,
    when,
    monotonically_increasing_id,
    expr,
)
from typing import Set, Optional
from functools import reduce


def generate_comparison_conditions(
    source_columns: list, columns_to_ignore: Set[str]
) -> col:
    conditions = [
        col(f"source.{column}") != col(f"current.{column}")
        for column in source_columns
        if column not in columns_to_ignore
    ]
    return reduce(lambda x, y: x | y, conditions)


def apply_scd_type2(
    source: DataFrame,
    target: DataFrame,
    pk: str,
    non_versioned_fields: Set[str] = None,
    control_column: Optional[str] = "update_date",
    start_column: Optional[str] = "valid_from",
    end_column: Optional[str] = "valid_to",
    flag_column: Optional[str] = "is_current",
    surrogate_key_name: Optional[str] = "surrogate_key",
    surrogate_key_strategy: Optional[str] = "uuid",
    start_period: Optional[str] = "1900-01-01 00:00:00",
    future_timestamp: Optional[str] = "3000-01-01 00:00:00",
) -> DataFrame:

    def create_surrogate_key(df: DataFrame) -> DataFrame:
        if surrogate_key_strategy == "concat":
            surrogate_key_column = (
                col(pk)
                .cast("string")
                .concat(lit("_"))
                .concat(col(start_column).cast("string"))
            )
        elif surrogate_key_strategy == "uuid":
            surrogate_key_column = expr("uuid()")
        elif surrogate_key_strategy == "monotonic":
            surrogate_key_column = monotonically_increasing_id()
        else:
            raise ValueError(
                f"Invalid surrogate_key_strategy: {surrogate_key_strategy}\n"
                f"Try: 'concat' | 'uuid' | 'monotonic'"
            )
        return df.withColumn(surrogate_key_name, surrogate_key_column)

    non_versioned_columns = non_versioned_fields or set()
    start_period = lit(start_period).cast("timestamp")
    future_timestamp = lit(future_timestamp).cast("timestamp")

    combined_condition = generate_comparison_conditions(
        source.columns, non_versioned_columns
    )

    new_data = create_surrogate_key(
        source.withColumn(start_column, start_period).withColumn(
            end_column, future_timestamp
        )
    )

    joined_data = new_data.alias("source").join(
        target.alias("current").filter(col(end_column) == future_timestamp),
        pk,
        "left_outer",
    )

    rec_changed_df = joined_data.filter(combined_condition).select(
        "source.*", lit(True).alias(flag_column)
    )

    rec_hist_df = (
        joined_data.filter(combined_condition)
        .withColumn(
            f"current.{end_column}", col(f"source.{control_column}").cast("timestamp")
        )
        .select("current.*")
    )

    rec_new_df = (
        new_data.alias("source")
        .join(target.alias("current"), [pk], "left_anti")
        .select("source.*")
        .withColumn(start_column, start_period)
        .withColumn(end_column, future_timestamp)
        .withColumn(flag_column, lit(True))
    )

    rec_unchanged_df = (
        target.alias("source")
        .join(
            new_data.filter(col(end_column) == future_timestamp).alias("current"),
            on=pk,
            how="left",
        )
        .filter(~combined_condition)
        .select("current.*")
    )

    print(source.show(truncate=False))
    print(target.show(truncate=False))

    print(rec_changed_df.printSchema())
    print(rec_hist_df.printSchema())
    print(rec_new_df.printSchema())
    print(rec_unchanged_df.printSchema())

    print(rec_changed_df.show(truncate=False))
    print(rec_hist_df.show(truncate=False))
    print(rec_new_df.show(truncate=False))
    print(rec_unchanged_df.show(truncate=False))

    union_all_df = (
        rec_changed_df.unionByName(rec_hist_df)
        .unionByName(rec_new_df)
        .unionByName(rec_unchanged_df)
    )

    window_spec_desc = Window.partitionBy(pk).orderBy(col(control_column).desc())

    middle_df = (
        union_all_df.withColumn("row_number", row_number().over(window_spec_desc))
        .withColumn(
            "previous_update_date", lag(col(control_column), 1).over(window_spec_desc)
        )
        .withColumn(
            end_column,
            when(col("row_number") > 1, col("previous_update_date")).otherwise(
                col(end_column)
            ),
        )
        .drop("row_number", "previous_update_date")
    )

    window_spec_asc = Window.partitionBy(pk).orderBy(col(control_column))

    final_df = (
        middle_df.withColumn("row_number", row_number().over(window_spec_asc))
        .withColumn(
            start_column,
            when(col("row_number") == 1, start_period).otherwise(col(control_column)),
        )
        .drop("row_number")
    )

    return final_df
