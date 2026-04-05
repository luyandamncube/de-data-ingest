"""PySpark-backed adapter for running real benchmark workloads."""

from __future__ import annotations

import inspect
import os
from pathlib import Path
import resource
import time

from benchmarks.adapters.base import BenchmarkExecution, EngineAdapter
from benchmarks.implementation_resolver import ResolvedImplementation
from pipeline.registry import TrackingUnit


class PySparkDeltaAdapter(EngineAdapter):
    """Adapter that can execute workloads against a local PySpark session."""

    def __init__(self) -> None:
        self.name = "pyspark_delta"

    def is_available(self) -> bool:
        try:
            import pyspark  # noqa: F401
        except ImportError:
            return False
        return True

    def supported_families(self) -> tuple[str, ...]:
        return ("docker_smoke",)

    def run(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
    ) -> BenchmarkExecution:
        if implementation.kind not in {"python", "sql"}:
            raise NotImplementedError(
                f"unsupported implementation kind for pyspark_delta: {implementation.kind}"
            )

        data_root = Path("/benchmarks/data")
        needs_spark = implementation.kind == "sql" or self._python_requires_spark(
            implementation
        )
        spark = self._build_spark_session(workload) if needs_spark else None
        try:
            if implementation.kind == "sql":
                assert spark is not None
                execution = self._run_sql_workload(
                    workload,
                    dataset_profile,
                    implementation,
                    attempt=attempt,
                    spark=spark,
                    data_root=data_root,
                )
            else:
                execution = self._run_python_workload(
                    workload,
                    dataset_profile,
                    implementation,
                    attempt=attempt,
                    spark=spark,
                    data_root=data_root,
                )
            execution.notes = (
                f"{execution.notes}; adapter=pyspark_delta; impl={implementation.ref}"
            )
            execution.artifacts.setdefault("spark_enabled", spark is not None)
            return execution
        finally:
            if spark is not None:
                spark.stop()

    def _run_python_workload(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
        spark,
        data_root: Path,
    ) -> BenchmarkExecution:
        if implementation.callable_obj is None:
            raise NotImplementedError(
                "python-backed implementation has no callable object"
            )

        workload_impl = implementation.callable_obj
        signature = inspect.signature(workload_impl)
        accepts_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )

        context: dict[str, object] = {
            "engine_name": self.name,
            "workload": workload,
            "dataset_profile": dataset_profile,
            "attempt": attempt,
            "data_root": data_root,
            "spark": spark,
        }
        start = time.perf_counter()
        kwargs = {
            key: value
            for key, value in context.items()
            if accepts_kwargs or key in signature.parameters
        }
        execution = workload_impl(**kwargs)
        elapsed_seconds = time.perf_counter() - start
        if execution.elapsed_seconds <= 0:
            execution.elapsed_seconds = elapsed_seconds
        return execution

    def _python_requires_spark(self, implementation: ResolvedImplementation) -> bool:
        if implementation.kind != "python" or implementation.callable_obj is None:
            return False
        signature = inspect.signature(implementation.callable_obj)
        return "spark" in signature.parameters

    def _run_sql_workload(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
        spark,
        data_root: Path,
    ) -> BenchmarkExecution:
        if implementation.sql_path is None:
            raise NotImplementedError("sql-backed implementation has no sql_path")

        sql_text = Path(implementation.sql_path).read_text(encoding="utf-8")
        rows_in = self._register_sql_inputs(workload, spark, data_root)

        cpu_start = self._cpu_user_seconds()
        start = time.perf_counter()
        result_df = spark.sql(sql_text)
        output_rows = result_df.collect()
        elapsed_seconds = time.perf_counter() - start
        cpu_user_seconds = self._cpu_user_seconds() - cpu_start
        cpu_pct = (
            (cpu_user_seconds / elapsed_seconds) * 100.0
            if elapsed_seconds > 0
            else None
        )

        preview = [row.asDict(recursive=True) for row in output_rows[:3]]

        return BenchmarkExecution(
            workload_id=workload.id,
            engine=self.name,
            elapsed_seconds=elapsed_seconds,
            peak_memory_mb=self._peak_memory_mb(),
            rows_in=rows_in,
            rows_out=len(output_rows),
            cpu_user_seconds=cpu_user_seconds,
            cpu_pct=cpu_pct,
            delta_write_seconds=None,
            delta_read_seconds=None,
            tmp_peak_mb=None,
            output_file_count=0,
            delta_roundtrip_ok=False,
            notes=(
                "sql-backed PySpark workload"
                f"; dataset_profile={dataset_profile}"
                f"; attempt={attempt}"
            ),
            artifacts={
                "sql_path": implementation.sql_path,
                "preview": preview,
                "registered_tables": ["customers"],
            },
        )

    def _register_sql_inputs(self, workload: TrackingUnit, spark, data_root: Path) -> int:
        if workload.id != "BM_07":
            raise NotImplementedError(
                f"sql input registration is not configured for workload {workload.id}"
            )

        customers = self._read_customers(spark, data_root)
        customers.createOrReplaceTempView("customers")
        return customers.count()

    def _read_customers(self, spark, data_root: Path):
        from pyspark.sql import types as T

        customers_path = data_root / "customers.csv"
        if not customers_path.exists():
            raise FileNotFoundError(f"customers dataset not found: {customers_path}")

        schema = T.StructType(
            [
                T.StructField("customer_id", T.StringType(), False),
                T.StructField("id_number", T.StringType(), True),
                T.StructField("first_name", T.StringType(), True),
                T.StructField("last_name", T.StringType(), True),
                T.StructField("dob", T.StringType(), True),
                T.StructField("gender", T.StringType(), True),
                T.StructField("province", T.StringType(), True),
                T.StructField("income_band", T.StringType(), True),
                T.StructField("segment", T.StringType(), True),
                T.StructField("risk_score", T.IntegerType(), True),
                T.StructField("kyc_status", T.StringType(), True),
                T.StructField("product_flags", T.StringType(), True),
            ]
        )

        return (
            spark.read.option("header", True)
            .schema(schema)
            .csv(str(customers_path))
        )

    def _build_spark_session(self, workload: TrackingUnit):
        import pyspark
        from pyspark.sql import SparkSession

        spark_home = Path(pyspark.__file__).resolve().parent
        spark_bin = str(spark_home / "bin")
        os.environ["SPARK_HOME"] = str(spark_home)
        os.environ["PYSPARK_PYTHON"] = "python3"
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
        current_path = os.environ.get("PATH", "")
        if spark_bin not in current_path.split(":"):
            os.environ["PATH"] = f"{spark_bin}:{current_path}" if current_path else spark_bin

        return (
            SparkSession.builder.appName(f"benchmark-{workload.id}")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.local.dir", "/tmp/spark-local")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

    def _peak_memory_mb(self) -> float:
        self_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        children_usage = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        return max(self_usage, children_usage) / 1024.0

    def _cpu_user_seconds(self) -> float:
        self_usage = resource.getrusage(resource.RUSAGE_SELF)
        children_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        return (
            self_usage.ru_utime
            + self_usage.ru_stime
            + children_usage.ru_utime
            + children_usage.ru_stime
        )
