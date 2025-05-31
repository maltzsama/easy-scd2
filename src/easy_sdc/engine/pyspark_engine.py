from typing import List, Optional, Any, Union, Literal
from datetime import datetime
import warnings
from easy_sdc.engine.base_engine import SCDEngine

class PySparkSCDEngine(SCDEngine):
    """Engine COMPLETA para PySpark DataFrames."""
    
    def __init__(self, spark_session=None):
        if spark_session is None:
            try:
                from pyspark.sql import SparkSession
                self.spark = SparkSession.getActiveSession()
                if self.spark is None:
                    raise ValueError("Nenhuma sessão Spark ativa. Passe spark_session.")
            except ImportError:
                raise ImportError("PySpark não está instalado. Execute: pip install scd-lib[pyspark]")
        else:
            self.spark = spark_session
    
    def process_scd_type1(self, current_data, new_data, business_keys: List[str], track_columns: List[str]):
        """
        SCD Tipo 1 para PySpark.
        
        Algoritmo:
        1. Identifica registros por business_keys usando joins
        2. Left anti join para encontrar registros unchanged
        3. Inner join para encontrar registros a atualizar
        4. Substitui track_columns nos registros existentes
        5. Union de unchanged + updated + new records
        """
        
        try:
            from pyspark.sql import functions as F
        except ImportError:
            raise ImportError("PySpark não está disponível")
        
        if current_data.count() == 0:
            return new_data
        
        if new_data.count() == 0:
            return current_data
        
        # 1. Registros que só existem em current (manter inalterados)
        unchanged_records = current_data.join(new_data, business_keys, "left_anti")
        
        # 2. Registros que existem em ambos (atualizar)
        # Usar apenas business_keys + outras colunas não-track para preservar estrutura
        non_track_cols = [col for col in current_data.columns if col not in track_columns]
        base_records = current_data.select(*non_track_cols)
        
        # Join com new_data para pegar novos valores de track_columns
        updated_records = base_records.join(new_data, business_keys, "inner")
        
        # 3. Registros que só existem em new (inserir)
        new_records = new_data.join(current_data.select(*business_keys), business_keys, "left_anti")
        
        # 4. Combinar resultados
        result = unchanged_records.union(updated_records).union(new_records)
        
        return result.orderBy(*business_keys)
    
    def process_scd_type2(self, current_data, new_data, business_keys: List[str], 
                         track_columns: List[str], effective_date: Optional[datetime] = None):
        """
        SCD Tipo 2 para PySpark.
        
        Algoritmo:
        1. Valida estrutura SCD (surrogate_key, valid_from, valid_to, is_current)
        2. Join new_data com current records ativos
        3. Detecta mudanças usando coalesce e comparação de colunas
        4. Gera novos surrogate_keys usando window functions
        5. Fecha registros antigos e cria novos registros
        6. Union de historical + new records
        """
        
        try:
            from pyspark.sql import functions as F
            from pyspark.sql.window import Window
        except ImportError:
            raise ImportError("PySpark não está disponível")
        
        if effective_date is None:
            effective_date = datetime.now()
        
        # Validar campos SCD
        required_fields = {'surrogate_key', 'valid_from', 'valid_to', 'is_current'}
        current_cols = set(current_data.columns)
        if not required_fields.issubset(current_cols):
            missing = required_fields - current_cols
            raise ValueError(f"SCD Tipo 2 requer campos: {missing}")
        
        if current_data.count() == 0:
            # Primeiro carregamento
            window = Window.orderBy(F.monotonically_increasing_id())
            result = new_data.withColumn("surrogate_key", F.row_number().over(window)) \
                            .withColumn("valid_from", F.lit(effective_date)) \
                            .withColumn("valid_to", F.lit(None).cast("timestamp")) \
                            .withColumn("is_current", F.lit(True))
            return result
        
        if new_data.count() == 0:
            return current_data
        
        # 1. Join com registros ativos
        current_active = current_data.filter(F.col("is_current") == True)
        
        joined = new_data.join(current_active, business_keys, "left") \
                        .select(
                            *[F.col(f"left.{col}").alias(col) for col in new_data.columns],
                            *[F.col(f"right.{col}").alias(f"{col}_current") for col in current_active.columns]
                        )
        
        # 2. Detectar mudanças
        change_conditions = []
        for col in track_columns:
            col_current = f"{col}_current"
            # Mudança: valor diferente OU registro novo (current é null)
            change_conditions.append(
                (F.col(col) != F.col(col_current)) | F.col(col_current).isNull()
            )
        
        # Combinar condições com OR
        combined_condition = change_conditions[0]
        for condition in change_conditions[1:]:
            combined_condition = combined_condition | condition
        
        changed_records = joined.filter(combined_condition)
        
        if changed_records.count() == 0:
            return current_data
        
        # 3. Fechar registros antigos
        expired_surrogate_keys = changed_records.select("surrogate_key_current") \
                                               .filter(F.col("surrogate_key_current").isNotNull()) \
                                               .distinct()
        
        updated_current = current_data.join(
            expired_surrogate_keys.withColumnRenamed("surrogate_key_current", "expired_key"),
            current_data.surrogate_key == F.col("expired_key"),
            "left"
        ).withColumn(
            "valid_to",
            F.when(F.col("expired_key").isNotNull(), F.lit(effective_date))
             .otherwise(F.col("valid_to"))
        ).withColumn(
            "is_current", 
            F.when(F.col("expired_key").isNotNull(), F.lit(False))
             .otherwise(F.col("is_current"))
        ).drop("expired_key")
        
        # 4. Preparar novos registros
        new_cols = new_data.columns
        new_records = changed_records.select(*new_cols)
        
        # Gerar novos surrogate_keys
        max_key = updated_current.agg(F.max("surrogate_key").alias("max_key")).collect()[0]["max_key"]
        if max_key is None:
            max_key = 0
        
        window = Window.orderBy(F.monotonically_increasing_id())
        new_records = new_records.withColumn(
            "surrogate_key", 
            F.row_number().over(window) + max_key
        ).withColumn(
            "valid_from", F.lit(effective_date)
        ).withColumn(
            "valid_to", F.lit(None).cast("timestamp")
        ).withColumn(
            "is_current", F.lit(True)
        )
        
        # 5. Combinar resultados
        result = updated_current.union(new_records)
        return result.orderBy("surrogate_key")
    
    def process_scd_type3(self, current_data, new_data, business_keys: List[str],
                         track_columns: List[str], max_versions: int = 3,
                         effective_date: Optional[datetime] = None):
        """
        SCD Tipo 3 para PySpark.
        
        Algoritmo:
        1. Cria estrutura de colunas versionadas se necessário
        2. Join current_data com new_data
        3. Para cada track_column, detecta mudanças
        4. Usa when/otherwise para fazer shift das versões
        5. Atualiza datas correspondentes
        """
        
        try:
            from pyspark.sql import functions as F
        except ImportError:
            raise ImportError("PySpark não está disponível")
        
        if effective_date is None:
            effective_date = datetime.now()
        
        if max_versions < 2:
            raise ValueError("max_versions deve ser pelo menos 2")
        
        if current_data.count() == 0:
            # Primeiro carregamento
            result = new_data
            for col in track_columns:
                result = self._initialize_spark_version_columns(result, col, max_versions, effective_date)
            return result
        
        if new_data.count() == 0:
            return current_data
        
        # 1. Garantir estrutura de colunas
        current_with_versions = current_data
        for col in track_columns:
            current_with_versions = self._ensure_spark_version_columns(current_with_versions, col, max_versions)
        
        # 2. Join dados
        joined = current_with_versions.join(new_data, business_keys, "outer") \
                                    .select(
                                        *[F.coalesce(f"left.{col}", f"right.{col}").alias(col) for col in business_keys],
                                        *[F.col(f"left.{col}") for col in current_with_versions.columns if col not in business_keys],
                                        *[F.col(f"right.{col}").alias(f"{col}_new") for col in track_columns]
                                    )
        
        # 3. Processar cada track_column
        result = joined
        for col in track_columns:
            result = self._process_spark_column_versions(result, col, max_versions, effective_date)
        
        # 4. Limpar colunas temporárias
        cols_to_drop = [c for c in result.columns if c.endswith("_new")]
        for col in cols_to_drop:
            result = result.drop(col)
        
        return result.orderBy(*business_keys)
    
    def _initialize_spark_version_columns(self, df, col: str, max_versions: int, effective_date: datetime):
        """Inicializa colunas de versão para Spark."""
        from pyspark.sql import functions as F
        
        if max_versions == 2:
            df = df.withColumn(f"{col}_previous", F.lit(None)) \
                   .withColumn(f"{col}_current", F.col(col)) \
                   .withColumn(f"{col}_previous_date", F.lit(None).cast("timestamp")) \
                   .withColumn(f"{col}_current_date", F.lit(effective_date))
        else:
            for i in range(1, max_versions):
                df = df.withColumn(f"{col}_v{i}", F.lit(None)) \
                       .withColumn(f"{col}_v{i}_date", F.lit(None).cast("timestamp"))
            df = df.withColumn(f"{col}_current", F.col(col)) \
                   .withColumn(f"{col}_current_date", F.lit(effective_date))
        
        return df.drop(col)
    
    def _ensure_spark_version_columns(self, df, col: str, max_versions: int):
        """Garante colunas de versão para Spark."""
        from pyspark.sql import functions as F
        
        # Se coluna original existe mas versões não, migrar
        if col in df.columns and f"{col}_current" not in df.columns:
            df = df.withColumn(f"{col}_current", F.col(col)) \
                   .withColumn(f"{col}_current_date", F.lit(None).cast("timestamp")) \
                   .drop(col)
        
        # Criar colunas que não existem
        if max_versions == 2:
            version_cols = [f"{col}_previous", f"{col}_current"]
            date_cols = [f"{col}_previous_date", f"{col}_current_date"]
        else:
            version_cols = [f"{col}_v{i}" for i in range(1, max_versions)] + [f"{col}_current"]
            date_cols = [f"{col}_v{i}_date" for i in range(1, max_versions)] + [f"{col}_current_date"]
        
        for vcol, dcol in zip(version_cols, date_cols):
            if vcol not in df.columns:
                df = df.withColumn(vcol, F.lit(None))
            if dcol not in df.columns:
                df = df.withColumn(dcol, F.lit(None).cast("timestamp"))
        
        return df
    
    def _process_spark_column_versions(self, df, col: str, max_versions: int, effective_date: datetime):
        """Processa versões de coluna para Spark."""
        from pyspark.sql import functions as F
        
        col_new = f"{col}_new"
        current_col = f"{col}_current"
        
        # Detectar mudança
        changed = (F.col(current_col) != F.col(col_new)) & F.col(col_new).isNotNull()
        
        if max_versions == 2:
            # Shift: previous = current, current = new
            df = df.withColumn(
                f"{col}_previous",
                F.when(changed, F.col(current_col)).otherwise(F.col(f"{col}_previous"))
            ).withColumn(
                f"{col}_current",
                F.when(changed, F.col(col_new))
                 .when(F.col(current_col).isNull() & F.col(col_new).isNotNull(), F.col(col_new))
                 .otherwise(F.col(current_col))
            ).withColumn(
                f"{col}_previous_date",
                F.when(changed, F.col(f"{col}_current_date")).otherwise(F.col(f"{col}_previous_date"))
            ).withColumn(
                f"{col}_current_date",
                F.when(changed | (F.col(current_col).isNull() & F.col(col_new).isNotNull()), 
                       F.lit(effective_date))
                 .otherwise(F.col(f"{col}_current_date"))
            )
        else:
            # Shift múltiplas versões
            # v1 = v2, v2 = v3, ..., current = new
            for i in range(1, max_versions - 1):
                df = df.withColumn(
                    f"{col}_v{i}",
                    F.when(changed, F.col(f"{col}_v{i+1}")).otherwise(F.col(f"{col}_v{i}"))
                ).withColumn(
                    f"{col}_v{i}_date",
                    F.when(changed, F.col(f"{col}_v{i+1}_date")).otherwise(F.col(f"{col}_v{i}_date"))
                )
            
            # Última versão = current
            last_v = max_versions - 1
            df = df.withColumn(
                f"{col}_v{last_v}",
                F.when(changed, F.col(current_col)).otherwise(F.col(f"{col}_v{last_v}"))
            ).withColumn(
                f"{col}_v{last_v}_date",
                F.when(changed, F.col(f"{col}_current_date")).otherwise(F.col(f"{col}_v{last_v}_date"))
            )
            
            # Current = new
            df = df.withColumn(
                f"{col}_current",
                F.when(changed, F.col(col_new))
                 .when(F.col(current_col).isNull() & F.col(col_new).isNotNull(), F.col(col_new))
                 .otherwise(F.col(current_col))
            ).withColumn(
                f"{col}_current_date",
                F.when(changed | (F.col(current_col).isNull() & F.col(col_new).isNotNull()), 
                       F.lit(effective_date))
                 .otherwise(F.col(f"{col}_current_date"))
            )
        
        return df