from typing import List, Optional, Any, Union, Literal
from datetime import datetime
import warnings
from easy_sdc.engine.base_engine import SCDEngine

class DuckDBSCDEngine(SCDEngine):
    """Engine COMPLETA para DuckDB."""
    
    def __init__(self, connection=None):
        try:
            import duckdb
            self.conn = connection or duckdb.connect()
        except ImportError:
            raise ImportError("DuckDB não está instalado. Execute: pip install scd-lib[duckdb]")
    
    def process_scd_type1(self, current_data: Union[str, Any], new_data: Union[str, Any],
                         business_keys: List[str], track_columns: List[str]) -> str:
        """
        SCD Tipo 1 para DuckDB.
        
        Algoritmo SQL:
        1. CTE unchanged: registros só em current_data
        2. CTE updated: registros em ambos com valores de new_data
        3. CTE inserted: registros só em new_data
        4. UNION ALL das 3 CTEs
        """
        
        # Registrar DataFrames se necessário
        current_table = self._register_data(current_data, 'current_table')
        new_table = self._register_data(new_data, 'new_table')
        
        # Construir condições de join
        join_conditions = " AND ".join([f"c.{key} = n.{key}" for key in business_keys])
        
        # Construir lista de colunas para update
        update_columns = []
        for col in track_columns:
            update_columns.append(f"n.{col}")
        
        # Construir lista de colunas para preservar (non-track)
        current_cols = self._get_table_columns(current_table)
        preserve_columns = [col for col in current_cols if col not in track_columns]
        
        preserve_select = ", ".join([f"c.{col}" for col in preserve_columns])
        update_select = ", ".join(update_columns)
        
        # Query SCD Tipo 1
        scd_query = f"""
        WITH 
        -- Registros inalterados (só existem em current)
        unchanged AS (
            SELECT c.*
            FROM {current_table} c
            LEFT JOIN {new_table} n ON {join_conditions}
            WHERE n.{business_keys[0]} IS NULL
        ),
        -- Registros atualizados (existem em ambos)
        updated AS (
            SELECT {preserve_select}, {update_select}
            FROM {current_table} c
            INNER JOIN {new_table} n ON {join_conditions}
        ),
        -- Registros novos (só existem em new)
        inserted AS (
            SELECT n.*
            FROM {new_table} n
            LEFT JOIN {current_table} c ON {join_conditions}
            WHERE c.{business_keys[0]} IS NULL
        )
        -- Combinar resultados
        SELECT * FROM unchanged
        UNION ALL
        SELECT * FROM updated  
        UNION ALL
        SELECT * FROM inserted
        ORDER BY {', '.join(business_keys)}
        """
        
        result_table = f'scd_type1_result_{id(self)}'
        self.conn.execute(f"CREATE OR REPLACE TABLE {result_table} AS {scd_query}")
        return result_table
    
    def process_scd_type2(self, current_data: Union[str, Any], new_data: Union[str, Any],
                         business_keys: List[str], track_columns: List[str],
                         effective_date: Optional[datetime] = None) -> str:
        """
        SCD Tipo 2 para DuckDB.
        
        Algoritmo SQL:
        1. CTE current_active: registros ativos
        2. CTE joined_data: join new_data com current_active
        3. CTE changed_records: detecta mudanças com IS DISTINCT FROM
        4. CTE expired_records: marca registros para fechar
        5. CTE updated_current: fecha registros antigos
        6. CTE new_records: cria novos registros com surrogate_key
        7. UNION de updated_current + new_records
        """
        
        if effective_date is None:
            effective_date = datetime.now()
        
        # Registrar dados
        current_table = self._register_data(current_data, 'current_table')
        new_table = self._register_data(new_data, 'new_table')
        
        # Validar campos SCD
        current_cols = self._get_table_columns(current_table)
        required_fields = {'surrogate_key', 'valid_from', 'valid_to', 'is_current'}
        if not required_fields.issubset(set(current_cols)):
            missing = required_fields - set(current_cols)
            raise ValueError(f"SCD Tipo 2 requer campos: {missing}")
        
        # Construir condições
        join_conditions = " AND ".join([f"n.{key} = c.{key}" for key in business_keys])
        
        change_conditions = []
        for col in track_columns:
            change_conditions.append(f"n.{col} IS DISTINCT FROM c.{col}")
        change_conditions.append(f"c.{business_keys[0]} IS NULL")  # Registro novo
        
        where_clause = " OR ".join(change_conditions)
        
        # Query SCD Tipo 2
        scd_query = f"""
        WITH current_active AS (
            SELECT * FROM {current_table} WHERE is_current = true
        ),
        joined_data AS (
            SELECT n.*, c.surrogate_key as old_surrogate_key
            FROM {new_table} n
            LEFT JOIN current_active c ON {join_conditions}
        ),
        changed_records AS (
            SELECT * FROM joined_data WHERE {where_clause}
        ),
        expired_records AS (
            SELECT c.*, '{effective_date}'::timestamp as new_valid_to, false as new_is_current
            FROM {current_table} c
            INNER JOIN changed_records ch ON c.surrogate_key = ch.old_surrogate_key
        ),
        updated_current AS (
            SELECT 
                c.*,
                COALESCE(e.new_valid_to, c.valid_to) as valid_to,
                COALESCE(e.new_is_current, c.is_current) as is_current
            FROM {current_table} c
            LEFT JOIN expired_records e ON c.surrogate_key = e.surrogate_key
        ),
        max_surrogate AS (
            SELECT COALESCE(MAX(surrogate_key), 0) as max_key FROM updated_current
        ),
        new_records AS (
            SELECT 
                ch.*,
                ROW_NUMBER() OVER (ORDER BY {business_keys[0]}) + m.max_key as surrogate_key,
                '{effective_date}'::timestamp as valid_from,
                NULL::timestamp as valid_to,
                true as is_current
            FROM changed_records ch
            CROSS JOIN max_surrogate m
        )
        SELECT * FROM updated_current
        UNION ALL
        SELECT * FROM new_records
        ORDER BY surrogate_key
        """
        
        result_table = f'scd_type2_result_{id(self)}'
        self.conn.execute(f"CREATE OR REPLACE TABLE {result_table} AS {scd_query}")
        return result_table
    
    def process_scd_type3(self, current_data: Union[str, Any], new_data: Union[str, Any],
                         business_keys: List[str], track_columns: List[str],
                         max_versions: int = 3,
                         effective_date: Optional[datetime] = None) -> str:
        """
        SCD Tipo 3 para DuckDB.
        
        Algoritmo SQL:
        1. Detecta se estrutura de versões já existe
        2. Cria colunas versionadas se necessário
        3. JOIN current_data com new_data
        4. CASE WHEN para fazer shift das versões quando há mudança
        5. Atualiza datas correspondentes
        """
        
        if effective_date is None:
            effective_date = datetime.now()
        
        if max_versions < 2:
            raise ValueError("max_versions deve ser pelo menos 2")
        
        # Registrar dados
        current_table = self._register_data(current_data, 'current_table')
        new_table = self._register_data(new_data, 'new_table')
        
        # Construir query baseada em max_versions
        join_conditions = " AND ".join([f"c.{key} = n.{key}" for key in business_keys])
        
        # Construir SELECTs para cada track_column
        select_clauses = []
        
        # Business keys primeiro
        for key in business_keys:
            select_clauses.append(f"COALESCE(c.{key}, n.{key}) as {key}")
        
        # Colunas não-track
        current_cols = self._get_table_columns(current_table)
        non_track_cols = [col for col in current_cols if col not in business_keys + track_columns]
        
        # Filtrar colunas de versão existentes
        non_version_cols = []
        for col in non_track_cols:
            is_version_col = False
            for track_col in track_columns:
                if col.startswith(f"{track_col}_v") or col.startswith(f"{track_col}_current") or col.startswith(f"{track_col}_previous"):
                    is_version_col = True
                    break
            if not is_version_col:
                non_version_cols.append(col)
        
        for col in non_version_cols:
            select_clauses.append(f"c.{col}")
        
        # Processar track_columns com versões
        for col in track_columns:
            if max_versions == 2:
                # Formato previous/current
                select_clauses.extend(self._build_type3_case_2versions(col, effective_date))
            else:
                # Formato v1/v2/.../current
                select_clauses.extend(self._build_type3_case_nversions(col, max_versions, effective_date))
        
        select_clause = ",\n    ".join(select_clauses)
        
        # Query final
        scd_query = f"""
        WITH joined AS (
            SELECT 
                {select_clause}
            FROM {current_table} c
            FULL OUTER JOIN {new_table} n ON {join_conditions}
        )
        SELECT * FROM joined
        ORDER BY {', '.join(business_keys)}
        """
        
        result_table = f'scd_type3_result_{id(self)}'
        self.conn.execute(f"CREATE OR REPLACE TABLE {result_table} AS {scd_query}")
        return result_table
    
    def _build_type3_case_2versions(self, col: str, effective_date: datetime) -> List[str]:
        """Constrói CASE WHEN para 2 versões (previous/current)."""
        return [
            f"""CASE 
                WHEN n.{col} IS NOT NULL AND n.{col} IS DISTINCT FROM c.{col}_current 
                THEN c.{col}_current 
                ELSE c.{col}_previous 
            END as {col}_previous""",
            
            f"""CASE 
                WHEN n.{col} IS NOT NULL AND n.{col} IS DISTINCT FROM c.{col}_current 
                THEN n.{col}
                WHEN n.{col} IS NOT NULL AND c.{col}_current IS NULL
                THEN n.{col}
                ELSE c.{col}_current 
            END as {col}_current""",
            
            f"""CASE 
                WHEN n.{col} IS NOT NULL AND n.{col} IS DISTINCT FROM c.{col}_current 
                THEN c.{col}_current_date 
                ELSE c.{col}_previous_date 
            END as {col}_previous_date""",
            
            f"""CASE 
                WHEN n.{col} IS NOT NULL AND (n.{col} IS DISTINCT FROM c.{col}_current OR c.{col}_current IS NULL)
                THEN '{effective_date}'::timestamp
                ELSE c.{col}_current_date 
            END as {col}_current_date"""
        ]
    
    def _build_type3_case_nversions(self, col: str, max_versions: int, effective_date: datetime) -> List[str]:
        """Constrói CASE WHEN para N versões (v1/v2/.../current)."""
        clauses = []
        
        # v1 até v(n-1): shift left quando há mudança
        for i in range(1, max_versions):
            if i == 1:
                # v1 = v2 quando há mudança
                next_col = f"{col}_v2" if max_versions > 2 else f"{col}_current"
                clauses.append(f"""CASE 
                    WHEN n.{col} IS NOT NULL AND n.{col} IS DISTINCT FROM c.{col}_current 
                    THEN c.{next_col}
                    ELSE c.{col}_v{i} 
                END as {col}_v{i}""")
                
                clauses.append(f"""CASE 
                    WHEN n.{col} IS NOT NULL AND n.{col} IS DISTINCT FROM c.{col}_current 
                    THEN c.{next_col}_date
                    ELSE c.{col}_v{i}_date 
                END as {col}_v{i}_date""")
            else:
                # v(i) = v(i+1) quando há mudança
                if i < max_versions - 1:
                    next_col = f"{col}_v{i+1}"
                    next_date = f"{col}_v{i+1}_date"
                else:
                    next_col = f"{col}_current"
                    next_date = f"{col}_current_date"
                
                clauses.append(f"""CASE 
                    WHEN n.{col} IS NOT NULL AND n.{col} IS DISTINCT FROM c.{col}_current 
                    THEN c.{next_col}
                    ELSE c.{col}_v{i} 
                END as {col}_v{i}""")
                
                clauses.append(f"""CASE 
                    WHEN n.{col} IS NOT NULL AND n.{col} IS DISTINCT FROM c.{col}_current 
                    THEN c.{next_date}
                    ELSE c.{col}_v{i}_date 
                END as {col}_v{i}_date""")
        
        # current: novo valor quando há mudança
        clauses.append(f"""CASE 
            WHEN n.{col} IS NOT NULL AND n.{col} IS DISTINCT FROM c.{col}_current 
            THEN n.{col}
            WHEN n.{col} IS NOT NULL AND c.{col}_current IS NULL
            THEN n.{col}
            ELSE c.{col}_current 
        END as {col}_current""")
        
        clauses.append(f"""CASE 
            WHEN n.{col} IS NOT NULL AND (n.{col} IS DISTINCT FROM c.{col}_current OR c.{col}_current IS NULL)
            THEN '{effective_date}'::timestamp
            ELSE c.{col}_current_date 
        END as {col}_current_date""")
        
        return clauses
    
    def _register_data(self, data: Union[str, Any], table_name: str) -> str:
        """Registra dados como tabela DuckDB."""
        if isinstance(data, str):
            return data  # Já é nome de tabela
        elif hasattr(data, 'to_df') or hasattr(data, 'columns'):
            self.conn.register(table_name, data)
            return table_name
        else:
            raise ValueError(f"Tipo de dados não suportado: {type(data)}")
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """Obtém colunas de uma tabela."""
        result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
        return [row[0] for row in result]

# ===== CLASSE PRINCIPAL =====

class SCDProcessor:
    """Processador principal para SCD com algoritmos COMPLETOS."""
    
    def __init__(self, engine_type: str = 'pandas', **engine_kwargs):
        self.engine = self._create_engine(engine_type, **engine_kwargs)
    
    def _create_engine(self, engine_type: str, **kwargs) -> SCDEngine:
        engine_type = engine_type.lower()
        
        if engine_type == 'pandas':
            return PandasSCDEngine()
        elif engine_type == 'pyspark':
            return PySparkSCDEngine(kwargs.get('spark_session'))
        elif engine_type == 'duckdb':
            return DuckDBSCDEngine(kwargs.get('connection'))
        else:
            raise ValueError(f"Engine não suportada: {engine_type}")
    
    def process(self, current_data: Any, new_data: Any,
               business_keys: List[str], track_columns: List[str],
               scd_type: SCDType = "type_2",
               max_versions: int = 3,
               effective_date: Optional[datetime] = None) -> Any:
        """Processa SCD com algoritmos COMPLETOS implementados."""
        
        if scd_type == "type_1":
            return self.engine.process_scd_type1(current_data, new_data, business_keys, track_columns)
        elif scd_type == "type_2":
            return self.engine.process_scd_type2(current_data, new_data, business_keys, track_columns, effective_date)
        elif scd_type == "type_3":
            return self.engine.process_scd_type3(current_data, new_data, business_keys, track_columns, max_versions, effective_date)
        else:
            raise ValueError(f"Tipo SCD não suportado: {scd_type}")

# ===== FUNÇÕES HELPER =====

def process_scd_pandas(current_df: pd.DataFrame, new_df: pd.DataFrame,
                      business_keys: List[str], track_columns: List[str],
                      scd_type: SCDType = "type_2",
                      max_versions: int = 3,
                      effective_date: Optional[datetime] = None) -> pd.DataFrame:
    """Função principal com algoritmos COMPLETOS implementados."""
    processor = SCDProcessor('pandas')
    return processor.process(current_df, new_df, business_keys, track_columns, scd_type, max_versions, effective_date)

def process_scd_pyspark(current_df, new_df, business_keys: List[str], 
                       track_columns: List[str], spark_session=None,
                       scd_type: SCDType = "type_2",
                       max_versions: int = 3,
                       effective_date: Optional[datetime] = None):
    """Função PySpark com algoritmos COMPLETOS implementados."""
    processor = SCDProcessor('pyspark', spark_session=spark_session)
    return processor.process(current_df, new_df, business_keys, track_columns, scd_type, max_versions, effective_date)

def process_scd_duckdb(current_data, new_data, business_keys: List[str],
                      track_columns: List[str], connection=None,
                      scd_type: SCDType = "type_2",
                      max_versions: int = 3,
                      effective_date: Optional[datetime] = None):
    """Função DuckDB com algoritmos COMPLETOS implementados."""
    processor = SCDProcessor('duckdb', connection=connection)
    return processor.process(current_data, new_data, business_keys, track_columns, scd_type, max_versions, effective_date)

# Funções específicas por tipo
def process_scd_type1(current_df: pd.DataFrame, new_df: pd.DataFrame,
                     business_keys: List[str], track_columns: List[str]) -> pd.DataFrame:
    """SCD Tipo 1 com algoritmo COMPLETO."""
    return process_scd_pandas(current_df, new_df, business_keys, track_columns, "type_1")

def process_scd_type2(current_df: pd.DataFrame, new_df: pd.DataFrame,
                     business_keys: List[str], track_columns: List[str],
                     effective_date: Optional[datetime] = None) -> pd.DataFrame:
    """SCD Tipo 2 com algoritmo COMPLETO."""
    return process_scd_pandas(current_df, new_df, business_keys, track_columns, "type_2", effective_date=effective_date)

def process_scd_type3(current_df: pd.DataFrame, new_df: pd.DataFrame,
                     business_keys: List[str], track_columns: List[str],
                     max_versions: int = 3,
                     effective_date: Optional[datetime] = None) -> pd.DataFrame:
    """SCD Tipo 3 com algoritmo COMPLETO."""
    return process_scd_pandas(current_df, new_df, business_keys, track_columns, "type_3", max_versions, effective_date)