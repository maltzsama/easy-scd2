from typing import List, Optional, Any, Union, Literal
from datetime import datetime
import warnings
import pandas as pd

from easy_sdc.engine.base_engine import SCDEngine


class PandasSCDEngine(SCDEngine):
    """Engine COMPLETA para Pandas DataFrames."""
    
    def process_scd_type1(self, current_data: pd.DataFrame, new_data: pd.DataFrame,
                         business_keys: List[str], track_columns: List[str]) -> pd.DataFrame:
        """
        SCD Tipo 1: Sobrescreve dados existentes sem manter histórico.
        
        Algoritmo:
        1. Identifica registros por business_keys
        2. Separa em: atualizar, inserir, manter inalterado
        3. Atualiza track_columns dos registros existentes
        4. Adiciona registros completamente novos
        5. Mantém registros que não apareceram em new_data
        """
        
        if current_data.empty:
            return new_data.copy()
        
        if new_data.empty:
            return current_data.copy()
        
        # Função para criar chave composta
        def make_key(df):
            return df[business_keys].apply(lambda row: tuple(row), axis=1)
        
        current_keys = set(make_key(current_data))
        new_keys = set(make_key(new_data))
        
        # Operações necessárias
        keys_to_update = current_keys & new_keys  # Existem em ambos
        keys_to_insert = new_keys - current_keys  # Só em new_data
        keys_unchanged = current_keys - new_keys  # Só em current_data
        
        results = []
        
        # 1. Manter registros que não mudaram
        if keys_unchanged:
            unchanged_mask = make_key(current_data).isin(keys_unchanged)
            unchanged_records = current_data[unchanged_mask].copy()
            results.append(unchanged_records)
        
        # 2. Atualizar registros existentes
        if keys_to_update:
            # Criar mapeamento de new_data por business_keys
            new_data_dict = {}
            for _, row in new_data.iterrows():
                key = tuple(row[business_keys])
                if key in keys_to_update:
                    new_data_dict[key] = row
            
            # Atualizar registros em current_data
            update_mask = make_key(current_data).isin(keys_to_update)
            updated_records = current_data[update_mask].copy()
            
            for idx, row in updated_records.iterrows():
                key = tuple(row[business_keys])
                if key in new_data_dict:
                    new_row = new_data_dict[key]
                    # Atualizar apenas track_columns
                    for col in track_columns:
                        if col in new_row:
                            updated_records.loc[idx, col] = new_row[col]
            
            results.append(updated_records)
        
        # 3. Inserir registros novos
        if keys_to_insert:
            insert_mask = make_key(new_data).isin(keys_to_insert)
            new_records = new_data[insert_mask].copy()
            results.append(new_records)
        
        # Combinar resultados
        if results:
            result = pd.concat(results, ignore_index=True)
            return result.sort_values(business_keys).reset_index(drop=True)
        else:
            return current_data.copy()

    def process_scd_type2(self, current_data: pd.DataFrame, new_data: pd.DataFrame,
                         business_keys: List[str], track_columns: List[str],
                         effective_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        SCD Tipo 2: Mantém histórico completo de mudanças.
        
        Algoritmo:
        1. Valida campos SCD obrigatórios em current_data
        2. Join new_data com registros ativos de current_data
        3. Detecta mudanças comparando track_columns
        4. Fecha registros antigos (valid_to = effective_date, is_current = False)
        5. Cria novos registros com surrogate_key únicos
        6. Combina dados históricos + novos registros
        """
        
        if effective_date is None:
            effective_date = datetime.now()
        
        # Validar campos SCD obrigatórios
        required_scd_fields = {'surrogate_key', 'valid_from', 'valid_to', 'is_current'}
        if not required_scd_fields.issubset(current_data.columns):
            missing_fields = required_scd_fields - set(current_data.columns)
            raise ValueError(
                f"SCD Tipo 2 requer campos SCD: {missing_fields}. "
                f"Use Tipo 1 para dados sem histórico ou adicione os campos necessários."
            )
        
        if current_data.empty:
            # Primeiro carregamento - criar estrutura SCD
            result = new_data.copy()
            result['surrogate_key'] = range(1, len(result) + 1)
            result['valid_from'] = effective_date
            result['valid_to'] = pd.NaT
            result['is_current'] = True
            return result
        
        if new_data.empty:
            return current_data.copy()
        
        # 1. Join new_data com registros ativos
        current_active = current_data[current_data['is_current'] == True].copy()
        
        joined = new_data.merge(
            current_active,
            on=business_keys,
            how='left',
            suffixes=('', '_current')
        )
        
        # 2. Detectar mudanças
        change_mask = pd.Series([False] * len(joined))
        
        for col in track_columns:
            col_current = f"{col}_current"
            if col_current in joined.columns:
                # Mudança: valor diferente OU registro novo (NaN em current)
                col_changed = (joined[col] != joined[col_current]) | joined[col_current].isna()
                change_mask |= col_changed
        
        changed_records = joined[change_mask].copy()
        
        if len(changed_records) == 0:
            # Nenhuma mudança detectada
            return current_data.copy()
        
        # 3. Fechar registros antigos que mudaram
        updated_current = current_data.copy()
        
        if 'surrogate_key_current' in changed_records.columns:
            # Encontrar surrogate_keys que precisam ser fechados
            expired_keys = changed_records['surrogate_key_current'].dropna().unique()
            
            if len(expired_keys) > 0:
                # Fechar registros antigos
                mask = updated_current['surrogate_key'].isin(expired_keys)
                updated_current.loc[mask, 'valid_to'] = effective_date
                updated_current.loc[mask, 'is_current'] = False
        
        # 4. Preparar novos registros
        new_records = changed_records[list(new_data.columns)].copy()
        
        # Gerar surrogate_keys únicos
        max_key = updated_current['surrogate_key'].max() if len(updated_current) > 0 else 0
        new_records['surrogate_key'] = range(max_key + 1, max_key + len(new_records) + 1)
        new_records['valid_from'] = effective_date
        new_records['valid_to'] = pd.NaT
        new_records['is_current'] = True
        
        # 5. Combinar resultados
        result = pd.concat([updated_current, new_records], ignore_index=True)
        return result.sort_values(['surrogate_key']).reset_index(drop=True)

    def process_scd_type3(self, current_data: pd.DataFrame, new_data: pd.DataFrame,
                         business_keys: List[str], track_columns: List[str],
                         max_versions: int = 3,
                         effective_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        SCD Tipo 3: Versões configuráveis em colunas.
        
        Algoritmo:
        1. Valida max_versions >= 2
        2. Garante estrutura de colunas versionadas para cada track_column
        3. Merge current_data com new_data por business_keys
        4. Para cada mudança detectada, faz "shift right" das versões
        5. Adiciona nova versão como _current, versão mais antiga é removida
        6. Atualiza datas de mudança correspondentes
        """
        
        if effective_date is None:
            effective_date = datetime.now()
        
        if max_versions < 2:
            raise ValueError("max_versions deve ser pelo menos 2 (previous + current)")
        
        if current_data.empty:
            # Primeiro carregamento - inicializar estrutura
            result = new_data.copy()
            for col in track_columns:
                self._initialize_version_columns(result, col, max_versions, effective_date)
            return result
        
        if new_data.empty:
            return current_data.copy()
        
        result = current_data.copy()
        
        # 1. Garantir estrutura de colunas versionadas
        for col in track_columns:
            self._ensure_version_columns(result, col, max_versions)
        
        # 2. Merge com new_data
        merged = result.merge(new_data, on=business_keys, how='outer', suffixes=('', '_new'))
        
        # 3. Processar mudanças para cada track_column
        for col in track_columns:
            self._process_column_versions(merged, col, max_versions, effective_date)
        
        # 4. Limpar e retornar
        final_result = merged.dropna(subset=business_keys)
        
        # Remover colunas temporárias _new
        cols_to_drop = [c for c in final_result.columns if c.endswith('_new')]
        if cols_to_drop:
            final_result = final_result.drop(columns=cols_to_drop)
        
        return final_result.sort_values(business_keys).reset_index(drop=True)
    
    def _initialize_version_columns(self, df: pd.DataFrame, col: str, max_versions: int, effective_date: datetime):
        """Inicializa colunas de versão para primeiro carregamento."""
        if max_versions == 2:
            df[f"{col}_previous"] = pd.NaT
            df[f"{col}_current"] = df[col]
            df[f"{col}_previous_date"] = pd.NaT
            df[f"{col}_current_date"] = effective_date
        else:
            for i in range(1, max_versions):
                df[f"{col}_v{i}"] = pd.NaT
                df[f"{col}_v{i}_date"] = pd.NaT
            df[f"{col}_current"] = df[col]
            df[f"{col}_current_date"] = effective_date
        
        # Remove coluna original
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    
    def _ensure_version_columns(self, df: pd.DataFrame, col: str, max_versions: int):
        """Garante que existem colunas de versão para o campo."""
        
        # Se é primeira vez, migrar coluna atual para estrutura de versões
        if col in df.columns and f"{col}_current" not in df.columns:
            df[f"{col}_current"] = df[col]
            df[f"{col}_current_date"] = pd.NaT
            df.drop(columns=[col], inplace=True)
        
        # Criar colunas de versão se não existirem
        if max_versions == 2:
            version_cols = [f"{col}_previous", f"{col}_current"]
            date_cols = [f"{col}_previous_date", f"{col}_current_date"]
        else:
            version_cols = [f"{col}_v{i}" for i in range(1, max_versions)] + [f"{col}_current"]
            date_cols = [f"{col}_v{i}_date" for i in range(1, max_versions)] + [f"{col}_current_date"]
        
        # Criar colunas que não existem
        for vcol, dcol in zip(version_cols, date_cols):
            if vcol not in df.columns:
                df[vcol] = pd.NaT
            if dcol not in df.columns:
                df[dcol] = pd.NaT
    
    def _process_column_versions(self, df: pd.DataFrame, col: str, max_versions: int, effective_date: datetime):
        """Processa mudanças para uma coluna com versões."""
        
        col_new = f"{col}_new"
        if col_new not in df.columns:
            return
        
        current_col = f"{col}_current"
        
        # Detectar mudanças (valor diferente E não é NaN)
        changed_mask = (df[current_col] != df[col_new]) & df[col_new].notna()
        
        if not changed_mask.any():
            return
        
        # Processar cada registro que mudou
        for idx in df[changed_mask].index:
            # Obter versões atuais
            if max_versions == 2:
                versions = [df.loc[idx, f"{col}_previous"], df.loc[idx, f"{col}_current"]]
                dates = [df.loc[idx, f"{col}_previous_date"], df.loc[idx, f"{col}_current_date"]]
            else:
                versions = []
                dates = []
                for i in range(1, max_versions):
                    versions.append(df.loc[idx, f"{col}_v{i}"])
                    dates.append(df.loc[idx, f"{col}_v{i}_date"])
                versions.append(df.loc[idx, f"{col}_current"])
                dates.append(df.loc[idx, f"{col}_current_date"])
            
            # Shift right: remover primeira versão, adicionar nova no final
            new_value = df.loc[idx, col_new]
            versions = versions[1:] + [new_value]
            dates = dates[1:] + [effective_date]
            
            # Atualizar no DataFrame
            if max_versions == 2:
                df.loc[idx, f"{col}_previous"] = versions[0]
                df.loc[idx, f"{col}_current"] = versions[1]
                df.loc[idx, f"{col}_previous_date"] = dates[0]
                df.loc[idx, f"{col}_current_date"] = dates[1]
            else:
                for i, (version, date) in enumerate(zip(versions[:-1], dates[:-1]), 1):
                    df.loc[idx, f"{col}_v{i}"] = version
                    df.loc[idx, f"{col}_v{i}_date"] = date
                df.loc[idx, f"{col}_current"] = versions[-1]
                df.loc[idx, f"{col}_current_date"] = dates[-1]
        
        # Registros novos (só existem em new_data)
        new_records_mask = df[current_col].isna() & df[col_new].notna()
        if new_records_mask.any():
            df.loc[new_records_mask, current_col] = df.loc[new_records_mask, col_new]
            df.loc[new_records_mask, f"{col}_current_date"] = effective_date