from abc import ABC, abstractmethod

class SCDEngine(ABC):
    """Interface abstrata para engines de SCD."""
    @abstractmethod
    def process_scd_type1(self, current_data: Any, new_data: Any,
                         business_keys: List[str], track_columns: List[str]) -> Any:
        """SCD Tipo 1: Sobrescrever sem histórico."""
        pass
    
    @abstractmethod
    def process_scd_type2(self, current_data: Any, new_data: Any, 
                         business_keys: List[str], track_columns: List[str],
                         effective_date: Optional[datetime] = None) -> Any:
        """SCD Tipo 2: Histórico completo."""
        pass
    
    @abstractmethod
    def process_scd_type3(self, current_data: Any, new_data: Any,
                         business_keys: List[str], track_columns: List[str],
                         max_versions: int = 3,
                         effective_date: Optional[datetime] = None) -> Any:
        """SCD Tipo 3: Versões configuráveis."""
        pass
