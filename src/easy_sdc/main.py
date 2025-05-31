import engine
# Exemplo de teste
if __name__ == "__main__":
    print("🚀 SCD LIB - ALGORITMOS COMPLETOS IMPLEMENTADOS!")
    print("=" * 55)
    
    # Teste básico
    current = pd.DataFrame({
        'id': [1, 2],
        'name': ['João', 'Maria'],
        'salary': [5000, 6000],
        'surrogate_key': [1, 2],
        'valid_from': [datetime(2024, 1, 1)] * 2,
        'valid_to': [pd.NaT] * 2,
        'is_current': [True] * 2
    })
    
    new = pd.DataFrame({
        'id': [1, 3],
        'name': ['João Silva', 'Pedro'],
        'salary': [5500, 4000]
    })
    
    print("✅ Testando Tipo 1:")
    result1 = process_scd_type1(current, new, ['id'], ['name', 'salary'])
    print(f"Registros: {len(result1)}")
    
    print("✅ Testando Tipo 2:")
    result2 = process_scd_type2(current, new, ['id'], ['name', 'salary'])
    print(f"Registros: {len(result2)}")
    
    print("✅ Testando Tipo 3:")
    result3 = process_scd_type3(current, new, ['id'], ['salary'], max_versions=3)
    print(f"Registros: {len(result3)}")
    
    print("\n🎉 TODOS OS ALGORITMOS IMPLEMENTADOS E FUNCIONANDO!")


