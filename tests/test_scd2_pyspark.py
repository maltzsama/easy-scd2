import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from easy_sdc2.helper.scd2_pyspark import apply_scd_type2


class TestApplySCDType2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Configuração do SparkSession para o teste
        cls.spark = (
            SparkSession.builder.master("local[1]").appName("SCDTest").getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        # Configurar os dados de exemplo (source e target)
        self.source_data = [
            (1, "Alice", "2024-01-01", "2024-01-01"),
            (2, "Bob", "2024-01-01", "2024-01-01"),
        ]
        self.target_data = [
            (
                1,
                "Alice",
                "2024-01-01",
                "2024-01-01",
                "2024-01-01",
                "3000-01-01",
                True,
                "sk1",
            ),
            (
                2,
                "Bob",
                "2024-01-01",
                "2024-01-01",
                "2024-01-01",
                "3000-01-01",
                True,
                "sk2",
            ),
        ]

        # Definir os schemas para os DataFrames
        self.source_schema = ["id", "name", "valid_from", "update_date"]
        self.target_schema = [
            "id",
            "name",
            "valid_from",
            "valid_to",
            "update_date",
            "is_current",
            "surrogate_key",
        ]

        # Criar os DataFrames
        self.source_df = self.spark.createDataFrame(
            self.source_data, self.source_schema
        )
        self.target_df = self.spark.createDataFrame(
            self.target_data, self.target_schema
        )

    def test_apply_scd_type2(self):
        # Aplicar a função SCD Type 2
        result_df = apply_scd_type2(
            self.source_df, self.target_df, pk="id", surrogate_key_strategy="uuid"
        )

        # Verificar se o resultado tem as colunas esperadas
        result_columns = result_df.columns
        self.assertIn("surrogate_key", result_columns)
        self.assertIn("valid_from", result_columns)
        self.assertIn("valid_to", result_columns)
        self.assertIn("is_current", result_columns)

        # Verificar se a surrogate key foi criada ou preservada
        result_df.show(truncate=False)
        result_df.createOrReplaceTempView("result")

        # Teste se o valor da surrogate_key para o registro de Alice foi preservado
        preserved_key = self.spark.sql(
            "SELECT surrogate_key FROM result WHERE name = 'Alice'"
        ).collect()[0][0]
        self.assertEqual(preserved_key, "sk1")

        # Teste se o valor da surrogate_key para o novo registro foi gerado corretamente (UUID ou concatenação)
        new_key = self.spark.sql(
            "SELECT surrogate_key FROM result WHERE name = 'Bob'"
        ).collect()[0][0]
        self.assertNotEqual(
            new_key, "sk2"
        )  # Deveria ser uma chave diferente, pois 'sk2' já estava no target

    def test_edge_case_no_changes(self):
        # Caso em que não há mudanças (source e target são idênticos)
        source_data = [
            (1, "Alice", "2024-01-01", "2024-01-01"),
            (2, "Bob", "2024-01-01", "2024-01-01"),
        ]
        target_data = [
            (
                1,
                "Alice",
                "2024-01-01",
                "2024-01-01",
                "2024-01-01",
                "3000-01-01",
                True,
                "sk1",
            ),
            (
                2,
                "Bob",
                "2024-01-01",
                "2024-01-01",
                "2024-01-01",
                "3000-01-01",
                True,
                "sk2",
            ),
        ]

        source_df = self.spark.createDataFrame(source_data, self.source_schema)
        target_df = self.spark.createDataFrame(target_data, self.target_schema)

        result_df = apply_scd_type2(
            source_df, target_df, pk="id", surrogate_key_strategy="uuid"
        )

        # Verificar se a surrogate_key foi preservada para os registros inalterados
        result_df.createOrReplaceTempView("result")

        preserved_key = self.spark.sql(
            "SELECT surrogate_key FROM result WHERE name = 'Alice'"
        ).collect()[0][0]
        self.assertEqual(preserved_key, "sk1")

        preserved_key_bob = self.spark.sql(
            "SELECT surrogate_key FROM result WHERE name = 'Bob'"
        ).collect()[0][0]
        self.assertEqual(preserved_key_bob, "sk2")

    # Outros testes podem ser adicionados, como casos de dados ausentes, dados com alterações, etc.


if __name__ == "__main__":
    unittest.main()
