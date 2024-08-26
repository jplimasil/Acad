# Importar bibliotecas necessárias
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Função para carregar dados de um arquivo CSV
def carregar_dados(caminho_arquivo):
    dados = pd.read_csv(caminho_arquivo)
    return dados

# Função para processar dados com PySpark
def processar_dados_spark(caminho_arquivo):
    spark = SparkSession.builder \
        .appName("MonitoramentoDesempenho") \
        .getOrCreate()
    
    df_spark = spark.read.csv(caminho_arquivo, header=True, inferSchema=True)
    df_spark.createOrReplaceTempView("desempenho")
    
    media_peso = spark.sql("SELECT exercicio, AVG(peso) as media_peso FROM desempenho GROUP BY exercicio")
    return media_peso

# Função para realizar análise de dados com Pandas
def analise_dados(dados):
    dados['data'] = pd.to_datetime(dados['data'])
    analise = dados.groupby('exercicio').agg({
        'repeticoes': 'mean',
        'peso': 'mean'
    }).reset_index()
    return analise

# Função para visualizar os dados analisados
def visualizar_dados(analise_resultado):
    plt.figure(figsize=(10, 6))
    
    # Gráfico de barras da média de repetições por exercício
    plt.subplot(1, 2, 1)
    plt.bar(analise_resultado['exercicio'], analise_resultado['repeticoes'], color='blue')
    plt.xlabel('Exercício')
    plt.ylabel('Média de Repetições')
    plt.title('Média de Repetições por Exercício')
    
    # Gráfico de barras da média de peso por exercício
    plt.subplot(1, 2, 2)
    plt.bar(analise_resultado['exercicio'], analise_resultado['peso'], color='red')
    plt.xlabel('Exercício')
    plt.ylabel('Média de Peso')
    plt.title('Média de Peso por Exercício')
    
    plt.tight_layout()
    plt.show()

# Caminho para o arquivo CSV
caminho = 'dados_desempenho.csv'  # Substitua pelo caminho do seu arquivo CSV

# Carregar e exibir dados
dados = carregar_dados(caminho)
print("Dados carregados:")
print(dados.head())

# Processar dados com PySpark
media_peso = processar_dados_spark(caminho)
print("Média de Peso por Exercício (PySpark):")
media_peso.show()

# Analisar dados com Pandas
analise_resultado = analise_dados(dados)
print("Análise de Dados (Pandas):")
print(analise_resultado)

# Visualizar resultados
visualizar_dados(analise_resultado)
