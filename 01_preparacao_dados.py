import polars as pl

# =======================================================================================
# DICIONÁRIO DE DADOS: VARIÁVEIS SELECIONADAS
# Todas as variáveis abaixo representam informações que o banco possuía ANTES
# da aprovação do crédito, garantindo que o modelo não sofra de Vazamento de Dados.
# =======================================================================================

# loan_amnt   : Valor total do empréstimo solicitado pelo cliente (em dólares).
# term        : Prazo estipulado para o pagamento (ex: 36 ou 60 meses).
# int_rate    : Taxa de juros aplicada ao empréstimo (%).
# grade       : Nota de risco interna da plataforma (A até G, sendo A o menor risco).
# annual_inc  : Renda anual autodeclarada pelo tomador do empréstimo.
# dti         : Debt-to-Income (%). Relação Dívida/Renda do cliente, calculada dividindo 
#               os pagamentos mensais de dívidas pela renda mensal.
# loan_status : Status original do empréstimo (usado para gerar a variável alvo 'target').
# emp_length           : Tempo de emprego no trabalho atual (em anos).
# home_ownership       : Situação de moradia (OWN = Própria, MORTGAGE = Financiada, RENT = Aluguel).
# fico_range_low       : Limite inferior do FICO Score (equivalente ao score do Serasa).
# fico_range_high      : Limite superior do FICO Score do cliente no momento da solicitação.
# open_acc             : Número de linhas de crédito (contas, cartões, empréstimos) atualmente abertas.
# total_acc            : Número total de linhas de crédito que o cliente já teve ao longo da vida.
# revol_util           : Taxa de utilização do limite rotativo (%). Mostra o quanto do limite do 
#                        cartão de crédito o cliente está usando no momento.
# mort_acc             : Quantidade de contas de financiamento imobiliário/hipoteca ativas.
# inq_last_6mths       : Consultas de crédito nos últimos 6 meses.
# pub_rec              : Número de registros públicos depreciativos (ex: execuções fiscais, penhoras).
# pub_rec_bankruptcies : Número de registros de falência pública no nome do cliente.
# issue_d              : Mês e ano em que o empréstimo foi originado. Utilizada exclusivamente para a separação temporal (out-of-time) dos conjuntos de treino e teste.
# =======================================================================================

important_cols = [
    'loan_amnt', 'term', 'int_rate', 'grade', 'annual_inc', 'dti', 'loan_status',
    'emp_length', 'home_ownership', 'fico_range_low', 'fico_range_high',
    'open_acc', 'pub_rec', 'revol_util', 'total_acc', 'mort_acc',
    'inq_last_6mths', 'pub_rec_bankruptcies', 'issue_d'
]

# Navega para a pasta DataSet_Loan, entra na pasta extraída e aponta para o CSV bruto
csv_path = r'DataSet_Loan\accepted_2007_to_2018q4.csv\accepted_2007_to_2018Q4.csv'

# Salva o Parquet limpo e finalizado direto na raiz da pasta DataSet_Loan
parquet_path = r'DataSet_Loan\base_credito_tcc.parquet'

print("Lendo CSV, aplicando filtros e convertendo para Parquet...")

(
    pl.scan_csv(csv_path, ignore_errors=True)
    
    .select(important_cols)
    
    # Filtra os status 
    .filter(pl.col('loan_status').is_in(['Fully Paid', 'Charged Off']))
    
    # Cria a coluna 'target' mapeando os valores
    .with_columns(
        pl.col('loan_status')
        .replace({'Fully Paid': 0, 'Charged Off': 1})
        .cast(pl.Int8) 
        .alias('target')
    )
    
    .drop('loan_status')
    
    .sink_parquet(parquet_path)
)

print(f"Sucesso! Arquivo gerado em: {parquet_path}")