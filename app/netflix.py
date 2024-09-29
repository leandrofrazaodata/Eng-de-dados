import streamlit as st
import psycopg2
import pandas as pd

st.set_page_config(
    page_title="Netflix", # Nome que aparece na aba do Browser
    page_icon="https://upload.wikimedia.org/wikipedia/commons/thumb/7/75/Netflix_icon.svg/2048px-Netflix_icon.svg.png",  # Imagem que aparece na aba do Browser
    layout="wide",  
)

def conectar_banco():
    try:
        conn = psycopg2.connect(
            host="172.18.0.6",    
            database="datalake", 
            user="postgres",       
            password="66422483", 
            port="5432"      
        )
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None
    
def listar_tabelas():
    conn = conectar_banco()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'mart' ORDER BY table_name;
            """)
            tabelas = cursor.fetchall()
            conn.close()
            return [t[0] for t in tabelas] 
        except Exception as e:
            st.error(f"Erro ao listar tabelas: {e}")
            return []

def carregar_dados_tabela(tabela):
    conn = conectar_banco()
    if conn:
        try:
            query = f"SELECT * FROM mart.{tabela};"
            dados = pd.read_sql(query, conn)
            conn.close()
            return dados
        except Exception as e:
            st.error(f"Erro ao carregar a tabela {tabela}: {e}")
            return pd.DataFrame()

st.title('Dados do catálogo da Netflix') # Header principal
st.sidebar.image("https://images.ctfassets.net/4cd45et68cgf/4nBnsuPq03diC5eHXnQYx/d48a4664cdc48b6065b0be2d0c7bc388/Netflix-Logo.jpg", use_column_width=True) # Imagem da barra lateral
tabelas = listar_tabelas()
tabela_selecionada = st.sidebar.selectbox("Escolha uma tabela", tabelas)

if tabela_selecionada:
    st.subheader(f'Dados da Tabela: {tabela_selecionada}')
    dados_tabela = carregar_dados_tabela(tabela_selecionada)
    
    if not dados_tabela.empty:
        st.dataframe(dados_tabela, use_container_width=True) # use_container_width=True ocupa todo o espaço disponível para o df
    else:
        st.write(f"A tabela {tabela_selecionada} está vazia.")