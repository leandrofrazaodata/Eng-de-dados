import streamlit as st
import psycopg2
import pandas as pd

st.set_page_config(
    page_title="Netflix", 
    page_icon="https://upload.wikimedia.org/wikipedia/commons/thumb/7/75/Netflix_icon.svg/2048px-Netflix_icon.svg.png",  
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

st.title('Dados Netflix')

tabelas = listar_tabelas()
tabela_selecionada = st.sidebar.selectbox("Escolha uma tabela", tabelas)

if tabela_selecionada:
    st.subheader(f'Dados da Tabela: {tabela_selecionada}')
    dados_tabela = carregar_dados_tabela(tabela_selecionada)
    
    if not dados_tabela.empty:
        st.dataframe(dados_tabela)
    else:
        st.write(f"A tabela {tabela_selecionada} está vazia.")