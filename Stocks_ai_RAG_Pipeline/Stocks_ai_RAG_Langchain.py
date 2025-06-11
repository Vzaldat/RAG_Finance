# Databricks notebook source
# MAGIC %pip install langchain langchain_community langchain_google_genai pyspark numpy<2 pandas faiss-cpu

# COMMAND ----------


from langchain.text_splitter import CharacterTextSplitter
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain.vectorstores.faiss import FAISS
from langchain.chains import RetrievalQA
from langchain.chat_models import init_chat_model
from langchain_core.vectorstores import InMemoryVectorStore
import os
import pyspark
spark = pyspark.sql.SparkSession.builder.getOrCreate()
import warnings
warnings.filterwarnings("ignore")

# Load API keys from environment variables - SET THESE IN YOUR DATABRICKS SECRETS OR LOCAL .env FILE
# os.environ["GOOGLE_API_KEY"] = "YOUR_GOOGLE_API_KEY_HERE"
# os.environ["COHERE_API_KEY"] = "YOUR_COHERE_API_KEY_HERE" 
# os.environ["MISTRALAI_API_KEY"] = "YOUR_MISTRAL_API_KEY_HERE"

# Get API keys from environment
api_key = os.environ.get("GOOGLE_API_KEY")
if not api_key:
    raise ValueError("GOOGLE_API_KEY environment variable is not set")

os.environ["DATABRICKS_HOST"] = "https://adb-3718798521757669.9.azuredatabricks.net/serving-endpoints/"

def load_and_process_csv(hist_data_and_news, holders):
  # data = pandas.read_csv(file_path)
  data = hist_data_and_news.toPandas()
  #SCHEMA - date close high low open volume ticker stock_name news
  # data1 = pandas.read_csv(holders)
  data1 = holders.toPandas()
  #SCHEMA - date_reported holder pctHeld shares Value pctChange ticker stock_name
  data['hist_data_and_news'] = data.apply(lambda row: f"Stock ({row['company_name']} - {row['ticker']}) on date {row['date'] }, opening {row['open']}, closing {row['close']}, high {row['high']}, low {row['low']}, volume {row['volume']}, and corresponding news - {' '.join(row['news_list']) if isinstance(row['news_list'], list) else ''} ", axis=1)
  holders_grouped = data1.groupby(['company_name', 'ticker']).apply(
      lambda group: f"Stock ({group.iloc[0]['company_name']} - {group.iloc[0]['ticker']}), with holders: " + 
      "; ".join([f"{row['Holder']} on {row['Date_Reported']} holding {row['pctHeld']}% ({row['Shares']} shares) valued at {row['Value']} " 
                 for _, row in group.iterrows()])
  ).reset_index(name='holders_text')
  return " ".join(data['hist_data_and_news'].tolist()) + " ".join(holders_grouped['holders_text'].tolist())

def chunkSplit(text):
  splitter = CharacterTextSplitter(chunk_size=1024, chunk_overlap=100)
  chunks = splitter.split_text(text)
  return chunks

def store_embeddings(chunks):
  embedding_model = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
  # embedding_model = MistralAIEmbeddings(model = "mistral-embed")
  # print(embedding_model.embed_documents('Hi'))
  VectorStore = FAISS.from_texts(chunks, embedding_model)
  return VectorStore

def generateContent(prompt, vectorstore, api_key=None):
  if not api_key:
    api_key = os.environ.get("GOOGLE_API_KEY")
  if not api_key:
    raise ValueError("API key not provided and GOOGLE_API_KEY environment variable is not set")
  
  qa = RetrievalQA.from_chain_type(
      llm = init_chat_model(model="gemini-2.0-flash", model_provider = "google_genai", api_key=api_key),
      retriever=vectorstore.as_retriever(search_kwargs={"k":10}),
      chain_type="stuff"
  )
  response = qa.invoke({"query" : prompt})
  return response['result']

hist_data_and_news = spark.sql('select * from stocks_ai.stocks_gold_layer.stocks_news_historical_rag')
holders = spark.sql('select * from stocks_ai.stocks_gold_layer.stocks_holders_rag')
textdb = load_and_process_csv(hist_data_and_news, holders)
chunks = chunkSplit(textdb)
vectorstore = store_embeddings(chunks)
# prompt = dbutils.widgets.get("prompt")
# response = generateContent(prompt, vectorstore)
# displayHTML(f"<h2>Response:</h2><p>{response}</p>")
# %python
# dbutils.widgets.text("prompt", "Enter your query here", "Prompt")
# response = dbutils.widgets.get("prompt")
# print(response)
print(generateContent("give me a summary of how the AAPL stock has performed in the past, use the historical data to see how it perform in the month of June ", vectorstore))

# COMMAND ----------

