"""
How to run in command prompt/terminal
    streamlit run path/to/your/file
"""
import streamlit as st
import os
import time
import pandas as pd
from langchain.text_splitter import CharacterTextSplitter
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.chains import RetrievalQA
from langchain_google_genai import ChatGoogleGenerativeAI
import os
import warnings

warnings.filterwarnings("ignore")

os.environ["GOOGLE_API_KEY"] = "GOOGLE_API_KEY" #or
api_key = "GOOGLE_API_KEY"

# Add custom CSS for better text formatting and consistent fonts
st.markdown("""
<style>
/* Global font and text styling */
.stChatMessage {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
}

.stChatMessage [data-testid="chatAvatarIcon-user"] {
    background-color: #007acc;
}

.stChatMessage [data-testid="chatAvatarIcon-assistant"] {
    background-color: #19c37d;
}

/* Fix text spacing and formatting in chat messages */
.stChatMessage .stMarkdown {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    line-height: 1.6;
    word-spacing: normal;
    letter-spacing: 0.02em;
}

.stChatMessage .stMarkdown p {
    margin-bottom: 1em;
    white-space: pre-wrap;
    word-wrap: break-word;
}

.stChatMessage .stMarkdown h1,
.stChatMessage .stMarkdown h2,
.stChatMessage .stMarkdown h3,
.stChatMessage .stMarkdown h4 {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    margin-top: 1.5em;
    margin-bottom: 0.5em;
}

.stChatMessage .stMarkdown ul,
.stChatMessage .stMarkdown ol {
    margin-left: 1.5em;
    margin-bottom: 1em;
}

.stChatMessage .stMarkdown li {
    margin-bottom: 0.5em;
}

/* Ensure proper spacing for numbers and text */
.stChatMessage .stMarkdown strong {
    font-weight: 600;
    margin-right: 0.3em;
}

/* Fix monospace font issues */
.stChatMessage .stMarkdown code {
    font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
    background-color: #f1f3f4;
    padding: 0.2em 0.4em;
    border-radius: 3px;
    font-size: 0.9em;
}

.stChatMessage .stMarkdown pre {
    font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
    background-color: #f8f9fa;
    padding: 1em;
    border-radius: 5px;
    overflow-x: auto;
    margin: 1em 0;
}

/* Suggestion buttons styling */
.suggestion-container {
    margin-bottom: 10px;
    padding: 10px 0;
}

.stButton > button {
    border-radius: 20px;
    border: 1px solid #e0e0e0;
    background-color: #f8f9fa;
    color: #333;
    font-size: 14px;
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    padding: 8px 16px;
    margin: 5px;
    transition: all 0.2s;
}

.stButton > button:hover {
    background-color: #e9ecef;
    border-color: #d0d7de;
}

/* Ensure consistent spacing in all text */
div[data-testid="stChatMessageContent"] {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif !important;
}

/* Fix any potential text rendering issues */
.stChatMessage * {
    text-rendering: optimizeLegibility;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}
</style>
""", unsafe_allow_html=True)

def load_and_process_csv(filepath1, filepath2):
    # data = hist_data_and_news.toPandas()
    data  = pd.read_csv(filepath1)
    # data1 = holders.toPandas()
    data1 = pd.read_csv(filepath2)
    data['hist_data_and_news'] = data.apply(
        lambda row: f"Stock ({row['company_name']} - {row['ticker']}) on date {row['date']}, opening {row['open']}, closing {row['close']}, high {row['high']}, low {row['low']}, volume {row['volume']}, and corresponding news - {' '.join(row['news_list']) if isinstance(row['news_list'], list) else ''} ", axis=1)
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
    VectorStore = FAISS.from_texts(chunks, embedding_model)
    return VectorStore

def generateContent(prompt, vectorstore, api_key=api_key):
    # Create LLM with system prompt for better stock prediction responses
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash", 
        google_api_key=api_key,
        temperature=0.7
    )
    
    # Create a more sophisticated chain with custom prompt template
    from langchain.prompts import PromptTemplate
    
    system_prompt = """
    You are an expert financial AI assistant specializing in stock market analysis and predictions. 
    
    Guidelines for stock predictions:
    1. Always base your predictions on the historical data, news sentiment, and holder information provided in the context
    2. Provide rough predictions with clear reasoning based on trends, patterns, and market sentiment
    3. Consider factors like: recent price movements, volume changes, news sentiment, institutional holdings
    4. Use technical analysis concepts when relevant (support/resistance levels, moving averages, trend analysis)
    5. Always include appropriate disclaimers about investment risks
    6. Be specific about timeframes (short-term, medium-term, long-term)
    7. Mention confidence levels and key risks/uncertainties
    8. Format your response with proper spacing and clear structure for better readability
    
    Format your responses as:
    
    **Current Analysis**: Brief overview of current stock status
    
    **Prediction**: Your rough forecast with reasoning
    
    **Key Factors**: Main drivers for your prediction
    
    **Risk Factors**: What could affect the prediction
    
    **Disclaimer**: Investment risk warning
    
    Use proper spacing between sections and ensure numbers are clearly separated from text with spaces.
    
    Context Information:
    {context}
    
    User Question: {question}
    
    Provide a comprehensive analysis with a rough prediction based on the available data:
    """
    
    prompt_template = PromptTemplate(
        template=system_prompt,
        input_variables=["context", "question"]
    )
    
    qa = RetrievalQA.from_chain_type(
        llm=llm,
        retriever=vectorstore.as_retriever(search_kwargs={"k": 10}),
        chain_type="stuff",
        chain_type_kwargs={"prompt": prompt_template}
    )
    
    response = qa.invoke({"query": prompt})
    
    # Clean up the response text to ensure proper spacing
    cleaned_response = response['result']
    
    # Fix common spacing issues
    import re
    # Add space after numbers followed by letters (e.g., "123ABC" -> "123 ABC")
    cleaned_response = re.sub(r'(\d)([A-Za-z])', r'\1 \2', cleaned_response)
    # Add space before numbers preceded by letters (e.g., "ABC123" -> "ABC 123")  
    cleaned_response = re.sub(r'([A-Za-z])(\d)', r'\1 \2', cleaned_response)
    # Fix multiple spaces
    cleaned_response = re.sub(r'\s+', ' ', cleaned_response)
    # Ensure proper line breaks around section headers
    cleaned_response = re.sub(r'\*\*([^*]+)\*\*', r'\n\n**\1**\n', cleaned_response)
    
    return cleaned_response.strip()


# Initialize the RAG system with CSV data
@st.cache_resource
def initialize_vectorstore():
    textdb = load_and_process_csv(r"filepath_stock_data_and_news_historical", r"filepath_holders")  
    # CSV files are loaded inside the function now
    chunks = chunkSplit(textdb)
    vectorstore = store_embeddings(chunks)
    return vectorstore

# Initialize vectorstore (cached for performance)
try:
    vectorstore = initialize_vectorstore()
except Exception as e:
    st.error(f"Error loading data: {e}")
    vectorstore = None


st.title("Stocks.ai")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Create a container at the bottom for suggestions and chat input
prompt_selected = None

# Add some spacing to push suggestions toward bottom
if len(st.session_state.messages) == 0:
    st.markdown("<br>" * 10, unsafe_allow_html=True)  # Add vertical space when no messages

# Suggestions positioned right above chat input (ChatGPT style)
if len(st.session_state.messages) == 0:  # Only show suggestions when chat is empty
    # Custom CSS for ChatGPT-like buttons
    st.markdown("""
    <style>
    .suggestion-container {
        margin-bottom: 10px;
        padding: 10px 0;
    }
    .stButton > button {
        border-radius: 20px;
        border: 1px solid #e0e0e0;
        background-color: #f8f9fa;
        color: #333;
        font-size: 14px;
        padding: 8px 16px;
        margin: 5px;
        transition: all 0.2s;
    }
    .stButton > button:hover {
        background-color: #e9ecef;
        border-color: #d0d7de;
    }
    </style>
    """, unsafe_allow_html=True)
    
# Chat input positioned at the very bottom
user_input = st.chat_input("Ask me anything about stocks and financial data...")

# Use selected prompt or user input
prompt = prompt_selected if prompt_selected else user_input

if prompt:
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Generate response using RAG system
    if vectorstore is not None:
        with st.spinner("Generating response..."):
            response = generateContent(prompt, vectorstore)
    else:
        response = "Please connect to your Databricks data source to enable AI responses."

    with st.chat_message("assistant"):
        # Use st.markdown with proper formatting for better display
        st.markdown(response, unsafe_allow_html=False)
    st.session_state.messages.append({"role": "assistant", "content": response})
    
    # Rerun to refresh the UI and hide the suggestions
    if prompt_selected:
        st.rerun()