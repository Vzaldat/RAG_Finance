# Stock Market AI  - Core Data Engineering + RAG Pipeline
Intelligent Stock Market Data Pipeline with RAG using Medallion Architecture in Databricks
## Overview

This project implements a comprehensive stock market data pipeline using the Medallion Architecture (Bronze-Silver-Gold) in Databricks, enhanced with Retrieval-Augmented Generation (RAG) for intelligent analysis and insights. The system ingests historical stock data, real-time and historical news, market trends, and major holders' data, all critical factors influencing stock performance process this through multiple layers of refinement, and provides natural language querying capabilities for financial analysis.


![image](https://github.com/user-attachments/assets/1bc51bf7-a30b-4d70-8f6f-45a5d320e2e7)

##  Architecture

```
Stock APIs → Bronze Layer → Silver Layer → Gold Layer → Vector DB → RAG System → StreamlitUI chatbot
    ↓           ↓             ↓            ↓           ↓         ↓            ↓
yfinance    Raw Data    Cleaned Data   Business    FAISS    Gemini AI   Chatbot Output
Finnhub     Storage     Validation     Ready       Vector   LangChain   
```

##  Tech Stack

### Data Engineering
- **Platform**: Databricks (Apache Spark)
- **Orchestration**: Databricks Workflows
- **Data Lake**: Delta Lake with ACID transactions
- **Data Sources**: 
  - Yahoo Finance (yfinance API)
  - Finnhub API (financial news)
- **Programming**: PySpark, Python
- **Storage**: Unity Catalog with medallion architecture
- **Scheduling**: Automated workflow scheduling

### RAG Components
- **Embeddings**: Google Generative AI Embeddings (models/embedding-001)
- **Vector Database**: FAISS (Facebook AI Similarity Search)
- **LLM**: Google Gemini 2.0 Flash
- **Framework**: LangChain for orchestration
- **Text Processing**: RecursiveCharacterTextSplitter for optimal chunking
- **Financial Analysis**: Custom financial metrics and indicators


##  Project Structure

```
├── notebooks/                          # Databricks notebooks
│   ├── bronze_layer/
│   │   ├── Stocks_ai_historical_bronze.py      # Historical data from yfinance
│   │   └── stocks_news_bronze_layer.py         # News data from Finnhub API
│   ├── silver_layer/
│   │   └── combined_silver_layer_historical+news.py  # Data cleansing & standardization
│   ├── gold_layer/
│   │   └── Stocks_ai_gold_layer.py             # Business-ready analytics tables
│   ├── rag_implementation/
│   │   ├── stock_market_rag_basic.py           # Current RAG implementation
│   │   └── stock_market_rag_enhanced.py        # Enhanced production-ready RAG
│   └── analytics/
│       └── financial_dashboard.py              # Interactive analytics dashboard
├── data_catalog/                       # Databricks Unity Catalog structure
│   └── stocks_ai/
│       ├── stocks_name_ticker/
│       │   └── stock_names                      # Master ticker & company names
│       ├── stocks_history_data/
│       │   ├── stock_history_bronze_layer       # Raw OHLCV data
│       │   └── stock_history_silver_layer       # Cleaned OHLCV data
│       ├── stocks_news_data/
│       │   ├── stock_news_bronze_layer          # Raw news summaries
│       │   └── stock_news_silver_layer          # Processed news data
│       ├── stocks_holders_data/
│       │   └── stock_institutional_holders      # Institutional holder data
│       └── stocks_GOLD_LAYER/
│           ├── Stocks_news_historical_RAG       # Combined analytics table
│           └── Stocks_holders_RAG               # Enriched holders data
├── src/                               # Utility functions and classes
│   ├── data_ingestion/
│   ├── data_processing/
│   ├── rag_components/
│   └── financial_analysis/
├── config/                            # Configuration files
├── tests/                             # Unit tests
└── docs/                              # Documentation
```

##  Data Pipeline Implementation

![image](https://github.com/user-attachments/assets/f1ca59cd-8ef3-49ef-9be6-8cd0006722eb)

### Bronze Layer - Raw Data Ingestion

#### 1. Historical Stock Data (`Stocks_ai_historical_bronze`)

**Data Source**: Yahoo Finance via yfinance API
**Table**: `stocks_ai.stocks_history_data.stock_history_bronze_layer`

**Key Features:**
- **Incremental Loading**: Automatically detects existing data and loads only new records
- **Date Logic**: Initial load (180 days) vs incremental load (since last date)
- **Multi-ticker Processing**: Batch processing of all tickers from master table
- **Schema Standardization**: Flattens multi-index columns from yfinance

#### 2. News Data (`stocks_news_bronze_layer`)

**Data Source**: Finnhub API
**Table**: `stocks_ai.stocks_news_data.stock_news_bronze_layer`

**Key Features:**
- **News Aggregation**: Company news summaries with timestamps
- **Rate Limiting**: Built-in API rate limiting (1 sec between requests)
- **Date-based Incremental Loading**: Initial load (5 days) vs incremental
- **Content Filtering**: Only articles with summary and datetime fields

### Silver Layer - Data Cleansing & Standardization

#### Combined Silver Processing (`combined_silver_layer_historical+news`)

**Historical Data Transformations:**
- **Date Formatting**: Standardized to MM-dd-yyyy format
- **Precision Control**: Price fields rounded to 3 decimal places
- **Column Standardization**: Consistent naming conventions
- **Data Validation**: Type casting and null handling

**Output Table**: `stocks_ai.stocks_history_data.stock_history_silver_layer`

### Gold Layer - Business-Ready Analytics (`Stocks_ai_gold_layer`)

**Data Integration Strategy:**
- **Multi-source Joining**: Combines historical, news, and reference data
- **News Aggregation**: Groups multiple news articles per stock per day
- **Company Enrichment**: Adds company names from reference table
- **Institutional Data**: Includes holder information for comprehensive analysis

![image](https://github.com/user-attachments/assets/64b6ccb2-7dec-46aa-91b6-e89d7d0a43a5)
![image](https://github.com/user-attachments/assets/227ca7bf-b538-4adb-81ed-2172b47d73ca)


**Output Tables:**
- `stocks_ai.stocks_GOLD_LAYER.Stocks_news_historical_RAG` - Main analytics table
- `stocks_ai.stocks_GOLD_LAYER.Stocks_holders_RAG` - Institutional holders data
  

### RAG Layer - AI-Powered Financial Analysis

#### Current Implementation Features:

**Text Processing & Embeddings:**
- **Text Concatenation**: Combines historical data, news, and holder information into contextual strings
- **Chunking Strategy**: Uses CharacterTextSplitter (1024 tokens, 100 overlap)
- **Embedding Model**: Google Generative AI embeddings (models/embedding-001)
- **Vector Store**: FAISS for fast similarity search


#### Sample Queries Supported:
- "Give me a summary of how AAPL stock has performed in the past"
- "What are the major institutional holders of Tesla?"
- "Analyze the correlation between news sentiment and stock price movements"
- "Which stocks showed the highest volatility in the last quarter?"

##  Data Schema

### Bronze Layer Tables

| Table | Schema | Description |
|-------|--------|-------------|
| **stock_history_bronze_layer** | | Raw OHLCV data |
| | date (DateType) | Trading date |
| | open, high, low, close (FloatType) | Price data |
| | adj_close (FloatType) | Adjusted closing price |
| | volume (IntegerType) | Trading volume |
| | ticker (StringType) | Stock symbol |
| **stock_news_bronze_layer** | | Raw news data |
| | stock (StringType) | Stock ticker |
| | summary (StringType) | News article summary |
| | date (DateType) | Article date |

### Gold Layer Tables

| Table | Key Features | Use Case |
|-------|-------------|----------|
| **Stocks_news_historical_RAG** | Combined OHLCV + aggregated news + company info | Primary RAG data source |
| **Stocks_holders_RAG** | Institutional holders + company metadata | Ownership analysis |



## 🔮 Future Enhancements

### Immediate Roadmap
-  **Enhanced RAG System**: Production-ready version with error handling
-  **Real-time Streaming**: Live market data integration
-  **Advanced Analytics**: Technical indicators and risk metrics
-  **ML Models**: Price prediction and volatility forecasting

### Advanced Features
-  **Multi-modal RAG**: Charts and graphs in responses
-  **Conversation Memory**: Multi-turn dialogue support  
-  **Custom Financial Metrics**: Sharpe ratio, Beta, VaR calculations
-  **Portfolio Analysis**: Multi-stock portfolio optimization
-  **Sentiment Analysis**: News sentiment scoring
-  **Options Data**: Volatility surface analysis


## 📈 Use Cases

1. **Financial Research**: "Analyze the correlation between oil prices and energy stocks"
2. **Risk Management**: "What are the risk factors for my tech stock portfolio?"
3. **Investment Decisions**: "Should I buy Tesla based on recent news and performance?"
4. **Market Analysis**: "How did the Fed announcement affect bank stocks?"
5. **Portfolio Optimization**: "Suggest a balanced portfolio based on current market conditions"

