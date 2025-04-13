
import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector
import seaborn as sns
import matplotlib.pyplot as plt


# --- Welcome Note ---
st.markdown("<h2 style='text-align: center;'>👋 Welcome to the Stock Market Dashboard!</h2>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center;'>Stay updated with trends, performance, and insights 📊📈</p>", unsafe_allow_html=True)


# Page title
st.title("📈 Stock Market Dashboard")

# --- Database connection ---
@st.cache_resource
def get_connection():
    return mysql.connector.connect(
        host="gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
        port=4000,
        user="4C5xvhb3JaX4mrB.root",
        password="M5kJTbyBovNOKjvP",
        database="stock_data"
    )

conn = get_connection()

# --- 1. Yearly Return Per Stock ---
@st.cache_data
def get_yearly_returns():
    query = """
    WITH price_bounds AS (
        SELECT 
            Ticker,
            MIN(date) AS first_date,
            MAX(date) AS last_date
        FROM stock_prices
        GROUP BY Ticker
    ),
    prices AS (
        SELECT 
            pb.Ticker,
            sp_first.close AS first_close,
            sp_last.close AS last_close
        FROM price_bounds pb
        JOIN stock_prices sp_first ON pb.Ticker = sp_first.Ticker AND pb.first_date = sp_first.date
        JOIN stock_prices sp_last ON pb.Ticker = sp_last.Ticker AND pb.last_date = sp_last.date
    )
    SELECT 
        Ticker,
        (last_close - first_close) / first_close AS yearly_return
    FROM prices
    """
    return pd.read_sql(query, conn)

returns_df = get_yearly_returns()
returns_df = returns_df.dropna(subset=["yearly_return"])

# --- 2. Top/Bottom 10 Stocks ---
top_10_green = returns_df.sort_values(by="yearly_return", ascending=False).head(10)
top_10_red = returns_df.sort_values(by="yearly_return").head(10)

st.subheader("🚀 Top 10 Green Stocks")
st.dataframe(top_10_green)

st.subheader("📉 Top 10 Loss Stocks")
st.dataframe(top_10_red)

# --- 3. Market Summary ---
@st.cache_data
def get_market_summary():
    query = """
    WITH ranked_prices AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY date ASC) AS rn_asc,
               ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY date DESC) AS rn_desc
        FROM stock_prices
    ),
    first_last AS (
        SELECT 
            Ticker,
            MAX(CASE WHEN rn_asc = 1 THEN close END) AS first_close,
            MAX(CASE WHEN rn_desc = 1 THEN close END) AS last_close,
            MAX(CASE WHEN rn_desc = 1 THEN volume END) AS last_volume
        FROM ranked_prices
        GROUP BY Ticker
    )
    SELECT 
        Ticker,
        first_close,
        last_close,
        last_volume,
        ((last_close - first_close) / first_close) * 100 AS yearly_return_pct
    FROM first_last
    """
    return pd.read_sql(query, conn)
 
summary_df = get_market_summary()
summary_df = summary_df.dropna()
 
# Calculate metrics just like your Python script
green_count = (summary_df["yearly_return_pct"] > 0).sum()
red_count = (summary_df["yearly_return_pct"] <= 0).sum()
avg_price = summary_df["last_close"].mean()
avg_volume = summary_df["last_volume"].mean()
 
# Display in Streamlit
st.subheader("📊 Market Summary")
st.metric("Green Stocks", green_count)
st.metric("Red Stocks", red_count)
st.metric("Average Price", f"₹{avg_price:.2f}")
st.metric("Average Volume", f"{int(avg_volume):,}")

# --- 4. Volatility Analysis ---
st.subheader("⚡ Top 10 Most Volatile Stocks")
 
@st.cache_data
def get_volatility():
    query = """
    WITH returns AS (
        SELECT
            Ticker,
            date,
            (close - LAG(close) OVER (PARTITION BY Ticker ORDER BY date)) / LAG(close) OVER (PARTITION BY Ticker ORDER BY date) AS daily_return
        FROM stock_prices
    )
    SELECT Ticker, STDDEV(daily_return) AS volatility
    FROM returns
    WHERE daily_return IS NOT NULL
    GROUP BY Ticker
    ORDER BY volatility DESC
    LIMIT 10
    """
    return pd.read_sql(query, conn)
 
volatility_df = get_volatility()
st.dataframe(volatility_df)
st.bar_chart(volatility_df.set_index("Ticker")["volatility"])

# --- 5. Cumulative Return Over Time ---
st.subheader("📈 Cumulative Return Over Time (Top 5 Performing Stocks)")

@st.cache_data
def get_cumulative_returns():
    query = """
    WITH returns AS (
        SELECT 
            Ticker,
            date,
            (close - LAG(close) OVER (PARTITION BY Ticker ORDER BY date)) / LAG(close) OVER (PARTITION BY Ticker ORDER BY date) AS daily_return
        FROM stock_prices
    ),
    cum_returns AS (
        SELECT
            Ticker,
            date,
            SUM(LOG(1 + daily_return)) OVER (PARTITION BY Ticker ORDER BY date) AS log_cum_return
        FROM returns
        WHERE daily_return IS NOT NULL
    ),
    ranked_returns AS (
        SELECT 
            Ticker,
            MAX(EXP(log_cum_return) - 1) AS cumulative_return
        FROM cum_returns
        GROUP BY Ticker
        ORDER BY cumulative_return DESC
        LIMIT 5
    )
    SELECT 
        c.Ticker, 
        c.date, 
        EXP(c.log_cum_return) - 1 AS cumulative_return
    FROM cum_returns c
    JOIN ranked_returns r ON c.Ticker = r.Ticker
    ORDER BY c.Ticker, c.date
    """
    return pd.read_sql(query, conn)

cum_df = get_cumulative_returns()
pivot_df = cum_df.pivot(index='date', columns='Ticker', values='cumulative_return')
st.line_chart(pivot_df)

# --- 7. Sector-wise Performance from CSV ---
st.subheader("🏢 Sector-wise Average Yearly Return")

@st.cache_data
def load_sector_csv_data():
    csv_path = "D:\Stockproject\data\sectorwise_data.csv"
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date'])
    return df

csv_sector_df = load_sector_csv_data()

# Calculate yearly return per Ticker
ticker_returns = csv_sector_df.sort_values(['Ticker', 'date']).groupby('Ticker').agg(
    first_close=('close', 'first'),
    last_close=('close', 'last'),
    sector=('sector', 'first')
).reset_index()

ticker_returns['yearly_return'] = (ticker_returns['last_close'] - ticker_returns['first_close']) / ticker_returns['first_close'] * 100

# Aggregate by sector
sector_returns = ticker_returns.groupby('sector')['yearly_return'].mean().reset_index()
sector_returns = sector_returns.rename(columns={'sector': 'Sector', 'yearly_return': 'AvgYearlyReturn'})
sector_returns = sector_returns.sort_values(by='AvgYearlyReturn', ascending=False)

# Show table
st.dataframe(sector_returns)

# Plot
fig = px.bar(sector_returns, x='Sector', y='AvgYearlyReturn',
             title="📊 Sector-wise Average Yearly Return (from CSV)",
             labels={"AvgYearlyReturn": "Avg Yearly Return (%)"},
             color='AvgYearlyReturn',
             color_continuous_scale='RdYlGn')
fig.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig)


# --- 7. Stock Correlation Heatmap ---
st.subheader("🔗 Stock Price Correlation Heatmap")

@st.cache_data
def get_close_prices():
    query = "SELECT Ticker, date, Close FROM stock_prices"
    return pd.read_sql(query, conn)

price_df = get_close_prices()
price_pivot = price_df.pivot(index='date', columns='Ticker', values='Close')
daily_pct_change = price_pivot.pct_change().dropna()
correlation_matrix = daily_pct_change.corr()

st.write("Correlation between daily closing returns of different stocks:")
fig, ax = plt.subplots(figsize=(20, 16))
sns.heatmap(correlation_matrix, cmap='coolwarm', annot=True, fmt=".2f", ax=ax, linewidths=0.5)
plt.title("📊 Stock Price Correlation Heatmap")
st.pyplot(fig)

# --- 8. Monthly Gainers & Losers ---
@st.cache_data
def get_monthly_returns():
    query = """
    WITH formatted_data AS (
        SELECT 
            Ticker,
            DATE_FORMAT(date, '%Y-%m') AS month,
            date,
            Close
        FROM stock_prices
    ),
    date_bounds AS (
        SELECT 
            Ticker,
            month,
            MIN(date) AS first_date,
            MAX(date) AS last_date
        FROM formatted_data
        GROUP BY Ticker, month
    ),
    monthly_prices AS (
        SELECT 
            db.Ticker,
            db.month,
            sp_first.Close AS first_close,
            sp_last.Close AS last_close
        FROM date_bounds db
        JOIN formatted_data sp_first 
            ON db.Ticker = sp_first.Ticker AND db.first_date = sp_first.date AND db.month = sp_first.month
        JOIN formatted_data sp_last 
            ON db.Ticker = sp_last.Ticker AND db.last_date = sp_last.date AND db.month = sp_last.month
    )
    SELECT 
        Ticker,
        month,
        ROUND((last_close - first_close) / first_close * 100, 2) AS monthly_return
    FROM monthly_prices
    WHERE first_close IS NOT NULL AND last_close IS NOT NULL
    """
    return pd.read_sql(query, conn)

monthly_df = get_monthly_returns()
monthly_df['month'] = pd.to_datetime(monthly_df['month'])
monthly_df = monthly_df.sort_values(by='month')
monthly_df['month_str'] = monthly_df['month'].dt.strftime('%B %Y')

months = monthly_df['month'].dt.to_period('M').drop_duplicates().sort_values()

for period in months:
    period_str = period.strftime('%B %Y')
    st.markdown(f"### 📅 {period_str} — Gainers & Losers")
    
    month_data = monthly_df[monthly_df['month'].dt.to_period('M') == period]
    top_gainers = month_data.sort_values(by='monthly_return', ascending=False).head(5)
    top_losers = month_data.sort_values(by='monthly_return').head(5)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🚀 Top 5 Gainers")
        fig1 = px.bar(top_gainers, x='Ticker', y='monthly_return',
                      color='monthly_return', color_continuous_scale='Greens',
                      labels={'monthly_return': '% Return'},
                      title=f"Gainers - {period_str}")
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.markdown("#### 📉 Top 5 Losers")
        fig2 = px.bar(top_losers, x='Ticker', y='monthly_return',
                      color='monthly_return', color_continuous_scale='Reds_r',
                      labels={'monthly_return': '% Return'},
                      title=f"Losers - {period_str}")
        st.plotly_chart(fig2, use_container_width=True)


# --- Final Thank You Note ---
st.markdown("---")
st.markdown("<h4 style='text-align: center;'>🙏 Thank you for exploring the Stock Market Dashboard! Happy Investing! 📊📈😊</h4>", unsafe_allow_html=True)
