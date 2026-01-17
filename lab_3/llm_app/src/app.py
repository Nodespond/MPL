import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from openai import OpenAI
import plotly.express as px
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

print(DB_CONFIG)

DB_URL = (f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
          f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
LLM_URL = "http://localhost:1234/v1"


@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


engine = get_engine()

def get_existing_tables():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result]
            return tables
    except Exception as e:
        st.error(f"Не удалось получить список таблиц\n{str(e)}")
        return []


def upload_csv_to_table(file, table_name, if_exists="replace"):
    try:
        df = pd.read_csv(file)
        df.columns = [col.strip().lower().replace(" ", "_").replace("-", "_").replace("__", "_") for col in df.columns]
        with st.spinner(f"Загружаем данные в таблицу '{table_name}'..."):
            df.to_sql(table_name, engine, if_exists=if_exists, index=False, chunksize=50_000, method="multi")
        row_count = len(df)
        st.success(f"Успешно загружено **{row_count:,}** строк в таблицу `{table_name}`")
        st.subheader("Первые 5 строк загруженных данных")
        st.dataframe(df.head())
    except Exception as e:
        st.error(f"Ошибка при загрузке:\n{str(e)}")


@st.cache_data(ttl=300)
def get_database_schema():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    table_name,
                    string_agg(
                        column_name || ' ' || data_type ||
                        CASE WHEN character_maximum_length IS NOT NULL 
                             THEN '(' || character_maximum_length || ')' 
                             ELSE '' 
                        END ||
                        CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END,
                        E',\n    '
                    ) as columns
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name NOT IN ('pg_stat_statements')  -- можно исключить служебные
                GROUP BY table_name
                ORDER BY table_name;
            """))

            schema_lines = ["Схема базы данных (PostgreSQL):\n"]
            for table, cols in result:
                schema_lines.append(f"Таблица: {table}")
                schema_lines.append(f"    {cols}")
                schema_lines.append("")

            return "\n".join(schema_lines)

    except Exception as e:
        st.error(f"Не удалось получить схему базы данных\n{str(e)}")
        return "Ошибка получения схемы базы данных."


def generate_response(question):
    prompt = f"""Ты эксперт по анализу финансовых данных акций и SQL/Plotly в Streamlit.

    Схема базы данных (используй ТОЛЬКО эти таблицы и колонки!):
    {get_database_schema()}
    
    ВАЖНЫЕ ПРАВИЛА ОТВЕТА (строго соблюдай!):
    1. Отвечай ТОЛЬКО на русском языке.
    2. Если запрос НЕ требует данных из БД и НЕ про графики → дай простой текстовый ответ.
    3. Если нужно показать данные, статистику или список → генерируй ТОЛЬКО SQL-запрос в блоке ```sql ... ```
    4. Если запрос про график, диаграмму, chart, plot или визуализацию → ОБЯЗАТЕЛЬНО генерируй:
       - Сначала SQL-запрос в ```sql ... ```
       - Сразу после него блок ```plot ... ``` с параметрами в ОДНОЙ строке: type=line x=date y=close title=Динамика цены Amazon
       Возможные type: line, bar, scatter, histogram
       Для сравнения компаний: в y через запятую (y=close_amzn,close_aapl), в SQL используй JOIN или UNION ALL по date.
    5. После SQL и plot можно добавить 1-2 предложения комментария на русском.
    6. Поле date хранится как text в формате YY-MM-DD → всегда кастуй к date: WHERE date::date >= CURRENT_DATE - INTERVAL '30 days', в SELECT date::date для группировок, в UNION ALL давай AS для колонок.
    7. Блок plot всегда в тройных обратных кавычках: ```plot type=...```
    
    Пример ответа на запрос "Построй график close Amazon и Apple за последний месяц":
    ```sql
    SELECT a.date::date AS date, a.close AS close_amzn, p.close AS close_aapl 
    FROM amazon a 
    JOIN apple p ON a.date = p.date 
    WHERE a.date::date >= CURRENT_DATE - INTERVAL '30 days' 
    ORDER BY a.date::date;```
    
    ```plot type=line x=date y=close_amzn,close_aapl title=Сравнение цен закрытия Amazon и Apple за 30 дней```
    
    Вот сравнение цен акций за последний месяц.
    Текущий запрос пользователя: {question}
    """

    try:
        client = OpenAI(base_url=LLM_URL, api_key="not_needed")
        stream = client.chat.completions.create(
            model="local-model",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8,
            max_tokens=5000,
            stream=True
        )

        return stream  # возвращаем генератор для стриминга

    except Exception as e:
        st.error(f"Ошибка соединения с LLM: {str(e)}")
        return None


def execute_sql(sql):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
            return df
    except Exception as e:
        st.error(f"Ошибка выполнения SQL:\n{str(e)}")
        return None


def display_table(df):
    if df.empty:
        st.info("Запрос выполнен, но данные не найдены")
    else:
        st.success(f"Найдено строк: {len(df):,}")
        st.dataframe(df.head(1000))


def display_plot(df, plot_params):
    try:
        plot_type = plot_params.get('type', 'line')
        x = plot_params.get('x')
        y = plot_params.get('y')
        color = plot_params.get('color')
        title = plot_params.get('title', 'График данных')

        if isinstance(y, str) and ',' in y:
            y = [col.strip() for col in y.split(',')]

        if plot_type == 'bar':
            fig = px.bar(df, x=x, y=y, color=color, title=title)
        elif plot_type == 'line':
            fig = px.line(df, x=x, y=y, color=color, title=title)
        elif plot_type == 'histogram':
            fig = px.histogram(df, x=x, y=y, color=color, title=title)
        elif plot_type == 'scatter':
            fig = px.scatter(df, x=x, y=y, color=color, title=title)
        else:
            fig = px.line(df, x=x, y=y, color=color, title=title)

        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.warning(f"Не удалось построить график: {str(e)}. Параметры: {plot_params}")


st.set_page_config(
    page_title="Ассистент по акциям 2025",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

with st.sidebar:
    st.title("Ассистент по акциям 2025")
    st.markdown("### Навигация")
    # Кнопки без лишних украшений, как в примере
    if st.button("Загрузка данных", use_container_width=True):
        st.session_state.page = "upload"
        st.rerun()
    if st.button("Чат-ассистент", use_container_width=True):
        st.session_state.page = "chat"
        st.rerun()
    st.divider()
    st.markdown(
        """
        <small>
        Данные акций топ 15 компаний за 2025 год<br>
        Партизанск ПГТ Инкорпорейтед
        </small>
        """,
        unsafe_allow_html=True
    )

if "page" not in st.session_state:
    st.session_state["page"] = "chat"

if st.session_state["page"] == "upload":
    st.header("Загрузка данных в базу")
    st.markdown("Загружайте CSV-файлы с котировками акций за 2025 год")

    try:
        with engine.connect():
            st.success("Подключение к базе данных успешно ✓")
    except Exception as e:
        st.error(f"Ошибка подключения к базе:\n{str(e)}")
        st.stop()

    tables = get_existing_tables()
    if tables:
        st.subheader("Существующие таблицы")
        st.write(", ".join(f"`{t}`" for t in tables))
    else:
        st.info("Пока нет таблиц в схеме public")

    st.subheader("Загрузить CSV-файл")
    uploaded_file = st.file_uploader("Выберите файл", type=["csv"])

    if uploaded_file:
        default_table_name = Path(uploaded_file.name).stem.lower().replace(" ", "_").replace("-", "_")
        table_name = st.text_input(
            "Имя таблицы в базе",
            value=default_table_name,
            max_chars=63
        ).strip().lower()

        if table_name:
            mode = st.radio(
                "Если таблица уже существует:",
                ["replace (заменить)", "append (добавить)", "fail (ошибка)"],
                horizontal=True
            )
            if_exists_map = {
                "replace (заменить)": "replace",
                "append (добавить)": "append",
                "fail (ошибка)": "fail"
            }

            if st.button("Загрузить данные", type="primary"):
                upload_csv_to_table(
                    uploaded_file,
                    table_name,
                    if_exists=if_exists_map[mode]
                )

elif st.session_state["page"] == "chat":
    st.header("Чат-ассистент по акциям 2025 года")
    st.caption("Задавайте вопросы на естественном языке — строим графики, считаем статистику, сравниваем акции")

    chat_container = st.container()

    with chat_container:
        if "messages" not in st.session_state:
            st.session_state.messages = []

        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    if prompt := st.chat_input("Спросите про акции, постройте график, сравните компании..."):
        st.session_state.messages.append({"role": "user", "content": prompt})

        with chat_container:
            with st.chat_message("user"):
                st.markdown(prompt)

        with chat_container:
            with st.chat_message("assistant"):
                message_placeholder = st.empty()
                full_response = ""

                stream = generate_response(prompt)

                if stream:
                    try:
                        for chunk in stream:
                            if chunk.choices[0].delta.content is not None:
                                full_response += chunk.choices[0].delta.content
                                message_placeholder.markdown(full_response + "▌")

                        message_placeholder.markdown(full_response)

                        import re

                        sql_match = re.search(r'```sql\s*(.*?)\s*```', full_response, re.DOTALL | re.IGNORECASE)
                        sql_query = None
                        df = None

                        if sql_match:
                            sql_query = sql_match.group(1).strip()
                            full_response = re.sub(r'```sql\s*(.*?)\s*```', '', full_response,
                                                   flags=re.DOTALL | re.IGNORECASE).strip()

                            df = execute_sql(sql_query)

                        plot_match = re.search(r'(?:```plot|plot)\s+(.+?)(?:\s*```|$)', full_response,
                                               re.DOTALL | re.IGNORECASE | re.MULTILINE)
                        plot_params = {}

                        if plot_match:
                            plot_str = plot_match.group(1).strip()
                            full_response = re.sub(r'(?:```plot|plot)\s+.+?(?:\s*```|\n|$)', '', full_response,
                                                   flags=re.DOTALL | re.IGNORECASE).strip()

                            parts = re.split(r'\s+(?=\w+=)', plot_str)
                            for part in parts:
                                if '=' in part:
                                    key, value = part.split('=', 1)
                                    plot_params[key.strip()] = value.strip()


                        if full_response:
                            message_placeholder.markdown(full_response)

                        if df is not None:
                            display_table(df)

                            if plot_params:
                                display_plot(df, plot_params)

                        st.session_state.messages.append({"role": "assistant", "content": full_response})

                    except Exception as e:
                        message_placeholder.error(f"Ошибка обработки ответа: {str(e)}")
                else:
                    message_placeholder.warning("Не удалось получить ответ от модели.")