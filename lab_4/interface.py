import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import io

# Настройки страницы
st.set_page_config(
    page_title="СПБ Данные",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Стили CSS для улучшения вида
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .data-table {
        font-size: 0.9rem;
    }
    .stButton>button {
        background-color: #1E3A8A;
        color: white;
    }
</style>
""", unsafe_allow_html=True)


# Функция подключения к БД
@st.cache_resource
def get_db_connection():
    """Создает подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=st.secrets["DB_HOST"],
            port=st.secrets["DB_PORT"],
            database=st.secrets["DB_NAME"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASSWORD"]
        )
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к БД: {e}")
        return None


def load_data_from_db():
    """Загружает данные из базы данных"""
    conn = get_db_connection()
    if conn:
        try:
            query = "SELECT * FROM trade_data ORDER BY \"Дата\" DESC"
            df = pd.read_sql_query(query, conn)

            # Преобразуем дату из типа date в datetime
            if 'Дата' in df.columns:
                df['Дата'] = pd.to_datetime(df['Дата']).dt.date

            return df
        except Exception as e:
            st.error(f"Ошибка загрузки данных: {e}")
            return pd.DataFrame()
    return pd.DataFrame()


def filter_data(df, filters):
    """Фильтрует данные по заданным критериям"""
    filtered_df = df.copy()

    # Фильтр по дате
    if filters['start_date'] and filters['end_date']:
        # Приведем все к datetime для единообразного сравнения
        start_date = pd.to_datetime(filters['start_date'])
        end_date = pd.to_datetime(filters['end_date'])

        # Преобразуем даты в DataFrame к datetime
        filtered_df['Дата_dt'] = pd.to_datetime(filtered_df['Дата'])

        # Сравниваем как datetime (но учитываем только дату)
        mask = (filtered_df['Дата_dt'] >= start_date) & \
               (filtered_df['Дата_dt'] <= (end_date + pd.Timedelta(days=1)))
        filtered_df = filtered_df[mask]
        filtered_df = filtered_df.drop('Дата_dt', axis=1)  # Удаляем вспомогательную колонку

    # Фильтр по инструментам
    if filters['selected_instruments']:
        filtered_df = filtered_df[filtered_df['КодИнструмента'].isin(filters['selected_instruments'])]

    # Фильтр по типу товара
    if filters['selected_products']:
        filtered_df = filtered_df[filtered_df['Товар'].isin(filters['selected_products'])]

    # Фильтр по диапазону цен
    if filters['min_price'] is not None:
        filtered_df = filtered_df[filtered_df['СреднЦена'] >= filters['min_price']]
    if filters['max_price'] is not None:
        filtered_df = filtered_df[filtered_df['СреднЦена'] <= filters['max_price']]

    return filtered_df


def export_to_excel(df):
    """Экспортирует DataFrame в Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='СПБ_Данные')
    return output.getvalue()


def export_to_csv(df):
    """Экспортирует DataFrame в CSV"""
    return df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')


# Основное приложение
def main():
    # Заголовок
    st.markdown('<h1 class="main-header">📊 СПБ Данные - Анализ торгов</h1>', unsafe_allow_html=True)

    # Загрузка данных
    with st.spinner('Загрузка данных из базы...'):
        df = load_data_from_db()

    if df.empty:
        st.warning("Нет данных для отображения")
        return

    # Боковая панель с фильтрами
    with st.sidebar:
        st.header("🔍 Фильтры")

        # Фильтр по дате
        st.subheader("Период дат")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Начало",
                value=datetime.now() - timedelta(days=30),
                max_value=datetime.now()
            )
        with col2:
            end_date = st.date_input(
                "Конец",
                value=datetime.now(),
                max_value=datetime.now()
            )

        # Фильтр по инструментам
        st.subheader("Инструменты")
        all_instruments = sorted(df['КодИнструмента'].unique())
        selected_instruments = st.multiselect(
            "Выберите инструменты",
            options=all_instruments,
            default=all_instruments[:5] if len(all_instruments) > 5 else all_instruments
        )

        # Фильтр по типу товара
        st.subheader("Тип товара")
        all_products = sorted(df['Товар'].unique())
        selected_products = st.multiselect(
            "Выберите товары",
            options=all_products,
            default=all_products[:5] if len(all_products) > 5 else all_products
        )

        # Фильтр по диапазону цен
        st.subheader("Диапазон цен")
        min_price = st.number_input(
            "Минимальная цена",
            min_value=0.0,
            value=0.0,
            step=1000.0
        )
        max_price = st.number_input(
            "Максимальная цена",
            min_value=0.0,
            value=float(df['СреднЦена'].max()) if not df['СреднЦена'].isnull().all() else 100000.0,
            step=1000.0
        )

        # Кнопка применения фильтров
        apply_filters = st.button("Применить фильтры", type="primary")

    if 'filtered_df' not in st.session_state:
        st.session_state.filtered_df = df.copy()

    if apply_filters:
        filtered_df = df.copy()

        # Фильтр по дате
        if start_date and end_date:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            filtered_df['Дата_temp'] = pd.to_datetime(filtered_df['Дата'])
            filtered_df = filtered_df[
                (filtered_df['Дата_temp'] >= start_dt) &
                (filtered_df['Дата_temp'] <= end_dt)
                ].drop(columns=['Дата_temp'])

        # Фильтр по инструментам
        if selected_instruments:
            filtered_df = filtered_df[filtered_df['КодИнструмента'].isin(selected_instruments)]

        # Фильтр по товарам
        if selected_products:
            filtered_df = filtered_df[filtered_df['Товар'].isin(selected_products)]

        # Фильтр по ценам
        if min_price > 0:
            filtered_df = filtered_df[filtered_df['СреднЦена'] >= min_price]
        if max_price > 0 and max_price >= min_price:
            filtered_df = filtered_df[filtered_df['СреднЦена'] <= max_price]

        # Сохраняем результат в session_state
        st.session_state.filtered_df = filtered_df

    # Используем сохранённый результат (или исходные данные, если фильтр ещё не применяли)
    filtered_df = st.session_state.filtered_df

    # Показываем сколько записей осталось
    st.info(f"Найдено записей после фильтров: **{len(filtered_df)}**")
    # ───────────────────────────────────────────────────────────────

    # Панель статистики
    st.subheader("📈 Статистика")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Всего записей", len(filtered_df))
    with col2:
        st.metric("Уникальных инструментов", filtered_df['КодИнструмента'].nunique())
    with col3:
        avg_price = filtered_df['СреднЦена'].mean()
        st.metric("Средняя цена", f"{avg_price:,.0f} ₽" if not pd.isna(avg_price) else "N/A")
    with col4:
        total_volume = filtered_df['ОбъемДоговоровРуб'].sum()
        st.metric("Общий объем", f"{total_volume:,.0f} ₽" if not pd.isna(total_volume) else "N/A")

    # Таблица с данными
    st.subheader("📋 Данные")

    # Настройки отображения таблицы
    col1, col2 = st.columns([3, 1])
    with col2:
        # Выбор колонок для отображения
        all_columns = list(filtered_df.columns)
        default_columns = ['Дата', 'КодИнструмента', 'Товар', 'СреднЦена', 'ОбъемДоговоровРуб']
        visible_columns = st.multiselect(
            "Показать колонки",
            options=all_columns,
            default=default_columns
        )

    if visible_columns:
        display_df = filtered_df[visible_columns]
    else:
        display_df = filtered_df

    # Сортировка
    sort_column = st.selectbox(
        "Сортировать по",
        options=display_df.columns,
        index=0
    )
    sort_ascending = st.checkbox("По возрастанию", value=False)

    display_df = display_df.sort_values(by=sort_column, ascending=sort_ascending)

    # Отображение таблицы
    st.dataframe(
        display_df,
        use_container_width=True,
        height=400
    )

    # Экспорт данных
    st.subheader("📤 Экспорт данных")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("📊 Экспорт в Excel", use_container_width=True):
            excel_data = export_to_excel(filtered_df)
            st.download_button(
                label="⬇️ Скачать Excel файл",
                data=excel_data,
                file_name=f"spimex_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    with col2:
        if st.button("📄 Экспорт в CSV", use_container_width=True):
            csv_data = export_to_csv(filtered_df)
            st.download_button(
                label="⬇️ Скачать CSV файл",
                data=csv_data,
                file_name=f"spimex_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )

if __name__ == "__main__":
    main()