import pandas as pd


def load_data(num_rows: int = None, path: str = None) -> pd.DataFrame:
    try:
        if num_rows:
            print(f"Load {num_rows:} rows of data...")
            df = pd.read_csv(path, nrows=num_rows, low_memory=False)
        else:
            print("Load all data...")
            df = pd.read_csv(path, low_memory=False)

        print(f"Data loaded successfully!")
        return df

    except FileNotFoundError:
        raise FileNotFoundError(f"File scv not found in path: {path}")


def get_top_tanks_in_tears(df: pd.DataFrame, top_size: int = 10) -> pd.DataFrame:
    tank_count = (df.groupby(["tier", "name"])
              .size()
              .div(get_total_battles(df))
              .round(2)
              .reset_index(name="counts"))

    top_tanks_sort = (tank_count.sort_values(['tier', 'counts'], ascending=[True, False])
                      .groupby('tier')
                      .head(top_size))

    return top_tanks_sort


def get_light_tanks_spotting_asist(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    lt_df = df[df['class'] == 'LT'][['display_name',
                                     'name',
                                     'tier',
                                     'spawn',
                                     'spotting_assist',
                                     'battle_time']].copy()
    # лучший засвет по боям
    best_battle_spot = (lt_df.groupby('battle_time')['spotting_assist'].max()
                        .reset_index(name='max_spot'))
    # делаем таблицу боев и их карт
    best_on_map = lt_df[['battle_time', 'display_name']].drop_duplicates()
    # свяжем с лучшим засветом на каждой карте
    best_spot_on_map = best_on_map.merge(
        best_battle_spot,
        on='battle_time'
    )

    average_maps_spot = (best_spot_on_map.groupby('display_name')['max_spot']
                        .mean()
                        .reset_index()
                        .sort_values('max_spot', ascending=False)
                        .head(top_n)
                        .reset_index(drop=True))

    return average_maps_spot


def get_tanks_max_average_damage(df: pd.DataFrame, tank_type: str, top_n: int = 10) -> pd.DataFrame:
    tank_class = df[df['class'] == tank_type].copy()

    avg_tank_damage = (tank_class.groupby(['name', 'tier'])['damage']
                       .mean()
                       .sort_values(ascending=False)
                       .head(top_n)
                       .reset_index())

    avg_tank_damage.columns = ['name', 'tier', 'avg_damage']
    avg_tank_damage['type'] = tank_type
    return avg_tank_damage.round(0)


def get_max_average_damage_in_types(df: pd.DataFrame, tank_types: list[str]) -> pd.DataFrame:
    res_data = []

    for t_type in tank_types:
        tank_df = get_tanks_max_average_damage(df, t_type, 1)
        res_data.append(tank_df)

    if res_data:
        return (pd.concat(res_data)
                .sort_values(by=['avg_damage'], ascending=False)
                .reset_index(drop=True))
    else:
        return pd.DataFrame(columns=['name', 'tier', 'avg_damage', 'type'])


def get_total_battles(df: pd.DataFrame) -> int:
    return df['battle_time'].nunique()


def get_total_tanks(df: pd.DataFrame) -> int:
    return df['name'].nunique()

