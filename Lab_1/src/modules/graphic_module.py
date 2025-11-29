import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def show_top_tanks_by_tier(top_tanks: pd.DataFrame, top_n: int = 5):
    sns.set_style('darkgrid')

    for tier in range(8, 12):
        data = top_tanks[top_tanks['tier'] == tier].head(top_n)
        plt.figure(figsize=(10, 5))
        ax = sns.barplot(x='counts', y='name', data=data)

        for i, (index, row) in enumerate(data.iterrows()):
            ax.text(row['counts'] / 2, i, str(row['counts']) ,
                    ha='center',
                    va='center',
                    fontweight='bold',
                    color='black')

        plt.title(f"Top {top_n} popular tanks on {tier} tier")
        plt.xlabel("Coefficient in battles")
        plt.ylabel("")
        plt.show()


def show_top_damage_by_detected_maps(top_maps: pd.DataFrame, top_n: int = 10):
    sns.set_style('darkgrid')
    plt.figure(figsize=(12, 8))
    ax = sns.barplot(data=top_maps,
                     x='max_spot',
                     y='display_name',
                     hue='display_name',
                     palette='viridis')

    for i, (index, row) in enumerate(top_maps.iterrows()):
        ax.text(row['max_spot'] / 2, i,
                f"{row['max_spot']:.0f}",
                va='center',
                ha='center',
                fontweight='bold',
                color='black')

    plt.title(f'Top {top_n} maps for damage by detected on LT',
              fontsize=16,
              fontweight='bold')
    plt.xlabel('Average max damage for detected')
    plt.ylabel('Map')
    plt.tight_layout()
    plt.show()


def show_top10_damage_tanks(df: pd.DataFrame, class_type: str):
    # формируем подписи
    labels = [f"{row['name']} (LVL {row['tier']})" for index, row in df.iterrows()]

    plt.figure(figsize=(12, 6))
    plt.plot(labels, df['avg_damage'],
             marker='o',
             linewidth=5,
             markersize=10,
             color='green',
             markerfacecolor='red')

    for i, dmg in enumerate(df['avg_damage']):
        plt.text(i, dmg + 30, f"{int(dmg):.0f}",
                 ha='center',
                 va='bottom',
                 fontsize=14,
                 fontweight='bold')

    plt.title(f'{class_type} — Top 10 by avg damage',
              fontsize=14,
              fontweight='bold')
    plt.xlabel('')
    plt.ylabel('Avg damage in battle')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()


def show_top_tank_types_by_damage(top_tanks: pd.DataFrame):
    sns.set_style('darkgrid')
    plt.figure(figsize=(12, 6))

    labels = [f"{row['name']} - ({row['type']}" +
              f"{row['tier']})" for index, row in top_tanks.iterrows()]

    ax = sns.barplot(x='avg_damage',
                     y=labels,
                     data=top_tanks,
                     hue='name',
                     legend=False,
                     palette='viridis')

    for i, (index, row) in enumerate(top_tanks.iterrows()):
        ax.text(row['avg_damage'] / 2, i, f"{row['avg_damage']:.0f}",
                ha='center',
                va='center',
                fontweight='bold',
                color='white')

    plt.title(f"Top tank types by avg damage among MT, HT and TD")
    plt.xlabel("Avg damage")
    plt.ylabel("")
    plt.show()
