from src.modules.data_module import *
from src.modules.graphic_module import *


def main():
    wot_data = load_data(None,"src/data/wot.csv")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(wot_data.head(5))
    wot_data.info()
    print(f"Total battles - {get_total_battles(wot_data)}")
    print(f"Total tanks - {get_total_tanks(wot_data)}")
    show_top_tanks_by_tier(get_top_tanks_in_tears(wot_data), 10)
    show_top_damage_by_detected_maps(get_light_tanks_spotting_asist(wot_data),10)
    show_top10_damage_tanks(get_tanks_max_average_damage(wot_data,"MT",10), "MT")
    show_top10_damage_tanks(get_tanks_max_average_damage(wot_data,"HT",10), "HT")
    show_top10_damage_tanks(get_tanks_max_average_damage(wot_data,"TD",10), "TD")
    show_top_tank_types_by_damage(get_max_average_damage_in_types(wot_data,["MT","HT","TD"]))
    return


if __name__ == "__main__":
    main()