import pandas as pd

document = True

loc_folder = "D:/Wichtig/Uni/MSc Political Science/Other materials/CCIP seminar paper datasets/CSV/"

loc_homicides = loc_folder + "homicides.csv"
loc_ged = loc_folder + "GED_cleaned.csv"
loc_conflict_end = loc_folder + "ucdp-term-acd-3-2021.csv"
loc_conflict_all = loc_folder + "ucdp-prio-acd-221.csv"

def main():
    homicides = pd.read_csv(loc_homicides, sep=';')
    ged = pd.read_csv(loc_ged)
    conflict_end = pd.read_csv(loc_conflict_end, sep=';')
    if document: print("Shape of GED before cleaning up inactive year events:", ged.shape)
    ged_active = ged[ged["active_year"] == 1]
    if document: print("Shape of GED after cleaning up inactive year events:", ged_active.shape)
    conflict_all = pd.read_csv(loc_conflict_all, sep=';')

    # conf_end = conflict[conflict["ep_end_date"] == 1]
    # if document: print("Shape of ACD after cleaning up ongoing conflicts:", conf_end.shape)

    conflict_new = conflict_all[["conflict_id", "start_date2", "ep_end", "ep_end_date"]]
    conflict_new = conflict_new[conflict_new["ep_end"] == 1]

    print(conflict_new.head())

    return 0

if __name__ == "__main__":
    main()