import pandas as pd

document = True

loc_folder = "D:/Wichtig/Uni/MSc Political Science/Other materials/CCIP seminar paper datasets/"

loc_homicides = loc_folder + "homicides.csv"
loc_ged = loc_folder + "GED_cleaned.csv"
loc_conflict = loc_folder + "ucdp-term-acd-3-2021.csv"

def main():
    homicides = pd.read_csv(loc_homicides, sep=';')
    ged = pd.read_csv(loc_ged)
    conflict = pd.read_csv(loc_conflict, sep=';')
    if document: print("Shape of GED dataset before cleaning up inactive year events:", ged.shape)
    ged_active = ged[ged["active_year"] == 1]
    if document: print("Shape of GED dataset after cleaning up inactive year events:", ged_active.shape)

    return 0

if __name__ == "__main__":
    main()