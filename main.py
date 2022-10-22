import pandas as pd


document = True

loc_homicides = "homicides.csv"
loc_ged = "GED_cleaned.csv"
loc_conflict_end ="ucdp-term-acd-3-2021.csv"
loc_conflict_all = "ucdp-prio-acd-221.csv"


def ccy_create(row):
    return tuple([row["country"], row["conflict_new_id"], row["year"]])


def main():
    # Read datasets
    homicides = pd.read_csv(loc_homicides, sep=';')
    ged = pd.read_csv(loc_ged)
    conflict_end = pd.read_csv(loc_conflict_end, sep=';')
    if document: print("Shape of GED before cleaning up inactive year events:", ged.shape)
    ged_active = ged[ged["active_year"] == 1]
    if document: print("Shape of GED after cleaning up inactive year events:", ged_active.shape)
    conflict_all = pd.read_csv(loc_conflict_all, sep=';')

    return 0
    # conf_end = conflict[conflict["ep_end_date"] == 1]
    # if document: print("Shape of ACD after cleaning up ongoing conflicts:", conf_end.shape)

    conflict_new = conflict_all[["conflict_id", "start_date2", "ep_end", "ep_end_date"]]
    conflict_new = conflict_new[conflict_new["ep_end"] == 1]

    # Prep the GED dataset for merging all events with the same CCY into one
    ged_active["CCY"] = ged_active.apply(lambda row: ccy_create(row), axis=1)
    ccy_merge = pd.DataFrame(ged_active["CCY"].unique())
    ccy_merge.rename(columns={0 : "CCY"}, inplace=True)
    ccy_merge["deaths"] = 0

    # Merge GED deaths by CCY
    for row in ged_active.iterrows():
        found = ccy_merge.loc[ccy_merge["CCY"] == row["CCY"]]

    print(ccy_merge.head())

    # ged_active.to_csv(loc_ged)
    return 0

if __name__ == "__main__":
    main()