import pandas as pd

document = False

loc_homicides = "homicides.csv"
loc_ged = "GED_cleaned.csv"
loc_conflict_end = "ucdp-term-acd-3-2021.csv"
loc_conflict_all = "ucdp-prio-acd-221.csv"
loc_merged_conflicts = "merged_conflicts.xlsx"


def main():
    # Read datasets
    homicides = pd.read_csv(loc_homicides)
    ged = pd.read_csv(loc_ged)
    conflict_end = pd.read_csv(loc_conflict_end)
    if document: print("Shape of GED before cleaning up inactive year events:", ged.shape)
    ged_active = ged[ged["active_year"] == 1]
    if document: print("Shape of GED after cleaning up inactive year events:", ged_active.shape)
    conflict_all = pd.read_csv(loc_conflict_all)

    # Filter the ACD to exclude rows of non-ending conflicts to avoid doubles
    conflict_new = conflict_all[["conflict_id", "start_date2", "ep_end", "ep_end_date", "year"]]

    # Mark the rows of conflicts that are over
    df = conflict_new.loc[:, ["conflict_id", "start_date2", "ep_end"]].groupby(["conflict_id", "start_date2"]).sum()
    df.reset_index(inplace=True)
    df.rename(columns={"ep_end":"has_ended"}, inplace=True)
    conflict_new = conflict_new.merge(df, left_on=["conflict_id", "start_date2"], right_on=["conflict_id", "start_date2"])

    # Remove presently ongoing conflicts
    conflict_new = conflict_new[conflict_new["has_ended"] == 1]

    # Fill end date
    conflict_new["ep_end_date"].fillna(method = "bfill", inplace=True)
    print(conflict_new.head(10))

    # conflict_new = conflict_new[conflict_new["ep_end_date"].notna()]
    conflict_new["start_year"] = conflict_new["start_date2"].str[:4]
    conflict_new["end_year"] = conflict_new["ep_end_date"].str[:4]
    conflict_new["duration"] = conflict_new["end_year"].astype(int) - conflict_new["start_year"].astype(int) + 1

    # Add up deaths per CCY triad
    ccy_merge = ged_active.loc[:, ["country", "conflict_new_id", "year", "best"]].groupby(
        by=["country", "conflict_new_id", "year"]).sum()

    ccy_merge.reset_index(inplace=True)

    ccy_merge.rename(columns={"conflict_new_id" : "conflict_id"}, inplace=True)
    print(ccy_merge.columns)
    ccy_complete = ccy_merge.merge(conflict_new, left_on=["conflict_id", "year"], right_on=["conflict_id", "year"])
    print(ccy_complete.head(20))
    print(ccy_complete.columns)

    ccy_complete.to_excel(loc_merged_conflicts)

    # ged_active.to_csv(loc_ged)
    return 0


if __name__ == "__main__":
    main()
