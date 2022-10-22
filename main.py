import pandas as pd
import numpy as np

loc_homicides = "homicides.csv"
loc_ged = "GED_cleaned.csv"
loc_conflict_end = "ucdp-term-acd-3-2021.csv"
loc_conflict_all = "ucdp-prio-acd-221.csv"
loc_merged_conflicts = "merged_conflicts.csv"
loc_concordance = "Country_names.csv"


def main():
    # Read datasets

    homicides = pd.read_csv(loc_homicides)
    ged = pd.read_csv(loc_ged)
    conflict_end = pd.read_csv(loc_conflict_end)
    ged_active = ged[ged["active_year"] == 1]
    conflict_all = pd.read_csv(loc_conflict_all)

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

    # conflict_new = conflict_new[conflict_new["ep_end_date"].notna()]
    conflict_new["start_year"] = conflict_new["start_date2"].str[:4]
    conflict_new["end_year"] = conflict_new["ep_end_date"].str[:4]
    conflict_new["duration"] = conflict_new["end_year"].astype(int) - conflict_new["start_year"].astype(int) + 1

    # Add up deaths per CCY triad
    ccy_merge = ged_active.loc[:, ["country", "conflict_new_id", "year", "best"]].groupby(
        by=["country", "conflict_new_id", "year"]).sum()

    ccy_merge.reset_index(inplace=True)

    ccy_merge.rename(columns={"conflict_new_id" : "conflict_id"}, inplace=True)
    ccy_complete = ccy_merge.merge(conflict_new, left_on=["conflict_id", "year"], right_on=["conflict_id", "year"])
    ccy_complete.to_csv(loc_merged_conflicts)

    # Compact CCY into CC, removing the one line per year attribute
    cc_iv = ccy_complete.loc[:, ["country", "conflict_id", "start_year", "end_year", "duration", "best"]].groupby(by=["country", "conflict_id", "start_year", "end_year", "duration"]).sum().reset_index()
    cc_iv["avg_deaths"] = cc_iv["best"]/cc_iv["duration"]

    # Harmonize country names
    country_df = pd.read_csv(loc_concordance, sep=';')
    country_dict = dict(zip(list(country_df["cc_iv"]), list(country_df["homicides"])))
    cc_iv.replace({"country": country_dict}, inplace=True)

    # Check that the country names are compatible
    set_a = set(cc_iv["country"])
    set_b = set(homicides["Country Name"])
    print("CC_IV but not Homicides:", set_a - set_b, ", total: ", len(set_a - set_b))



    return 0


if __name__ == "__main__":
    main()
