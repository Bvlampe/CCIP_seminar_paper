import pandas as pd
import numpy as np
import sys

loc_homicides = "homicides.csv"
loc_ged = "GED_cleaned.csv"
loc_conflict_end = "ucdp-term-acd-3-2021.csv"
loc_conflict_all = "ucdp-prio-acd-221.csv"
loc_merged_conflicts = "merged_conflicts.csv"
loc_concordance = "Country_names.csv"


def avg_years(values):
    try:
        for i in range(len(values)):
            if isinstance(values[i], str):
                values[i] = float(values[i].replace(',', '.'))
        for i in range(len(values)):
            if not np.isnan(values[i]):
                return np.nanmean(values)
        return np.nan
    except:
        print(values)
        print([type(x) for x in values])
        sys.exit("Error in averaging the homicide rates for the following series:")


def main():
    # Read datasets
    homicides = pd.read_csv(loc_homicides)
    homicides.drop(columns=homicides.columns[0], axis=1, inplace=True)
    ged = pd.read_csv(loc_ged)
    conflict_end = pd.read_csv(loc_conflict_end)
    ged_active = ged[ged["active_year"] == 1]
    conflict_all = pd.read_csv(loc_conflict_all)

    conflict_new = conflict_all[["conflict_id", "start_date2", "ep_end", "ep_end_date", "year"]]

    # Mark the rows of conflicts that are over
    df = conflict_new.loc[:, ["conflict_id", "start_date2", "ep_end"]].groupby(["conflict_id", "start_date2"]).sum()
    df.reset_index(inplace=True)
    df.rename(columns={"ep_end": "has_ended"}, inplace=True)
    conflict_new = conflict_new.merge(df, left_on=["conflict_id", "start_date2"],
                                      right_on=["conflict_id", "start_date2"])

    # Remove presently ongoing conflicts
    conflict_new = conflict_new[conflict_new["has_ended"] == 1]

    # Fill end date for all conflict-years, add start and end year as well as duration
    conflict_new["ep_end_date"].fillna(method="bfill", inplace=True)
    conflict_new["start_year"] = conflict_new["start_date2"].str[:4].astype(int)
    conflict_new["end_year"] = conflict_new["ep_end_date"].str[:4].astype(int)
    conflict_new["duration"] = conflict_new["end_year"].astype(int) - conflict_new["start_year"].astype(int) + 1

    # Add up deaths per CCY triad
    ccy_merge = ged_active.loc[:, ["country", "conflict_new_id", "year", "best"]].groupby(
        by=["country", "conflict_new_id", "year"]).sum()

    ccy_merge.reset_index(inplace=True)

    ccy_merge.rename(columns={"conflict_new_id": "conflict_id"}, inplace=True)
    ccy_complete = ccy_merge.merge(conflict_new, left_on=["conflict_id", "year"], right_on=["conflict_id", "year"])
    # ccy_complete.to_csv(loc_merged_conflicts)

    # Compact CCY into CC, removing the one line per year attribute
    cc_iv = ccy_complete.loc[:, ["country", "conflict_id", "start_year", "end_year", "duration", "best"]].groupby(
        by=["country", "conflict_id", "start_year", "end_year", "duration"]).sum().reset_index()
    cc_iv["avg_deaths"] = cc_iv["best"] / cc_iv["duration"]

    # Remove conflicts that started before 1965 or only just ended
    cc_iv.drop(cc_iv[cc_iv.start_year < 1965].index, inplace=True)
    cc_iv.drop(cc_iv[cc_iv.end_year > 2020].index, inplace=True)

    # Harmonize country names
    country_df = pd.read_csv(loc_concordance, sep=';')
    country_dict = dict(zip(list(country_df["cc_iv"]), list(country_df["homicides"])))
    cc_iv.replace({"country": country_dict}, inplace=True)

    # Create DV columns
    cc_iv["HR_before"] = None
    cc_iv["HR_after"] = None

    # Insert values for DV columns
    for i in cc_iv.index:  # cc_iv.shape[0]
        country = cc_iv.loc[i, "country"]
        start_year = int(cc_iv.loc[i, "start_year"])
        end_year = int(cc_iv.loc[i, "end_year"])

        homicides_row = homicides.loc[homicides["Country Name"] == country, :]
        rates_before = [homicides_row.loc[:, str(y)].values[0] for y in range(start_year - 5, start_year)]
        rates_after = [homicides_row.loc[:, str(y)].values[0] for y in range(end_year + 1, min(end_year + 6, 2022))]

        avg_before = avg_years(rates_before)
        avg_after = avg_years(rates_after)

        cc_iv.loc[i, "HR_before"] = avg_before
        cc_iv.loc[i, "HR_after"] = avg_after

    # Ensure numeric format
    cc_iv["HR_before"] = cc_iv["HR_before"].astype(float)
    cc_iv["HR_after"] = cc_iv["HR_after"].astype(float)

    # Drop rows that don't have both before and after values for the DV
    cc_iv.dropna(subset=["HR_before", "HR_after"], inplace=True)

    # Add DV as ration of HR_after and HR_before
    cc_ivdv = cc_iv
    cc_ivdv["HR_rel_change"] = cc_ivdv["HR_after"] / cc_ivdv["HR_before"]

    # Descriptive statistics
    print(cc_ivdv.info())
    print(cc_ivdv.describe(include='all'))
    cc_ivdv.describe(include='all').to_csv("descriptive_stats.csv", sep=';', decimal=',',)

    return 0


if __name__ == "__main__":
    main()
