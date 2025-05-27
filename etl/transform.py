import pandas as pd
import numpy as np

def ordinal(n):
    n = int(n)
    if 10 <= n % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return f"{n}{suffix}"

def generate_blurb(row, metric, rank_col, is_lowest_better=False):
    state_name = row['key_row'].replace('_', ' ').title()
    rank = row[rank_col]
    if rank == 1:
        if is_lowest_better:
            return f"{state_name} has the single lowest {metric} in the nation among states, DC, and Puerto Rico."
        else:
            return f"{state_name} has the single highest {metric} in the nation among states, DC, and Puerto Rico."
    elif rank == row['total']:
        if is_lowest_better:
            return f"{state_name} has the single highest {metric} in the nation among states, DC, and Puerto Rico."
        else:
            return f"{state_name} has the single lowest {metric} in the nation among states, DC, and Puerto Rico."
    else:
        ord_rank = ordinal(rank)
        if is_lowest_better:
            return f"{state_name} has the {ord_rank} lowest {metric} in the nation among states, DC, and Puerto Rico."
        else:
            return f"{state_name} has the {ord_rank} highest {metric} in the nation among states, DC, and Puerto Rico."

def main():
    pop_df = pd.read_csv('data/staged/population.csv')
    mhi_df = pd.read_csv('data/staged/income.csv')
    sale_df = pd.read_csv('data/staged/sale_price.csv')

    df = pop_df.merge(mhi_df, on='key_row').merge(sale_df, on='key_row')
    df = df.dropna(subset=['census_population', 'median_household_income', 'median_sale_price'])

    df['population_rank'] = df['census_population'].rank(ascending=False, method='min').astype(int)
    df['median_household_income_rank'] = df['median_household_income'].rank(ascending=False, method='min').astype(int)
    df['median_sale_price_rank'] = df['median_sale_price'].rank(ascending=False, method='min').astype(int)
    df['house_affordability_ratio'] = (df['median_sale_price'] / df['median_household_income']).round(1)
    df['house_affordability_ratio_rank'] = df['house_affordability_ratio'].rank(ascending=True, method='min').astype(int)
    df['total'] = len(df)

    df['population_blurb'] = df.apply(lambda row: f"{row['key_row'].replace('_', ' ').title()} is {ordinal(row['population_rank'])} in the nation in population among states, DC, and Puerto Rico.", axis=1)
    df['median_household_income_blurb'] = df.apply(lambda row: f"{row['key_row'].replace('_', ' ').title()} is {ordinal(row['median_household_income_rank'])} in the nation in median household income among states, DC, and Puerto Rico.", axis=1)
    df['median_sale_price_blurb'] = df.apply(lambda row: generate_blurb(row, 'median sale price on homes', 'median_sale_price_rank') + ", according to Redfin data from " + " ".join(df.columns[-3].split()[:2]) + ".", axis=1)
    df['house_affordability_ratio_blurb'] = df.apply(lambda row: generate_blurb(row, 'house affordability ratio', 'house_affordability_ratio_rank', is_lowest_better=True) + ", according to Redfin data from " + " ".join(df.columns[-3].split()[:2]) + ".", axis=1)

    df['population_rank'] = df['population_rank'].apply(ordinal)
    df['median_household_income_rank'] = df['median_household_income_rank'].apply(ordinal)
    df['median_sale_price_rank'] = df['median_sale_price_rank'].apply(ordinal)
    df['house_affordability_ratio_rank'] = df['house_affordability_ratio_rank'].apply(ordinal)

    output_cols = [
        'key_row',
        'census_population', 'population_rank', 'population_blurb',
        'median_household_income', 'median_household_income_rank', 'median_household_income_blurb',
        'median_sale_price', 'median_sale_price_rank', 'median_sale_price_blurb',
        'house_affordability_ratio', 'house_affordability_ratio_rank', 'house_affordability_ratio_blurb'
    ]
    df_out = df[output_cols]
    df_out.to_csv('data/output/output.csv', index=False)
    print(f"Output written to data/output/output.csv with {df_out.shape[0]} rows.")

if __name__ == "__main__":
    main()
