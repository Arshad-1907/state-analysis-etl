import pandas as pd
import numpy as np

def extract_population(keys):
    pop = pd.read_csv('data/raw/CENSUS_POPULATION_STATE.tsv', sep='\t')
    pop_data = []
    for _, row in keys.iterrows():
        col = f"{row['census_msa']}!!Estimate"
        try:
            val = pop.loc[1, col]
            val = int(str(val).replace(',', ''))
            pop_data.append({'key_row': row['key_row'], 'census_population': val})
        except Exception:
            pop_data.append({'key_row': row['key_row'], 'census_population': np.nan})
    return pd.DataFrame(pop_data)

def extract_income(keys):
    income = pd.read_csv('data/raw/CENSUS_MHI_STATE.csv')
    households_row = income[income['Label (Grouping)'].str.strip().str.lower() == 'households']
    households_row = households_row.iloc[0]
    mhi_data = []
    for _, row in keys.iterrows():
        col = f"{row['census_msa']}!!Median income (dollars)!!Estimate"
        try:
            val = households_row[col]
            val = int(str(val).replace(',', '').replace('$', ''))
            mhi_data.append({'key_row': row['key_row'], 'median_household_income': val})
        except Exception:
            mhi_data.append({'key_row': row['key_row'], 'median_household_income': np.nan})
    return pd.DataFrame(mhi_data)

def extract_sale_price(keys):
    sale = pd.read_csv('data/raw/REDFIN_MEDIAN_SALE_PRICE.csv', header=1)
    sale.rename(columns={sale.columns[0]: 'redfin_region'}, inplace=True)
    # Dynamically pick the latest month column
    month_cols = [col for col in sale.columns if col != 'redfin_region']
    latest_month_col = month_cols[-1]
    print(f"Processing Redfin sale price for: {latest_month_col}")
    sale_data = []
    for _, row in keys.iterrows():
        region = 'Puerto Rico' if row['key_row'] == 'puerto_rico' else str(row['redfin_region'])
        try:
            val = sale[sale['redfin_region'] == region][latest_month_col].values
            if len(val) == 0:
                price = np.nan
            else:
                price = val[0]
                if pd.isna(price) or price == '-':
                    price = np.nan
                else:
                    s = str(price).replace('$', '').replace(',', '')
                    if s.endswith('K'):
                        price = float(s[:-1]) * 1000
                    else:
                        price = float(s)
            sale_data.append({'key_row': row['key_row'], 'median_sale_price': price})
        except Exception:
            sale_data.append({'key_row': row['key_row'], 'median_sale_price': np.nan})
    # Patch Puerto Rico as required by the assignment
    if not (pd.Series([d['key_row'] for d in sale_data]) == 'puerto_rico').any():
        sale_data.append({'key_row': 'puerto_rico', 'median_sale_price': 138000.0})
    else:
        for d in sale_data:
            if d['key_row'] == 'puerto_rico':
                d['median_sale_price'] = 138000.0
    return pd.DataFrame(sale_data)

def main():
    keys = pd.read_csv('data/raw/KEYS.csv')
    keys = keys[(keys['region_type'] == 'state') | (keys['key_row'].isin(['washington_dc', 'puerto_rico']))].drop_duplicates(subset=['key_row']).reset_index(drop=True)
    pop_df = extract_population(keys)
    mhi_df = extract_income(keys)
    sale_df = extract_sale_price(keys)
    pop_df.to_csv('data/staged/population.csv', index=False)
    mhi_df.to_csv('data/staged/income.csv', index=False)
    sale_df.to_csv('data/staged/sale_price.csv', index=False)
    print("Extraction complete and staged files written.")

if __name__ == "__main__":
    main()
