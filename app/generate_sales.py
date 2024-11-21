import pandas as pd
import numpy as np



data = {
    'sale_id': range(1, 1000001),
    'customer_id': np.random.randint(1, 100001, size=1000000),
    'product_id': np.random.randint(1, 10001, size=1000000),
    'quantity': np.random.randint(1, 20, size=1000000),
    'sale_date': pd.date_range(start='1/1/2022', periods=1000000, freq='H'),
    'region': np.random.choice(['North', 'South', 'East', 'West'], size=1000000),
}

df = pd.DataFrame(data)
df[:10]

df.drop_duplicates(inplace=True)

# Агрегация данных
# aggregated_data = df.groupby(['region', 'product_id']).agg(
#     total_sales=('quantity', 'sum'),
#     average_sale_amount=('quantity', 'mean'),
# ).reset_index()

# aggregated_data[:10]
df.to_csv('sales_data.csv', index=False)