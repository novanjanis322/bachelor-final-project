import pandas as pd

def csv_to_xlsx(csv_file, xlsx_file):
    # Read CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
    # Write DataFrame to Excel file
    df.to_excel(xlsx_file, index=False)

# Example usage:
csv_file = "Tiktok_prambanan1.csv"  # Replace with your CSV file path
xlsx_file = "Tiktok_prambanan1.xlsx"  # Replace with desired XLSX file path
csv_to_xlsx(csv_file, xlsx_file)
