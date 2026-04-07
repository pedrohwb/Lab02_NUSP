from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from config import RAW_DIR, SILVER_DIR, DOCS_DIR

SOURCE_FILE_NAME = "online_retail_ii.xlsx"
RAW_FILE_PATH = RAW_DIR / SOURCE_FILE_NAME

SILVER_FILE_PATH = SILVER_DIR / "online_retail_ii_silver.parquet"
NULL_REPORT_PATH = SILVER_DIR / "silver_null_report.csv"
STATS_REPORT_PATH = SILVER_DIR / "silver_descriptive_stats.csv"
QUALITY_REPORT_PATH = SILVER_DIR / "silver_quality_report.txt"
DATA_DICTIONARY_PATH = SILVER_DIR / "silver_data_dictionary.csv"

PLOTS_DIR = DOCS_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

GRAFICOS_MD_PATH = DOCS_DIR / "graficos.md"


def load_raw_data() -> pd.DataFrame:
    sheets_dict = pd.read_excel(RAW_FILE_PATH, sheet_name=None)

    frames = []
    for sheet_name, df in sheets_dict.items():
        df["source_sheet"] = sheet_name
        frames.append(df)

    df_raw = pd.concat(frames, ignore_index=True)
    return df_raw


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.rename(columns={
        "Invoice": "invoice",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "Price": "price",
        "Customer ID": "customer_id",
        "Country": "country",
        "source_sheet": "source_sheet",
    })
    return df


def generate_initial_reports(df: pd.DataFrame) -> None:
    null_report = pd.DataFrame({
        "column_name": df.columns,
        "null_count": df.isnull().sum().values,
        "null_percentage": (df.isnull().mean() * 100).round(2).values,
        "dtype": df.dtypes.astype(str).values
    })
    null_report.to_csv(NULL_REPORT_PATH, index=False, encoding="utf-8")

    descriptive_stats = df.describe(include="all").transpose()
    descriptive_stats.to_csv(STATS_REPORT_PATH, encoding="utf-8")


def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    string_cols = ["invoice", "stock_code", "description", "customer_id", "country", "source_sheet"]
    for col in string_cols:
        df[col] = df[col].astype("string").str.strip()

    df["description"] = df["description"].fillna("unknown")

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

    df["customer_id"] = df["customer_id"].str.replace(".0", "", regex=False)

    df = df.dropna(subset=["invoice", "stock_code", "invoice_date", "quantity", "price", "country"])

    df = df.drop_duplicates()

    df["quantity"] = df["quantity"].astype("int64")
    df["price"] = df["price"].astype("float64")

    df = df[df["price"] > 0]

    df["is_canceled"] = df["invoice"].str.startswith("C", na=False)

    df["gross_revenue"] = df["quantity"] * df["price"]

    df["invoice_year"] = df["invoice_date"].dt.year
    df["invoice_month"] = df["invoice_date"].dt.month
    df["invoice_year_month"] = df["invoice_date"].dt.to_period("M").astype(str)

    return df


def generate_quality_report(df_before: pd.DataFrame, df_after: pd.DataFrame) -> None:
    report = []
    report.append("SILVER QUALITY REPORT")
    report.append("")
    report.append(f"Rows before cleaning: {len(df_before)}")
    report.append(f"Rows after cleaning: {len(df_after)}")
    report.append(f"Rows removed: {len(df_before) - len(df_after)}")
    report.append("")
    report.append("Null count after cleaning:")
    report.append(df_after.isnull().sum().to_string())
    report.append("")
    report.append("Data types after cleaning:")
    report.append(df_after.dtypes.to_string())

    QUALITY_REPORT_PATH.write_text("\n".join(report), encoding="utf-8")


def generate_data_dictionary(df: pd.DataFrame) -> None:
    dictionary = pd.DataFrame([
        {"column_name": "invoice", "description": "Invoice number of the transaction"},
        {"column_name": "stock_code", "description": "Product/item code"},
        {"column_name": "description", "description": "Product description"},
        {"column_name": "quantity", "description": "Quantity of items in the transaction"},
        {"column_name": "invoice_date", "description": "Transaction date and time"},
        {"column_name": "price", "description": "Unit price of the item"},
        {"column_name": "customer_id", "description": "Customer identifier"},
        {"column_name": "country", "description": "Country of the customer"},
        {"column_name": "source_sheet", "description": "Original sheet from source Excel"},
        {"column_name": "is_canceled", "description": "Indicates if the invoice was canceled"},
        {"column_name": "gross_revenue", "description": "Quantity multiplied by unit price"},
        {"column_name": "invoice_year", "description": "Year extracted from invoice date"},
        {"column_name": "invoice_month", "description": "Month extracted from invoice date"},
        {"column_name": "invoice_year_month", "description": "Year-month extracted from invoice date"},
    ])
    dictionary.to_csv(DATA_DICTIONARY_PATH, index=False, encoding="utf-8")


def plot_monthly_revenue(df: pd.DataFrame) -> str:
    data = df.groupby("invoice_year_month", as_index=False)["gross_revenue"].sum()
    plt.figure(figsize=(12, 6))
    plt.plot(data["invoice_year_month"], data["gross_revenue"])
    plt.xticks(rotation=45)
    plt.title("Monthly Revenue")
    plt.xlabel("Year-Month")
    plt.ylabel("Gross Revenue")
    plt.tight_layout()
    file_path = PLOTS_DIR / "monthly_revenue.png"
    plt.savefig(file_path)
    plt.close()
    return "plots/monthly_revenue.png"


def plot_top_10_countries_revenue(df: pd.DataFrame) -> str:
    data = df.groupby("country", as_index=False)["gross_revenue"].sum().sort_values(
        "gross_revenue", ascending=False
    ).head(10)
    plt.figure(figsize=(12, 6))
    plt.bar(data["country"], data["gross_revenue"])
    plt.xticks(rotation=45)
    plt.title("Top 10 Countries by Revenue")
    plt.xlabel("Country")
    plt.ylabel("Gross Revenue")
    plt.tight_layout()
    file_path = PLOTS_DIR / "top_10_countries_revenue.png"
    plt.savefig(file_path)
    plt.close()
    return "plots/top_10_countries_revenue.png"


def plot_top_10_products_quantity(df: pd.DataFrame) -> str:
    data = (
        df.groupby(["stock_code", "description"], as_index=False)["quantity"]
        .sum()
        .sort_values("quantity", ascending=False)
        .head(10)
    )
    labels = data["stock_code"].astype(str) + " - " + data["description"].astype(str)
    plt.figure(figsize=(12, 6))
    plt.bar(labels, data["quantity"])
    plt.xticks(rotation=75)
    plt.title("Top 10 Products by Quantity Sold")
    plt.xlabel("Product")
    plt.ylabel("Quantity")
    plt.tight_layout()
    file_path = PLOTS_DIR / "top_10_products_quantity.png"
    plt.savefig(file_path)
    plt.close()
    return "plots/top_10_products_quantity.png"


def generate_markdown_with_graphs(graph_paths: list[str]) -> None:
    content = [
        "# Silver Layer - Graphical Analysis",
        "",
        "## 1. Monthly Revenue",
        f"![Monthly Revenue]({graph_paths[0]})",
        "",
        "## 2. Top 10 Countries by Revenue",
        f"![Top 10 Countries by Revenue]({graph_paths[1]})",
        "",
        "## 3. Top 10 Products by Quantity Sold",
        f"![Top 10 Products by Quantity Sold]({graph_paths[2]})",
    ]
    GRAFICOS_MD_PATH.write_text("\n".join(content), encoding="utf-8")


def save_silver(df: pd.DataFrame) -> None:
    df.to_parquet(SILVER_FILE_PATH, index=False)


def main() -> None:
    df_raw = load_raw_data()
    df_raw = standardize_column_names(df_raw)

    generate_initial_reports(df_raw)

    df_silver = clean_and_transform(df_raw)

    generate_quality_report(df_raw, df_silver)
    generate_data_dictionary(df_silver)

    graph_paths = [
        plot_monthly_revenue(df_silver),
        plot_top_10_countries_revenue(df_silver),
        plot_top_10_products_quantity(df_silver),
    ]
    generate_markdown_with_graphs(graph_paths)

    save_silver(df_silver)

    print("Silver processing completed successfully.")
    print(f"Rows in silver dataset: {len(df_silver)}")
    print(f"Columns in silver dataset: {len(df_silver.columns)}")
    print(f"Parquet saved at: {SILVER_FILE_PATH}")


if __name__ == "__main__":
    main()
