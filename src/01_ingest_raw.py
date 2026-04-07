from pathlib import Path
from datetime import datetime
import pandas as pd
from config import RAW_DIR

SOURCE_FILE_NAME = "online_retail_ii.xlsx"
RAW_FILE_PATH = RAW_DIR / SOURCE_FILE_NAME
METADATA_FILE_PATH = RAW_DIR / "raw_metadata.txt"


def validate_source_file(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado em: {file_path}\n"
            f"Coloque o arquivo original da base dentro de data/raw/."
        )


def ingest_raw() -> None:
    validate_source_file(RAW_FILE_PATH)

    sheets_dict = pd.read_excel(RAW_FILE_PATH, sheet_name=None)

    total_rows = 0
    total_columns = None
    sheet_details = []

    for sheet_name, df in sheets_dict.items():
        total_rows += df.shape[0]
        total_columns = df.shape[1]
        sheet_details.append(f"{sheet_name}: {df.shape[0]} linhas")

    first_df = next(iter(sheets_dict.values()))
    column_names = list(first_df.columns)

    metadata = (
        f"ingestion_timestamp: {datetime.now().isoformat()}\n"
        f"file_name: {SOURCE_FILE_NAME}\n"
        f"sheets: {list(sheets_dict.keys())}\n"
        f"sheet_details: {sheet_details}\n"
        f"rows_total: {total_rows}\n"
        f"columns: {total_columns}\n"
        f"column_names: {column_names}\n"
    )

    METADATA_FILE_PATH.write_text(metadata, encoding="utf-8")

    print("Ingestão raw concluída com sucesso.")
    print(f"Arquivo localizado em: {RAW_FILE_PATH}")
    print(f"Abas encontradas: {list(sheets_dict.keys())}")
    print(f"Total de linhas: {total_rows}")
    print(f"Quantidade de colunas: {total_columns}")
    print("Colunas encontradas:")
    print(column_names)


if __name__ == "__main__":
    ingest_raw()
