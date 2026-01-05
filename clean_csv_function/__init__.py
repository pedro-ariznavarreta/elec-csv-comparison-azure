import logging
import pandas as pd
import azure.functions as func
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import os


def fix_encoding(text):
    if isinstance(text, str):
        try:
            return text.encode("latin1").decode("utf-8")
        except UnicodeDecodeError:
            return text
    return text


def main(inputblob: func.InputStream):
    logging.info(f"Procesando archivo: {inputblob.name}")

    df = pd.read_csv(
        BytesIO(inputblob.read()),
        encoding="latin1",
        dtype={"numero_suministro": str}
    )

    # Arreglar tildes
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(fix_encoding)

    # Eliminar filas vacías
    df = df.dropna(how="all")

    # Eliminar columnas si existen
    for col in ["fecha", "_id"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Limpiar numero_suministro
    if "numero_suministro" in df.columns:
        df["numero_suministro"] = (
            df["numero_suministro"]
            .str.replace(".", "", regex=False)
            .astype(int)
        )

    # Limpiar energia
    if "energia" in df.columns:
        df["energia"] = (
            pd.to_numeric(df["energia"], errors="coerce")
            .astype(int)
        )

    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip().str.lower()

    # Guardar en results
    blob_service_client = BlobServiceClient.from_connection_string(
        os.environ["AzureWebJobsStorage"]
    )

    file_name = os.path.basename(inputblob.name)

    output_blob_client = blob_service_client.get_blob_client(
        container="results",
        blob=file_name
    )

    output = BytesIO()
    df.to_csv(output, index=False, encoding="utf-8-sig")
    output.seek(0)

    output_blob_client.upload_blob(output, overwrite=True)

    logging.info(f"Archivo limpio guardado en results/{file_name}")
