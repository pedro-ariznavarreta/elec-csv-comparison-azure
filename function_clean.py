import logging
import pandas as pd
import azure.functions as func
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import os

app = func.FunctionApp()

# =========================
# FUNCIÓN PARA ARREGLAR TILDE
# =========================
def fix_encoding(text):
    if isinstance(text, str):
        try:
            return text.encode("latin1").decode("utf-8")
        except UnicodeDecodeError:
            return text
    return text

# =========================
# BLOB TRIGGER
# =========================
@app.function_name(name="clean_csv_function")
@app.blob_trigger(
    arg_name="inputblob",
    path="raw-csv/{name}",
    connection="AzureWebJobsStorage"
)
def main(inputblob: func.InputStream):

    logging.info(f"Procesando archivo: {inputblob.name}")

    # =========================
    # LEER CSV DESDE RAW
    # =========================
    df = pd.read_csv(
        BytesIO(inputblob.read()),
        encoding="latin1",
        dtype={"numero_suministro": str}
    )

    # Arreglar tildes
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(fix_encoding)

    # Eliminar filas completamente vacías
    df = df.dropna(how="all")

    # Eliminar columnas que no sirven (si existen)
    for col in ["fecha", "_id"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # numero_suministro → quitar separadores de miles
    if "numero_suministro" in df.columns:
        df["numero_suministro"] = (
            df["numero_suministro"]
            .str.replace(".", "", regex=False)
            .astype(int)
        )

    # energia → decimal a int
    if "energia" in df.columns:
        df["energia"] = (
            pd.to_numeric(df["energia"], errors="coerce")
            .astype(int)
        )

    # Limpiar nombres de columnas
    df.columns = df.columns.str.strip().str.lower()

    # =========================
    # GUARDAR EN RESULTS
    # =========================
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
