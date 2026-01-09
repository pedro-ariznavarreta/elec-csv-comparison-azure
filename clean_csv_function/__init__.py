import logging
import pandas as pd
import azure.functions as func
from io import BytesIO

def fix_encoding(text):
    if isinstance(text, str):
        try:
            return text.encode("latin1").decode("utf-8")
        except UnicodeDecodeError:
            return text
    return text

def main(inputblob: func.InputStream, outputblob: func.Out[bytes]):
    logging.info(f"Procesando archivo: {inputblob.name}")

    # Leer CSV desde blob de entrada
    df = pd.read_csv(
        BytesIO(inputblob.read()),
        encoding="latin1",
        dtype={"numero_suministro": str}
    )

    # Arreglar tildes en columnas tipo texto
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(fix_encoding)

    # Eliminar filas vacías
    df = df.dropna(how="all")

    # Eliminar columnas específicas si existen
    for col in ["fecha", "_id"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Limpiar número de suministro
    if "numero_suministro" in df.columns:
        df["numero_suministro"] = (
            df["numero_suministro"]
            .str.replace(".", "", regex=False)
            .astype(int)
        )

    # Limpiar energía
    if "energia" in df.columns:
        df["energia"] = (
            pd.to_numeric(df["energia"], errors="coerce")
            .astype(int)
        )

    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip().str.lower()

    # Guardar CSV limpio en blob de salida usando binding de Azure
    output_bytes = BytesIO()
    df.to_csv(output_bytes, index=False, encoding="utf-8-sig")
    output_bytes.seek(0)

    outputblob.set(output_bytes.read())

    logging.info(f"Archivo limpio guardado en results/{inputblob.name}")
