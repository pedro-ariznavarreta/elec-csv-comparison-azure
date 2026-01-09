import logging
import azure.functions as func

def main(inputblob: func.InputStream, outputblob: func.Out[bytes]):
    logging.info(f"Procesando archivo: {inputblob.name}")
    # Copiamos el blob tal cual, sin tocar pandas
    outputblob.set(inputblob.read())
