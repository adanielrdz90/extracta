import os
import re
import math
import datetime
import time
import io
import requests
import json
import xml.etree.ElementTree as ET
from pdf2image import convert_from_path
from google.cloud import storage
from google.cloud import vision
import gspread
from google.oauth2.service_account import Credentials
import concurrent.futures
import cv2
import numpy as np
from PIL import Image
import openai  # Se importa la librería de OpenAI

# Configuración de la API Key de OpenAI
openai.api_key = "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"

# ------------------- CONFIGURACIONES GLOBALES -------------------
DPI = 400
SCALE = DPI / 72.0  # Convierte coordenadas de PDF (puntos) a píxeles
MARGIN = 5         # Margen en píxeles para ampliar el área de recorte

os.makedirs("debug_images", exist_ok=True)

# ------------------- CONFIGURACIONES INICIALES -------------------
poppler_path = r"C:\Users\adani\OneDrive\Desktop\Phyton workdesk\poppler-24.08.0\Library\bin"
GCP_CREDENTIALS_PATH = "sistema-empresarial-conta-8369ee2f94ed.json"
BUCKET_NAME = "bd_pedimentos"
INPUT_FOLDER = "Pedimentos/2025/"
PROCESSED_FOLDER = "Pedimentos/2025/Procesados/"
SPREADSHEET_ID = "1GWX8Fk_V3t9fahGeaLsHsLXhtkPuNau6aWSipexH8U4"
RANGE_NAME = "Pedimentos!A1"

credentials = Credentials.from_service_account_file(
    GCP_CREDENTIALS_PATH,
    scopes=["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/spreadsheets"]
)
storage_client = storage.Client(credentials=credentials)
cliente_sheets = gspread.authorize(credentials)
sheet = cliente_sheets.open_by_url(
    "https://docs.google.com/spreadsheets/d/1GWX8Fk_V3t9fahGeaLsHsLXhtkPuNau6aWSipexH8U4/edit#gid=0"
).worksheet("Pedimentos")

# ------------------- VARIABLES DE CONTROL -------------------
ENABLE_REFERENCIA = True
ENABLE_TIPO_CAMBIO = True
ENABLE_PRECIO_PAGADO = True

# ------------------- DEFINICIÓN DE LAS ZONAS DE CROPPING -------------------
FIELD_COORDS_PAGE1 = {
    "pedimento": ((92.0, 757.0), (173.0, 745.0)),
    "referencia": ((92.0, 757.0), (173.0, 745.0)),
    "tipo_cambio": ((117.0, 747.0), (213.0, 732.0)),
    "valor_dolares": ((274.0, 737.0), (425.0, 724.0)),
    "valor_aduana": ((274.0, 727.0), (427.0, 706.0)),
    "precio_pagado": ((274.0, 727.0), (427.0, 706.0)),
    "fecha_entrada": ((76.0, 513.0), (173.0, 482.0)),
    "total": ((384.0, 416.0), (433.0, 403.0)),
    "igi_pagado": ((116.0, 435.0), (155.0, 409.0)),
    "dta_pagado": ((116.0, 435.0), (155.0, 409.0)),
    "iva_pagado": ((243.0, 434.0), (284.0, 417.0))
}
FIELD_COORDS_PAGE2 = {
    "numero_serie": [((42.0, 608.0), (131.0, 596.0)), ((77.0, 533.0), (261.0, 501.0))],
    "descripcion_vehiculo": ((44.0, 653.0), (417.0, 634.0)),
    "kilometraje": ((160.0, 618.0), (225.0, 605.0))
}
ALT_KILOMETRAJE = ((163.0, 608.0), (227.0, 594.0))  # Coordenadas alternativas

# ------------------- CONSTANTES DE FALLBACK -------------------
FALLBACK_VALOR_DOLARES_COORDS = ((342.0, 745.0), (441.0, 697.0))
FALLBACK_DESCRIPCION_COORDS = ((43.0, 668.0), (414.0, 595.0))
FALLBACK_DESCRIPCION_COORDS_2 = ((39.0, 659.0), (411.0, 651.0))
FALLBACK_TOTAL_COORDS = ((368.0, 447.0), (434.0, 391.0))
FALLBACK_IGI_COORDS = ((95.0, 443.0), (154.0, 398.0))
FALLBACK_VIN_COORDS_1 = ((43.0, 621.0), (138.0, 613.0))
FALLBACK_VIN_COORDS_2 = ((45.0, 622.0), (125.0, 612.0))
FALLBACK_VALOR_DOLARES_AI_COORDS = ((8.0, 791.0), (429.0, 699.0))
FALLBACK_KILOMETRAJE_AI_COORDS = ((24.0, 648.0), (418.0, 572.0))
FALLBACK_DTA_AI_COORDS = ((21.0, 457.0), (158.0, 389.0))  # Actualmente ya no se usa para IA, se usa la imagen completa

# ------------------- FUNCIÓN DE VALIDACIÓN ESTRICTA DE DESCRIPCIÓN -------------------
def validate_strict_description(desc: str) -> bool:
    # Se espera el formato: VEHICULO <Make> <Model> <BodyCabType>, MOD. <ModelYear>, <EngineCylinders> CIL.
    # El BodyCabType es opcional.
    patron = r'^VEHICULO\s+([\w&]+(?:\s+[\w&]+)*)\s+([\w0-9&\-]+)(?:\s+(.*))?,\s+MOD\.\s+(\d{4}),\s+(\d+)\s+CIL\.$'
    match = re.match(patron, desc, re.I)
    if not match:
        return False
    try:
        model_year = int(match.group(4))
        if model_year < 1900 or model_year > 2030:
            return False
        engine_cyl = int(match.group(5))
        if engine_cyl not in [4, 6, 8, 10, 12]:
            return False
    except:
        return False
    return True

# ------------------- FUNCIONES DE FALLBACK VERTICAL DEL KILOMETRAJE -------------------
def crop_field_vertical(image, coords, scale, vertical_expansion_factor, margin=MARGIN):
    (x1, y1), (x2, y2) = coords
    left_pdf = min(x1, x2)
    right_pdf = max(x1, x2)
    bottom_pdf = min(y1, y2)
    top_pdf = max(y1, y2)
    left_px = left_pdf * scale - margin
    right_px = right_pdf * scale + margin
    center_y = (bottom_pdf + top_pdf) / 2
    original_height = top_pdf - bottom_pdf
    new_half_height = (original_height * vertical_expansion_factor) / 2
    new_bottom_pdf = center_y - new_half_height
    new_top_pdf = center_y + new_half_height
    top_px = max(image.height - (new_top_pdf * scale) - margin, 0)
    bottom_px = min(image.height - (new_bottom_pdf * scale) + margin, image.height)
    return image.crop((left_px, top_px, right_px, bottom_px))

def fallback_kilometraje_vertical(image):
    coords = FIELD_COORDS_PAGE2["kilometraje"]
    cropped = crop_field_vertical(image, coords, SCALE, vertical_expansion_factor=1.5, margin=MARGIN)
    cropped.save("debug_images/kilometraje_fallback_vertical.png")
    text = ocr_field(cropped)
    print("[DEBUG] Texto extraído en fallback vertical (kilometraje):", text)
    matches = re.findall(r'\d+', text)
    for m in matches:
        try:
            km_val = int(m)
            if 100000 <= km_val <= 999999:
                return str(km_val)
        except:
            continue
    return ""

# ------------------- NUEVA FUNCIÓN: FALLBACK PARA KILOMETRAJE (SIN IA) -------------------
def fallback_kilometraje(image):
    """
    Fallback Kilometraje 40%: Expande el área de recorte en un 140% para buscar la palabra "KILOMETRAJE" y extraer el número de 5 o 6 dígitos.
    """
    print("[LOG] Fallback Kilometraje 40%: Expandiendo área en 140% para búsqueda de 'KILOMETRAJE'.")
    coords = FIELD_COORDS_PAGE2["kilometraje"]
    expanded_crop = crop_field_expanded(image, coords, SCALE, expansion_factor=1.4)
    expanded_crop.save("debug_images/kilometraje_fallback_140.png")
    extracted_text = ocr_field(expanded_crop)
    print("[DEBUG] Texto extraído en fallback Kilometraje:", extracted_text)
    lines = extracted_text.splitlines()
    header_index = None
    for i, line in enumerate(lines):
        if "KILOMETRAJE" in line.upper():
            header_index = i
            break
    if header_index is None:
        print("[LOG] Fallback Kilometraje 40%: No se encontró la palabra 'KILOMETRAJE' en el texto.")
        return ""
    forbidden_keywords = ["PERMISO", " O ", "NOM", "FIRMA", "DESCARGO"]
    for j in range(header_index + 1, len(lines)):
        current_line = lines[j].strip()
        if any(keyword in current_line.upper() for keyword in forbidden_keywords):
            print(f"[LOG] Fallback Kilometraje 40%: Línea prohibida encontrada: {current_line}")
            break
        match = re.search(r'(\d{5,6})', current_line)
        if match:
            km_value = match.group(1)
            print(f"[LOG] Fallback Kilometraje 40%: Valor encontrado: {km_value}")
            return km_value
    print("[LOG] Fallback Kilometraje 40%: No se encontró un valor válido en el rango esperado.")
    return ""

# ------------------- NUEVO FALLBACK IA PARA KILOMETRAJE -------------------
def fallback_kilometraje_ai(page):
    cropped = crop_field(page, FALLBACK_KILOMETRAJE_AI_COORDS, SCALE)
    cropped.save("debug_images/kilometraje_ai_fallback.png")
    texto_extraido = ocr_field(cropped)
    print("[DEBUG] Texto entregado a IA (kilometraje):", texto_extraido)
    if not texto_extraido.strip():
        print("[LOG] El bloque de texto para kilómetro está vacío; no se llama a la IA.")
        return ""
    prompt = f"""Analiza el siguiente bloque de texto extraído del área del PDF:
"{texto_extraido}"
En este bloque, busca la palabra "KILOMETRAJE" (en mayúsculas) y extrae únicamente el número de 5 o 6 dígitos que aparece justo debajo o al lado de esta palabra.
Responde solo con el número, sin ningún texto adicional."""
    print("[LOG] Prompt para IA en fallback de Kilometraje:", prompt)
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0.2
        )
        ai_kilometraje = response.choices[0].message.content.strip()
        print("[LOG] Respuesta de IA en fallback de Kilometraje:", ai_kilometraje)
        if re.fullmatch(r'\d{5,6}', ai_kilometraje):
            return ai_kilometraje
        else:
            return ""
    except Exception as e:
        print("[LOG] Error al llamar a OpenAI API en fallback de Kilometraje:", e)
        return ""

# ------------------- NUEVO FALLBACK IA PARA DTA PAGADO -------------------
def fallback_dta_ai(page):
    """
    Fallback para extraer el valor del DTA usando la imagen completa de la página 1.
    Se asume que la API de OpenAI soporta análisis de imágenes (ej. GPT-4 Vision). 
    Se debe extraer el número que se encuentre directamente a la derecha de la palabra "DTA".
    Responde únicamente con el valor numérico, sin ningún texto adicional.
    """
    image_path = "debug_images/dta_pagado_fallback_img.png"
    page.save(image_path)
    print("[DEBUG] Imagen completa guardada para fallback DTA (imagen completa).")
    
    prompt = (
        "Analiza la imagen adjunta y extrae el número que se encuentra directamente a la derecha de la palabra 'DTA'.\n"
        "Responde únicamente con el valor numérico, sin ningún texto adicional."
    )
    print("[LOG] Enviando imagen a la IA para extracción de DTA con el siguiente prompt:")
    print(prompt)
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4-vision",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0.2,
            file=open(image_path, "rb")
        )
        dta_ai_val = response.choices[0].message.content.strip()
        print("[LOG] Respuesta de la IA para DTA:", dta_ai_val)
        if re.fullmatch(r'\d+', dta_ai_val):
            return dta_ai_val
        else:
            print("[LOG] El valor devuelto no coincide con el formato numérico esperado.")
            return ""
    except Exception as e:
        print("[LOG] Error al llamar a la API de OpenAI para DTA:", e)
        return ""

# ------------------- FUNCIONES ADICIONALES -------------------
def crop_field_expanded(image, coords, scale, expansion_factor, margin=MARGIN):
    (x1, y1), (x2, y2) = coords
    left_pdf = min(x1, x2)
    right_pdf = max(x1, x2)
    bottom_pdf = min(y1, y2)
    top_pdf = max(y1, y2)
    center_x = (left_pdf + right_pdf) / 2
    center_y = (bottom_pdf + top_pdf) / 2
    half_width = (right_pdf - left_pdf) / 2 * expansion_factor
    half_height = (top_pdf - bottom_pdf) / 2 * expansion_factor
    new_left_pdf = center_x - half_width
    new_right_pdf = center_x + half_width
    new_bottom_pdf = center_y - half_height
    new_top_pdf = center_y + half_height
    left_px = max(new_left_pdf * scale - margin, 0)
    right_px = min(new_right_pdf * scale + margin, image.width)
    top_px = max(image.height - (new_top_pdf * scale) - margin, 0)
    bottom_px = min(image.height - (new_bottom_pdf * scale) + margin, image.height)
    crop_box = (left_px, top_px, right_px, bottom_px)
    return image.crop(crop_box)

def fallback_vin_enhanced(image, coords, expansion_factor):
    cropped = crop_field_expanded(image, coords, SCALE, expansion_factor=expansion_factor)
    cropped.save(f"debug_images/numero_serie_fallback_{int(expansion_factor*100)}.png")
    text = ocr_field(cropped)
    print("[DEBUG] Texto extraído para VIN (enhanced):", text)
    match_vin = re.search(r'([0-9A-Z]{17})', text.upper())
    if match_vin:
        vin_candidate = match_vin.group(1)
        vin_candidate = corregir_vin_extra(vin_candidate)
        if validate_vin(vin_candidate):
            return vin_candidate
    return None

def fallback_vin_specific(image, coords):
    cropped = crop_field(image, coords, SCALE)
    cropped.save("debug_images/numero_serie_fallback_specific.png")
    text = ocr_field(cropped)
    print("[DEBUG] Texto extraído para VIN (specific):", text)
    match_vin = re.search(r'([0-9A-Z]{17})', text.upper())
    if match_vin:
        vin_candidate = match_vin.group(1)
        vin_candidate = corregir_vin_extra(vin_candidate)
        if validate_vin(vin_candidate):
            return vin_candidate
    return None

def corregir_vin_extra(vin_extraccion: str) -> str:
    return vin_extraccion.upper().replace('O', '0').replace('I', '1')

def extraer_vin_terciario_fallback(texto: str) -> str:
    match = re.search(r'NIV/NUM\. SERIE\s*[:\-]?\s*(.*)', texto, re.IGNORECASE)
    if match:
        texto_siguiente = match.group(1)
        match_vin = re.search(r'([A-HJ-NPR-Z0-9]{17})', texto_siguiente.upper())
        if match_vin:
            return match_vin.group(1)
    return None

def validar_igi_field(texto: str) -> str:
    igi_limpio = texto.strip()
    igi_numeros = re.sub(r'\D', '', igi_limpio)
    if not (4 <= len(igi_numeros) <= 5):
        return igi_numeros
    return igi_numeros

# ------------------- FUNCIONES DE DESCRIPCIÓN DEL VEHÍCULO -------------------
def process_descripcion_vehiculo(text):
    print("[DEBUG] Texto original para procesamiento:", text)
    text = re.sub(r'^[^A-Za-z]*', '', text).strip()
    idx = text.upper().find("VEHICULO")
    if idx != -1:
        text = text[idx:]
    text = re.sub(r"LAS\s+MAI\s+ARCA:", "MARCA:", text, flags=re.IGNORECASE)
    text_clean = text.strip()
    print("[DEBUG] Texto después de la limpieza inicial:", text_clean)
    text_upper = text_clean.upper()
    # Caso específico: si se detecta "TRANSPORTE DE MERCANCIAS" y "SERIE:"
    if "TRANSPORTE DE MERCANCIAS" in text_upper and "SERIE:" in text_upper:
        start = text_upper.find("VEHICULO")
        end = text_upper.find("SERIE:")
        if start != -1 and end != -1 and end > start:
            desc = text_clean[start:end].strip()
            desc = re.sub(r'[,\s]+$', '', desc)
            print("[DEBUG] Descripción extraída (TRANSPORTE):", desc)
            return desc
    # Caso específico: si se encuentra "PICK UP." se trunca hasta esa palabra
    if "PICK UP." in text_upper:
        idx = text_upper.find("PICK UP.")
        result = text_clean[:idx + len("PICK UP.")].strip()
        print("[DEBUG] Descripción extraída (PICK UP):", result)
        return result
    # Extracción hasta "CIL." con expresión regular no codiciosa
    match_simple = re.search(r'(VEHICULO.*?CIL\.)', text_clean, flags=re.IGNORECASE|re.DOTALL)
    if match_simple:
        desc = match_simple.group(1).strip()
        if re.search(r'CIL\.\s*$', desc, flags=re.IGNORECASE):
            print("[DEBUG] Descripción extraída (simple):", desc)
            return desc
        desc = re.sub(r'(\s+\d+(\.\d+)?)+\s*$', '', desc).strip()
        print("[DEBUG] Descripción extraída (después de eliminación de números finales):", desc)
        return desc
    # Si se detectan las palabras clave, se trunca la cadena según la primera aparición
    indices = []
    for kw in ["TUMBABURROS.", "MINIVAN.", "CON CAJA DE HERRAMIENTAS.", "CON TAPA DE CAJA.", "CON CAMPER.", "CON CAMPER Y CAJAS DE HERRAMIENTAS."]:
        idx_kw = text_upper.find(kw)
        if idx_kw != -1:
            indices.append(idx_kw + len(kw))
    if indices:
        cutoff = min(indices)
        result = text_clean[:cutoff].strip()
        print("[DEBUG] Descripción extraída (según palabra clave):", result)
        return result

    result = re.sub(r'\s+', ' ', text_clean).strip()
    print("[DEBUG] Descripción final procesada:", result)
    return result

def validar_formato_descripcion(desc: str) -> bool:
    patron = r'^VEHICULO\s+\S+\s+\S+(?:\s+\S+)?(?:\s+\S+)?,\s+MOD\.\s+\d{4},\s+\d+\s+CIL\.$'
    return bool(re.match(patron, desc.strip(), flags=re.IGNORECASE))

def fallback_formatear_descripcion(desc: str) -> str:
    if '|' in desc:
        desc = desc.split('|')[0].strip()
    patron = r'(VEHICULO\s+.+?,\s+MOD\.\s+\d{4},\s+\d+\s+CIL\.)'
    match = re.search(patron, desc, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return desc.strip()

def process_descripcion_vehiculo_con_fallback(text: str, image, vin: str):
    # Primero, intenta procesar la descripción obtenida vía OCR.
    processed = process_descripcion_vehiculo(text)
    print("[DEBUG] Descripción procesada inicialmente:", processed)
    if processed and validate_strict_description(processed):
        return processed

    # Si la descripción no es válida y se cuenta con un VIN válido, se invoca la API de NHTSA.
    if vin != "No encontrado" and validate_vin(vin):
        print("[LOG] Descripción OCR inválida. Se encontró un VIN válido; consultando la API de NHTSA...")
        vin_desc = decode_vin_description(vin)
        if vin_desc and validate_strict_description(vin_desc):
            print("[LOG] Descripción obtenida vía NHTSA:", vin_desc)
            return vin_desc
        else:
            print("[LOG] La API NHTSA no devolvió una descripción válida o completa.")

    # Fallback utilizando OCR expandido.
    fallback_desc = fallback_descripcion_vehiculo_enhanced(image)
    if fallback_desc != "error en la extraccion" and validate_strict_description(fallback_desc):
        return fallback_desc

    # Finalmente, fallback vía IA.
    fallback_ai_desc = fallback_descripcion_vehiculo_ai(image)
    if fallback_ai_desc != "error en la extraccion" and validate_strict_description(fallback_ai_desc):
        return fallback_ai_desc

    print("[ERROR] Todos los métodos para obtener la descripción fallaron.")
    return "error en la extraccion"

def decode_vin_description(vin):
    try:
        print("[LOG] Activando fallback de la API de NHTSA para descripción de vehículo.")
        url = f"https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValues/{vin}?format=xml"
        print("[DEBUG] Solicitud a la API NHTSA:", url)
        response = requests.get(url)
        print("[DEBUG] Respuesta NHTSA (status code):", response.status_code)
        print("[DEBUG] Respuesta NHTSA (contenido):", response.text)
        root = ET.fromstring(response.text)
        make = root.find('.//Make').text.strip() if root.find('.//Make').text else ""
        model_raw = root.find('.//Model').text.strip() if root.find('.//Model').text else ""
        model_year = root.find('.//ModelYear').text.strip() if root.find('.//ModelYear').text else ""
        engine_cyl = root.find('.//EngineCylinders').text.strip() if root.find('.//EngineCylinders').text else ""
        print("[DEBUG] Datos extraídos de NHTSA:", make, model_raw, model_year, engine_cyl)
        if not all([make, model_raw, model_year, engine_cyl]):
            return None

        # Separa el campo model_raw para extraer Model y, opcionalmente, BodyCabType
        tokens = model_raw.split()
        if len(tokens) == 1:
            model = tokens[0]
            bodycab = ""
        else:
            model = tokens[0]
            bodycab = " ".join(tokens[1:])

        if bodycab:
            description = f"VEHICULO {make} {model} {bodycab}, MOD. {model_year}, {engine_cyl} CIL."
        else:
            description = f"VEHICULO {make} {model}, MOD. {model_year}, {engine_cyl} CIL."
        return description
    except Exception as e:
        print("[ERROR] Error al decodificar VIN:", e)
        return None

def fallback_descripcion_vehiculo_enhanced(image):
    coords = FIELD_COORDS_PAGE2["descripcion_vehiculo"]
    for factor in [1.2, 1.4]:
        print(f"[LOG] Intentando fallback Descripción con expansión del {int(factor*100)}%.")
        cropped = crop_field_expanded(image, coords, SCALE, expansion_factor=factor)
        cropped.save(f"debug_images/descripcion_vehiculo_fallback_{int(factor*100)}.png")
        text = ocr_field(cropped)
        print("[DEBUG] Texto extraído en fallback descripción:", text)
        if not text.strip():
            print("[LOG] OCR devolvió texto vacío, se intenta otro recorte con otro factor.")
            continue
        processed = process_descripcion_vehiculo(text)
        print("[DEBUG] Descripción procesada intermedia:", processed)
        idx = processed.upper().find("VEHICULO")
        if idx != -1:
            processed = processed[idx:]
        if (processed.upper().startswith("VEHICULO") and ("MOD." in processed.upper() and "CIL." in processed.upper())):
            return processed
    print("[LOG] Fallback con coordenadas específicas para Descripción activado.")
    cropped = crop_field(image, FALLBACK_DESCRIPCION_COORDS_2, SCALE)
    cropped.save("debug_images/descripcion_vehiculo_fallback_spec.png")
    text = ocr_field(cropped)
    print("[DEBUG] Texto extraído en fallback descripción (coordenadas específicas):", text)
    if not text.strip():
        print("[LOG] OCR devolvió texto vacío en coordenadas específicas.")
        return "error en la extraccion"
    processed = process_descripcion_vehiculo(text)
    print("[DEBUG] Descripción procesada (coordenadas específicas):", processed)
    idx = processed.upper().find("VEHICULO")
    if idx != -1:
        processed = processed[idx:]
    if (processed.upper().startswith("VEHICULO") and ("MOD." in processed.upper() and "CIL." in processed.upper())):
        return processed
    print("[LOG] Fallback final: invocando API de OpenAI para Descripción.")
    return fallback_descripcion_vehiculo_ai(image)

def fallback_descripcion_vehiculo_ai(image):
    coords = FIELD_COORDS_PAGE2["descripcion_vehiculo"]
    cropped = crop_field_expanded(image, coords, SCALE, expansion_factor=1.4)
    cropped.save("debug_images/descripcion_vehiculo_fallback_ai.png")
    extracted_text = ocr_field(cropped)
    print("[DEBUG] Texto entregado a IA para descripción:", extracted_text)
    if not extracted_text.strip():
        print("[LOG] El texto extraído para fallback IA de descripción está vacío.")
        return "error en la extraccion"
    prompt = f"""A partir del siguiente texto, extrae y reestructura la descripción de un vehículo con el siguiente formato:
VEHICULO <Make> <Model> <BodyCabType>, MOD. <ModelYear>, <EngineCylinders> CIL.
El resultado debe encajar exactamente en ese formato.
Responde solo con la descripción formateada correctamente, sin ningún texto adicional.

Texto:
{extracted_text}
"""
    print("[LOG] Prompt para IA en fallback de Descripción:", prompt)
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0
        )
        ai_description = response.choices[0].message.content.strip()
        print("[LOG] Respuesta de IA para descripción:", ai_description)
        if validate_strict_description(ai_description):
            return ai_description
        else:
            return "error en la extraccion"
    except Exception as e:
        print("[LOG] Error al llamar a OpenAI API en fallback de Descripción:", e)
        return "error en la extraccion"

# ------------------- FALLBACK PARA PEDIMENTO CON IA -------------------
def fallback_pedimento_ai(page, registros_batch):
    cropped = crop_field(page, ((22.0, 772.0), (230.0, 714.0)), SCALE)
    cropped.save("debug_images/pedimento_fallback.png")
    texto_extraido = ocr_field(cropped)
    print("[DEBUG] Texto entregado a IA para pedimento:", texto_extraido)
    if not texto_extraido.strip():
        print("[LOG] El bloque OCR para pedimento está vacío; no se llama a la IA.")
        return ""
    valid_pedimentos = [p for p in registros_batch if re.fullmatch(r'500\d{4}', p)]
    ultimos_pedimentos = valid_pedimentos[-5:] if len(valid_pedimentos) >= 5 else valid_pedimentos
    prompt = f"""Analiza el siguiente texto extraído del área del PDF:
"{texto_extraido}"
Adicionalmente, se proporcionan los últimos 5 números de pedimentos extraídos previamente: {ultimos_pedimentos}.
Determina cuál es el número de pedimento correcto que cumpla con el formato '500XXXX'.
Responde solo con el número, sin ningún texto adicional."""
    print("[LOG] Prompt para IA en fallback de pedimento:", prompt)
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0.2
        )
        ai_pedimento = response.choices[0].message.content.strip()
        print("[LOG] Respuesta de IA en fallback de pedimento:", ai_pedimento)
        return ai_pedimento
    except Exception as e:
        print("[LOG] Error al llamar a OpenAI API en fallback de pedimento:", e)
        return ""

def crop_field(image, coords, scale, margin=MARGIN):
    (x1, y1), (x2, y2) = coords
    left_pdf = min(x1, x2)
    right_pdf = max(x1, x2)
    bottom_pdf = min(y1, y2)
    top_pdf = max(y1, y2)
    left_px = left_pdf * scale - margin
    right_px = right_pdf * scale + margin
    img_height = image.height
    top_px = img_height - (top_pdf * scale) - margin
    bottom_px = img_height - (bottom_pdf * scale) + margin
    left_px = max(left_px, 0)
    top_px = max(top_px, 0)
    right_px = min(right_px, image.width)
    bottom_px = min(bottom_px, image.height)
    crop_box = (left_px, top_px, right_px, bottom_px)
    return image.crop(crop_box)

def ocr_field(image):
    buffered = io.BytesIO()
    image.save(buffered, format="PNG")
    image_content = buffered.getvalue()
    vision_client = vision.ImageAnnotatorClient(credentials=credentials)
    gcv_image = vision.Image(content=image_content)
    response = vision_client.text_detection(image=gcv_image)
    if response.error.message:
        raise Exception(f"Vision API error: {response.error.message}")
    text = response.text_annotations[0].description if response.text_annotations else ""
    return text.strip()

def limpiar_total(total_extraccion: str) -> str:
    lineas = [line.strip() for line in total_extraccion.splitlines() if line.strip()]
    if lineas and lineas[0] == "0":
        lineas = lineas[1:]
    valor_completo = " ".join(lineas)
    valor_limpio = re.sub(r'[^\d\.,]', '', valor_completo)
    return valor_limpio

def clean_field(field, text):
    if not text:
        return text
    if field == "pedimento":
        temp = re.sub(r"^(NUM\.?\s*PEDIMENTO[:\s]*)", "", text, flags=re.IGNORECASE)
        temp = re.sub(r"\s+", "", temp).strip()
        m = re.search(r'(500\d{4})', temp)
        if m:
            return m.group(1)
        m = re.fullmatch(r'(\d{4})', temp)
        if m:
            return "500" + m.group(1)
        if len(temp) >= 7:
            candidate = temp[-7:]
            if candidate.startswith("500"):
                return candidate
            else:
                m = re.search(r'(\d{4})$', candidate)
                if m:
                    return "500" + m.group(1)
        return temp
    elif field == "referencia":
        ref = re.sub(r"^(REF(?:ERENCIA)?[:\s]*)", "", text, flags=re.IGNORECASE)
        digits = re.findall(r'\d+', ref)
        if digits:
            last7 = ''.join(digits)[-7:]
            return "ER" + last7
        return "ER" + ref.strip()
    elif field == "tipo_cambio":
        match = re.search(r'(\d{2}\.\d{4,5})', text)
        return match.group(1).strip() if match else text.strip()
    elif field == "valor_dolares":
        match = re.search(r'(\d{1,3}(?:,\d{3})*\.\d{2})', text)
        if match:
            return match.group(1)
        return text.strip()
    elif field == "valor_aduana":
        matches = re.findall(r'(\d{1,3}(?:,\d{3})+)', text)
        if matches:
            return matches[-1]
        return text.strip()
    elif field == "precio_pagado":
        matches = re.findall(r'(\d{1,3}(?:,\d{3})+)', text)
        if matches:
            return matches[-1]
        return text.strip()
    elif field == "fecha_entrada":
        match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
        if match:
            return match.group(1)
        return text.strip()
    elif field == "total":
        text_clean = re.sub(r"^(TOTAL[:\s]*)", "", text, flags=re.IGNORECASE).strip()
        return limpiar_total(text_clean)
    elif field == "igi_pagado":
        lines = text.strip().splitlines()
        if lines:
            igi_text = re.sub(r"^(IGI\s*PAGADO[:\s]*)", "", lines[-1], flags=re.IGNORECASE).strip()
            return validar_igi_field(igi_text)
        return text.strip()
    elif field == "dta_pagado":
        match = re.search(r'(\d+)', text)
        if match:
            return match.group(1)
        return text.strip()
    elif field == "iva_pagado":
        lines = text.strip().splitlines()
        if lines:
            return re.sub(r"^(IVA\s*PAGADO[:\s]*)", "", lines[-1], flags=re.IGNORECASE).strip()
        return text.strip()
    elif field == "kilometraje":
        text_clean = text.upper().strip()
        if text_clean == "KILOMETRAJE":
            return ""
        match = re.search(r'(\d+)', text_clean)
        if match:
            num = match.group(1)
            if re.fullmatch(r'\d{5,6}', num):
                return num
            else:
                return ""
        return text_clean
    elif field == "numero_serie":
        match = re.search(r'([0-9A-Z]{17})', text.upper())
        if match:
            return match.group(1)
        return re.sub(r'\s+', '', text)
    elif field == "descripcion_vehiculo":
        return process_descripcion_vehiculo(text)
    else:
        return text.strip()

def extract_fields(image, field_coords):
    results = {}
    for field, coords in field_coords.items():
        if isinstance(coords, list):
            second_coords = coords[1] if len(coords) > 1 else None
            first_coords = coords[0]
            text_candidate = ""
            if second_coords:
                cropped_second = crop_field(image, second_coords, SCALE)
                cropped_second.save(f"debug_images/{field}_option2.png")
                text_option2 = ocr_field(cropped_second)
                if text_option2.strip():
                    text_candidate = text_option2
                else:
                    cropped_first = crop_field(image, first_coords, SCALE)
                    cropped_first.save(f"debug_images/{field}_option1.png")
                    text_candidate = ocr_field(cropped_first)
            else:
                cropped_first = crop_field(image, first_coords, SCALE)
                cropped_first.save(f"debug_images/{field}_option1.png")
                text_candidate = ocr_field(cropped_first)
            results[field] = clean_field(field, text_candidate)
        else:
            cropped = crop_field(image, coords, SCALE)
            cropped.save(f"debug_images/{field}_cropped.png")
            text = ocr_field(cropped)
            results[field] = clean_field(field, text)
    return results

def extract_pedimento_top(page_text):
    pattern = r"PEDIMENTO[:\s]+(\d+)"
    match = re.search(pattern, page_text, re.IGNORECASE)
    return match.group(1) if match else None

def extract_referencia_top(page_text):
    pattern = r"REFERENCIA[:\s]+(?:ER)?(\d+)"
    match = re.search(pattern, page_text, re.IGNORECASE)
    return match.group(1) if match else None

def correct_vin(vin):
    if len(vin) != 17:
        return vin
    if vin[0] == '2' and vin[1] == '0':
        return "2C" + vin[2:]
    return vin

def validate_vin(vin):
    vin = vin.upper()
    if len(vin) != 17:
        return False
    allowed = set("0123456789ABCDEFGHJKLMNPRSTUVWXYZ")
    if any(c not in allowed for c in vin):
        return False
    transliteration = {
        'A':1, 'B':2, 'C':3, 'D':4, 'E':5, 'F':6, 'G':7, 'H':8,
        'J':1, 'K':2, 'L':3, 'M':4, 'N':5, 'P':7, 'R':9, 'S':2,
        'T':3, 'U':4, 'V':5, 'W':6, 'X':7, 'Y':8, 'Z':9,
        '0':0, '1':1, '2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9
    }
    weights = [8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2]
    total = 0
    for i, char in enumerate(vin):
        total += transliteration[char] * weights[i]
    remainder = total % 11
    expected = 'X' if remainder == 10 else str(remainder)
    return vin[8] == expected

def fix_vin(vin):
    if len(vin) != 17:
        return vin
    vin = vin.upper()
    transliteration = {
        'A':1, 'B':2, 'C':3, 'D':4, 'E':5, 'F':6, 'G':7, 'H':8,
        'J':1, 'K':2, 'L':3, 'M':4, 'N':5, 'P':7, 'R':9, 'S':2,
        'T':3, 'U':4, 'V':5, 'W':6, 'X':7, 'Y':8, 'Z':9,
        '0':0, '1':1, '2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9
    }
    weights = [8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2]
    total = 0
    for i in range(17):
        total += transliteration[vin[i]] * weights[i]
    remainder = total % 11
    expected = 'X' if remainder == 10 else str(remainder)
    if vin[8] != expected:
        corrected_vin = vin[:8] + expected + vin[9:]
        if validate_vin(corrected_vin):
            return corrected_vin
    return vin

def fix_pedimento_number(pedimento, batch_pedimentos):
    pedimento = pedimento.strip()
    m = re.search(r'(500\d{4})', pedimento)
    if m:
        return m.group(1)
    m = re.fullmatch(r'(\d{4})', pedimento)
    if m:
        return "500" + m.group(1)
    if len(pedimento) >= 7:
        candidate = pedimento[-7:]
        if candidate.startswith("500"):
            return candidate
        else:
            m = re.search(r'(\d{4})$', candidate)
            if m:
                return "500" + m.group(1)
    return pedimento

# ------------------- NUEVO FALLBACK IA PARA VIN (CUANDO NO SE OBTIENE UN VIN VÁLIDO) -------------------
def fallback_vin_ai(page):
    """
    Fallback para extraer el VIN utilizando la imagen completa de la página.
    Esta función se invoca cuando, tras todos los intentos, no se ha obtenido un VIN válido.
    Se envía la imagen completa a la API (ej. utilizando GPT-4 Vision) y se le solicita
    que identifique y extraiga el VIN, el cual debe ser exactamente 17 caracteres alfanuméricos que cumplan 
    con las normativas internacionales.
    Responde únicamente con el VIN, sin ningún texto adicional.
    """
    image_path = "debug_images/vin_ai_fallback_img.png"
    page.save(image_path)
    print("[DEBUG] Imagen completa guardada para fallback VIN (imagen completa).")
    
    prompt = (
        "Analiza la imagen adjunta y extrae el número VIN del vehículo. "
        "El número VIN debe cumplir con las normativas internacionales: debe estar compuesto por exactamente 17 caracteres alfanuméricos, "
        "no contener las letras I, O o Q, y respetar el formato estándar (incluyendo el dígito de control en la posición 9). "
        "Responde únicamente con el número VIN, sin ningún texto adicional."
    )
    print("[LOG] Enviando imagen a la IA para extracción de VIN con el siguiente prompt:")
    print(prompt)
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4-vision",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0.2,
            file=open(image_path, "rb")
        )
        vin_ai_val = response.choices[0].message.content.strip()
        print("[LOG] Respuesta de la IA para VIN:", vin_ai_val)
        if re.fullmatch(r'[0-9A-Z]{17}', vin_ai_val):
            return vin_ai_val
        else:
            print("[LOG] El valor devuelto no es un VIN válido.")
            return "error en la extraccion"
    except Exception as e:
        print("[LOG] Error al llamar a la API de OpenAI para VIN:", e)
        return "error en la extraccion"

# ------------------- FLUJO PRINCIPAL -------------------
def main():
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=INPUT_FOLDER)
    pdfs_encontrados = [b.name for b in blobs if b.name.endswith(".pdf") and "Procesados" not in b.name]
    if not pdfs_encontrados:
        print("❌ No se encontraron archivos PDF.")
        return
    print("✅ Archivos PDF encontrados en Google Cloud Storage:")
    for pdf_name in pdfs_encontrados:
        print(f"- {pdf_name}")
        file_start = time.time()

        # Descargar PDF
        step_start = time.time()
        blob = storage_client.bucket(BUCKET_NAME).blob(pdf_name)
        temp_pdf = "temp.pdf"
        blob.download_to_filename(temp_pdf)
        print(f"Tiempo descarga: {time.time() - step_start:.2f} s.")

        # Convertir PDF a imágenes (mínimo 2 páginas)
        step_start = time.time()
        pages = convert_from_path(temp_pdf, poppler_path=poppler_path, dpi=DPI,
                                   use_cropbox=False, use_pdftocairo=True)
        print(f"Tiempo conversión PDF: {time.time() - step_start:.2f} s.")
        if len(pages) < 2:
            print("El PDF no tiene al menos dos páginas; se requiere la página 2 para extraer algunos campos.")
            continue
        page1 = pages[0]
        page2 = pages[1]

        # Extraer campos de cada página
        step_start = time.time()
        fields_page1 = extract_fields(page1, FIELD_COORDS_PAGE1)
        print(f"Tiempo extracción de campos (página 1): {time.time() - step_start:.2f} s.")
        step_start = time.time()
        fields_page2 = extract_fields(page2, FIELD_COORDS_PAGE2)
        print(f"Tiempo extracción de campos (página 2): {time.time() - step_start:.2f} s.")

        # Combinar resultados y asignar precio_pagado igual a valor_aduana
        fields = {**fields_page1, **fields_page2}
        pedimento = fields.get("pedimento", "No encontrado")
        referencia = fields.get("referencia", "No encontrado")
        tipo_cambio = fields.get("tipo_cambio", "No encontrado")
        valor_dolares = fields.get("valor_dolares", "No encontrado")
        valor_aduana = fields.get("valor_aduana", "No encontrado")
        precio_pagado = valor_aduana
        fecha_entrada = fields.get("fecha_entrada", "No encontrado")
        total = fields.get("total", "No encontrado")
        igi_pagado = fields.get("igi_pagado", "No encontrado")
        dta_pagado = fields.get("dta_pagado", "No encontrado")
        iva_pagado = fields.get("iva_pagado", "No encontrado")
        numero_serie = fields.get("numero_serie", "No encontrado")
        descripcion_vehiculo = fields.get("descripcion_vehiculo", "No encontrado")
        kilometraje = fields.get("kilometraje", "")

        # Validación adicional de pedimento y referencia
        ref_clean = referencia
        if ref_clean.startswith("ER"):
            ref_clean = ref_clean[2:]
        if pedimento != ref_clean:
            page1_text = ocr_field(page1)
            pedimento_top = extract_pedimento_top(page1_text)
            referencia_top = extract_referencia_top(page1_text)
            if pedimento_top and referencia_top:
                pedimento = pedimento_top
                referencia = "ER" + referencia_top

        registros_batch = sheet.col_values(2)
        pedimento = fix_pedimento_number(pedimento, registros_batch)
        if not re.fullmatch(r'500\d{4}', pedimento):
            print(f"[LOG] Pedimento extraído ('{pedimento}') no cumple con el formato '500XXXX'. Activando fallback IA.")
            fallback_result = fallback_pedimento_ai(page1, registros_batch)
            if fallback_result and re.fullmatch(r'500\d{4}', fallback_result):
                pedimento = fallback_result
                print("[LOG] Pedimento reemplazado por:", pedimento)
            else:
                print("[LOG] Fallback IA no devolvió un resultado válido. Se mantiene el pedimento extraído:", pedimento)

        # Validación y fallback para Valor Dólares
        if valor_dolares != "No encontrado":
            if not re.fullmatch(r'^\d{1,2},\d{3}\.\d{2}$', valor_dolares):
                print(f"[LOG] Valor Dólares extraído ('{valor_dolares}') no cumple con la estructura esperada. Activando fallback IA.")
                fallback_val = fallback_valor_dolares(page1)
                if fallback_val and re.fullmatch(r'^\d{1,2},\d{3}\.\d{2}$', fallback_val):
                    valor_dolares = fallback_val
                    print("[LOG] Valor Dólares reemplazado por:", valor_dolares)
                else:
                    print("[LOG] Fallback IA no devolvió resultado válido. Se mantiene el valor extraído:", valor_dolares)

        # Fallback para Kilometraje
        if not kilometraje and "TRANSPORTE DE MERCANCIAS" in descripcion_vehiculo.upper():
            alt_crop = crop_field(page2, ALT_KILOMETRAJE, SCALE)
            alt_text = ocr_field(alt_crop)
            alt_km = clean_field("kilometraje", alt_text)
            if alt_km:
                kilometraje = alt_km
        if not kilometraje:
            km_fallback = fallback_kilometraje(page2)
            if km_fallback:
                kilometraje = km_fallback
        try:
            km_val_int = int(kilometraje.replace(',', '')) if kilometraje != "" else 0
        except:
            km_val_int = 0
        if km_val_int < 100000 or km_val_int > 999999:
            km_fallback_vertical = fallback_kilometraje_vertical(page2)
            if km_fallback_vertical:
                kilometraje = km_fallback_vertical
        if not kilometraje:
            km_fallback_ai = fallback_kilometraje_ai(page2)
            if km_fallback_ai and re.fullmatch(r'\d{5,6}', km_fallback_ai):
                kilometraje = km_fallback_ai
                print("[LOG] Kilometraje obtenido mediante fallback IA:", kilometraje)
            else:
                print("[LOG] Fallback IA para Kilometraje no devolvió un resultado válido.")

        # Fallback para DTA pagado: si es menor a 3 dígitos
        if dta_pagado.isdigit():
            dta_num = int(dta_pagado)
            if dta_num < 100:
                print(f"[LOG] DTA pagado extraído ('{dta_pagado}') es menor a 3 dígitos. Activando fallback IA para DTA.")
                dta_fallback = fallback_dta_ai(page1)
                if dta_fallback and re.fullmatch(r'\d{3,}', dta_fallback):
                    dta_pagado = dta_fallback
                    print("[LOG] DTA pagado reemplazado por:", dta_pagado)
                else:
                    print("[LOG] Fallback IA para DTA no devolvió un resultado válido.")

        # Fallback para descripción del vehículo.
        descripcion_vehiculo = process_descripcion_vehiculo_con_fallback(descripcion_vehiculo, page2, numero_serie)

        # Fallback para VIN
        if numero_serie == "No encontrado" or not validate_vin(numero_serie):
            coords_vin = FIELD_COORDS_PAGE2["numero_serie"][0] if isinstance(FIELD_COORDS_PAGE2["numero_serie"], list) else FIELD_COORDS_PAGE2["numero_serie"]
            new_vin = fallback_vin_enhanced(page2, coords_vin, expansion_factor=1.2)
            if new_vin is None or not validate_vin(new_vin):
                new_vin = fallback_vin_enhanced(page2, coords_vin, expansion_factor=1.4)
                if new_vin is None or not validate_vin(new_vin):
                    new_vin = fallback_vin_specific(page2, FALLBACK_VIN_COORDS_1)
                    if new_vin is None or not validate_vin(new_vin):
                        new_vin = fallback_vin_specific(page2, FALLBACK_VIN_COORDS_2)
                        if new_vin is None or not validate_vin(new_vin):
                            print("[LOG] Métodos anteriores no devolvieron un VIN válido, activando fallback IA para VIN.")
                            new_vin = fallback_vin_ai(page2)
                            if new_vin == "error en la extraccion":
                                numero_serie = "error en la extraccion"
                            else:
                                numero_serie = new_vin
                        else:
                            numero_serie = new_vin
                    else:
                        numero_serie = new_vin
                else:
                    numero_serie = new_vin
            else:
                numero_serie = new_vin

        total_time = round(time.time() - file_start, 2)

        print("Pedimento:", pedimento)
        print("Referencia:", referencia)
        print("Tipo Cambio:", tipo_cambio)
        print("Valor Dólares:", valor_dolares)
        print("Valor Aduana:", valor_aduana)
        print("Precio Pagado:", precio_pagado)
        print("Fecha Entrada:", fecha_entrada)
        print("Total:", total)
        print("IGI Pagado:", igi_pagado)
        print("DTA Pagado:", dta_pagado)
        print("IVA Pagado:", iva_pagado)
        print("Kilometraje al importar:", kilometraje)
        print("Núm. Serie:", numero_serie)
        print("Desc. Vehículo:", descripcion_vehiculo)
        step_start = time.time()
        registros = sheet.col_values(2)
        if pedimento in registros:
            print(f"⚠️ El pedimento {pedimento} ya está registrado. Omitiendo...\n")
        else:
            registro = [
                referencia,
                pedimento,
                tipo_cambio,
                valor_dolares,
                valor_aduana,
                precio_pagado,
                fecha_entrada,
                total,
                kilometraje,
                igi_pagado,
                dta_pagado,
                iva_pagado,
                numero_serie,
                descripcion_vehiculo,
                pdf_name,
                total_time
            ]
            sheet.append_row(registro, value_input_option='USER_ENTERED')
            print("✅ Datos insertados en Google Sheets.")
        print(f"Tiempo actualización Google Sheets: {time.time() - step_start:.2f} s.")
        step_start = time.time()
        destino = PROCESSED_FOLDER + os.path.basename(pdf_name)
        bucket_blob = storage_client.bucket(BUCKET_NAME).blob(pdf_name)
        dest_blob = storage_client.bucket(BUCKET_NAME).blob(destino)
        dest_blob.upload_from_string(bucket_blob.download_as_bytes())
        bucket_blob.delete()
        print(f"Tiempo mover archivo: {time.time() - step_start:.2f} s.")
        print(f"Tiempo total para {pdf_name}: {time.time() - file_start:.2f} s.\n")

if __name__ == "__main__":
    main()