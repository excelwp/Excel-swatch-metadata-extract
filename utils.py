import os
import re
import base64
from typing import List, Tuple
import pandas as pd
from PIL import Image


ALLOWED_IMAGE_EXTS = {"png", "jpg", "jpeg", "webp"}


def ensure_dirs():
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("data", exist_ok=True)


def load_options():
    import pandas as pd

    def read_second_col(path: str) -> list[str]:
        df = pd.read_csv(path)
        # Prefer second column (names) if present
        if df.shape[1] >= 2:
            col = df.columns[1]
        else:
            col = df.columns[0]
        return (
            df[col]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )

    color_list = read_second_col("data/color_options.csv")      # uses GENERIC NAMES
    design_list = read_second_col("data/design_options.csv")    # uses DESIGN STYLE
    theme_list = read_second_col("data/Theme_option.csv")       # uses THEMES
    return color_list, design_list, theme_list


def filename_to_swatch_id(filename: str) -> str:
    # swatch id = filename without extension, trimmed
    base = os.path.splitext(os.path.basename(filename))[0].strip()
    return base


def normalize_list(values) -> List[str]:
    if values is None:
        return []
    if isinstance(values, list):
        return [str(v).strip() for v in values if str(v).strip()]
    # if comma string
    return [v.strip() for v in str(values).split(",") if v.strip()]


def image_bytes_to_base64(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode("utf-8")


def save_uploaded_image_overwrite(image_bytes: bytes, swatch_id: str, original_filename: str) -> str:
    """
    Overwrites the image file if same swatch_id uploaded again.
    We preserve extension from uploaded filename.
    """
    ext = os.path.splitext(original_filename)[1].lower().replace(".", "")
    if ext not in ALLOWED_IMAGE_EXTS:
        ext = "png"
    path = os.path.join("uploads", f"{swatch_id}.{ext}")
    with open(path, "wb") as f:
        f.write(image_bytes)
    return path


def safe_choice(value: str, allowed: List[str], fallback: str) -> str:
    """
    Ensures value is in allowed list; otherwise fallback.
    """
    v = (value or "").strip()
    return v if v in allowed else fallback
