import os
import json
import mimetypes
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import streamlit as st
import pandas as pd
from dotenv import load_dotenv

from utils import (
    ensure_dirs,
    load_options,
    filename_to_swatch_id,
    image_bytes_to_base64,
    normalize_list,
    save_uploaded_image_overwrite,
    safe_choice,
)
from llm import (
    PROMPT_VERSION,
    build_metadata_prompt,
    build_description_prompt,
    claude_extract_metadata,
    claude_generate_description,
)
from db import (
    init_db,
    upsert_swatch_record,
    insert_feedback,
    get_recent_feedback_examples,
    create_bulk_batch,
    finalize_bulk_batch,
    fetch_swatch_records,
)

load_dotenv()

# 🔐 Load secrets for both local (.env) and Streamlit Cloud
if "ANTHROPIC_API_KEY" not in os.environ:
    os.environ["ANTHROPIC_API_KEY"] = st.secrets.get("ANTHROPIC_API_KEY", "")

if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = st.secrets.get("DATABASE_URL", "")

ensure_dirs()

st.set_page_config(page_title="Wallpaper Metadata Extractor (Claude)", layout="wide")


def media_type_from_filename(filename: str) -> str:
    mt, _ = mimetypes.guess_type(filename)
    return mt or "image/jpeg"


def validate_and_fix_metadata(
    md: Dict[str, Any],
    allowed_colors: List[str],
    allowed_designs: List[str],
    allowed_themes: List[str],
) -> Dict[str, Any]:
    # Basic sanitation: ensure returned keys exist
    md = md or {}
    primary = safe_choice(str(md.get("primary_color", "")).strip(), allowed_colors, allowed_colors[0] if allowed_colors else "")
    secondary = normalize_list(md.get("secondary_colors", []))
    secondary = [c for c in secondary if c in allowed_colors and c != primary]
    # de-dup while preserving order
    seen = set()
    secondary2 = []
    for c in secondary:
        if c not in seen:
            secondary2.append(c)
            seen.add(c)

    design = safe_choice(str(md.get("design_style", "")).strip(), allowed_designs, allowed_designs[0] if allowed_designs else "")
    theme = safe_choice(str(md.get("theme", "")).strip(), allowed_themes, allowed_themes[0] if allowed_themes else "")
    suitable = str(md.get("suitable_for", "")).strip()

    return {
        "primary_color": primary,
        "secondary_colors": secondary2,
        "design_style": design,
        "theme": theme,
        "suitable_for": suitable,
    }


def header():
    st.title("Wallpaper Swatch Metadata Extractor (Claude + PostgreSQL)")
    st.caption("Single upload (review + accept) • Bulk multi-file upload (auto) • Export CSV anytime • Overwrite on duplicate swatch_id")


def tab_single(color_list, design_list, theme_list):
    st.subheader("Single Swatch (manual swatch_id • review • accept)")

    colA, colB = st.columns([1, 1])

    with colA:
        file = st.file_uploader("Upload 1 swatch image", type=["png", "jpg", "jpeg", "webp"], accept_multiple_files=False)
        swatch_id = st.text_input("Enter Swatch ID (required)", value="")

        if file:
            st.image(file, caption=file.name, use_container_width=True)

    with colB:
        st.markdown("### Extract + Review")
        if "single_md_raw" not in st.session_state:
            st.session_state.single_md_raw = None
        if "single_md_valid" not in st.session_state:
            st.session_state.single_md_valid = None
        if "single_desc" not in st.session_state:
            st.session_state.single_desc = None
        if "single_image_path" not in st.session_state:
            st.session_state.single_image_path = None
        if "single_llm_raw" not in st.session_state:
            st.session_state.single_llm_raw = None

        extract_btn = st.button("Extract Metadata with Claude", type="primary", disabled=(file is None or not swatch_id.strip()))

        if extract_btn:
            try:
                image_bytes = file.getvalue()
                image_b64 = image_bytes_to_base64(image_bytes)
                mt = media_type_from_filename(file.name)

                # Save image (overwrite if same swatch_id)
                image_path = save_uploaded_image_overwrite(image_bytes, swatch_id.strip(), file.name)
                st.session_state.single_image_path = image_path

                # Level-1 feedback examples (recent corrections)
                feedback_examples = get_recent_feedback_examples(limit=5)
                prompt = build_metadata_prompt(color_list, design_list, theme_list, feedback_examples=feedback_examples)

                md = claude_extract_metadata(image_b64, mt, prompt)
                st.session_state.single_md_raw = md
                st.session_state.single_llm_raw = md

                md_valid = validate_and_fix_metadata(md, color_list, design_list, theme_list)
                st.session_state.single_md_valid = md_valid
                st.session_state.single_desc = None

                st.success("Metadata extracted. Review/edit below, then Accept.")
            except Exception as e:
                st.session_state.single_md_raw = None
                st.session_state.single_md_valid = None
                st.session_state.single_desc = None
                st.error(f"Extraction failed: {e}")

        md_valid = st.session_state.single_md_valid
        if md_valid:
            st.markdown("### Review / Edit (constrained fields use options)")
            # Editable UI
            primary = st.selectbox("Primary Color", color_list, index=(color_list.index(md_valid["primary_color"]) if md_valid["primary_color"] in color_list else 0))
            secondary = st.multiselect("Secondary Colors", color_list, default=[c for c in md_valid["secondary_colors"] if c in color_list])
            design = st.selectbox("Design Style", design_list, index=(design_list.index(md_valid["design_style"]) if md_valid["design_style"] in design_list else 0))
            theme = st.selectbox("Theme", theme_list, index=(theme_list.index(md_valid["theme"]) if md_valid["theme"] in theme_list else 0))
            suitable_for = st.text_input("Suitable for (LLM-generated; editable)", value=md_valid.get("suitable_for", ""))

            corrected = {
                "primary_color": primary,
                "secondary_colors": [c for c in secondary if c != primary],
                "design_style": design,
                "theme": theme,
                "suitable_for": suitable_for.strip(),
            }

            accept_btn = st.button("Accept + Generate Physical Description + Save", type="secondary")
            if accept_btn:
                try:
                    image_bytes = file.getvalue()
                    image_b64 = image_bytes_to_base64(image_bytes)
                    mt = media_type_from_filename(file.name)

                    desc_prompt = build_description_prompt(corrected)
                    desc = claude_generate_description(image_b64, mt, desc_prompt)
                    st.session_state.single_desc = desc

                    # Determine if correction happened (for feedback)
                    original = st.session_state.single_md_raw or {}
                    original_valid = validate_and_fix_metadata(original, color_list, design_list, theme_list)

                    corrected_metadata = corrected.copy()
                    # Save main record (ACCEPTED)
                    upsert_swatch_record(
                        swatch_id=swatch_id.strip(),
                        image_path=st.session_state.single_image_path,
                        primary_color=corrected_metadata["primary_color"],
                        secondary_colors=corrected_metadata["secondary_colors"],
                        design_style=corrected_metadata["design_style"],
                        theme=corrected_metadata["theme"],
                        suitable_for=corrected_metadata["suitable_for"],
                        description=desc,
                        status="ACCEPTED",
                        source_type="single",
                        bulk_batch_id=None,
                        prompt_version=PROMPT_VERSION,
                        llm_raw_response=st.session_state.single_llm_raw,
                        error_message=None,
                    )

                    # Store feedback only if corrected differs from model output (Level 1)
                    if json.dumps(original_valid, sort_keys=True) != json.dumps(corrected_metadata, sort_keys=True):
                        st.info("You corrected the model output — saving feedback for future guidance.")
                        notes = st.text_area("Optional correction notes (saved)", value="", key="single_correction_notes")
                        insert_feedback(
                            swatch_id=swatch_id.strip(),
                            original_metadata=original_valid,
                            corrected_metadata=corrected_metadata,
                            correction_notes=notes.strip() if notes else None,
                        )

                    st.success("Saved (overwrites if swatch_id already existed).")
                except Exception as e:
                    st.error(f"Save/description failed: {e}")

        if st.session_state.single_desc:
            st.markdown("### Physical Description (Saved)")
            st.write(st.session_state.single_desc)


def tab_bulk(color_list, design_list, theme_list):
    st.subheader("Bulk Upload (select multiple images • auto-run • auto-accept • overwrite duplicates)")

    files = st.file_uploader(
        "Select multiple images (choose all from a folder)",
        type=["png", "jpg", "jpeg", "webp"],
        accept_multiple_files=True,
    )

    run_name = st.text_input("Run name (optional)", value=f"bulk_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

    start = st.button("Start Bulk Processing", type="primary", disabled=(not files))

    if start and files:
        total = len(files)
        batch_id = create_bulk_batch(run_name=run_name.strip() or None, total_files=total)

        prog = st.progress(0)
        status_box = st.empty()

        success_count = 0
        fail_count = 0
        failed_files: Dict[str, str] = {}

        for i, f in enumerate(files, start=1):
            try:
                swatch_id = filename_to_swatch_id(f.name)
                image_bytes = f.getvalue()
                image_b64 = image_bytes_to_base64(image_bytes)
                mt = media_type_from_filename(f.name)

                status_box.write(f"Processing {i}/{total}: **{f.name}** → swatch_id: `{swatch_id}`")

                # overwrite image file
                image_path = save_uploaded_image_overwrite(image_bytes, swatch_id, f.name)

                # Bulk: no per-item feedback examples needed (keeps it faster/cheaper)
                prompt = build_metadata_prompt(color_list, design_list, theme_list, feedback_examples=None)
                md = claude_extract_metadata(image_b64, mt, prompt)
                md_valid = validate_and_fix_metadata(md, color_list, design_list, theme_list)

                # Auto-generate description
                desc_prompt = build_description_prompt(md_valid)
                desc = claude_generate_description(image_b64, mt, desc_prompt)

                upsert_swatch_record(
                    swatch_id=swatch_id,
                    image_path=image_path,
                    primary_color=md_valid["primary_color"],
                    secondary_colors=md_valid["secondary_colors"],
                    design_style=md_valid["design_style"],
                    theme=md_valid["theme"],
                    suitable_for=md_valid["suitable_for"],
                    description=desc,
                    status="ACCEPTED",
                    source_type="bulk",
                    bulk_batch_id=batch_id,
                    prompt_version=PROMPT_VERSION,
                    llm_raw_response=md,
                    error_message=None,
                )

                success_count += 1
            except Exception as e:
                fail_count += 1
                failed_files[f.name] = str(e)

                # Save FAILED record (still overwrite existing swatch_id if same)
                try:
                    swatch_id = filename_to_swatch_id(f.name)
                    upsert_swatch_record(
                        swatch_id=swatch_id,
                        image_path=None,
                        primary_color=None,
                        secondary_colors=[],
                        design_style=None,
                        theme=None,
                        suitable_for=None,
                        description=None,
                        status="FAILED",
                        source_type="bulk",
                        bulk_batch_id=batch_id,
                        prompt_version=PROMPT_VERSION,
                        llm_raw_response=None,
                        error_message=str(e),
                    )
                except Exception:
                    pass

            prog.progress(i / total)

        st.success(f"Bulk completed. Success: {success_count}, Failed: {fail_count}")

        if failed_files:
            st.warning("Some files failed:")
            st.json(failed_files)

        st.markdown("### Optional feedback about this bulk run")
        bulk_feedback = st.text_area("Feedback message (optional)", value="")

        save_feedback = st.button("Save Bulk Feedback")
        if save_feedback:
            finalize_bulk_batch(
                bulk_batch_id=batch_id,
                success_count=success_count,
                fail_count=fail_count,
                failed_files=failed_files,
                optional_feedback_message=bulk_feedback.strip() if bulk_feedback else None,
            )
            st.success("Bulk feedback saved.")


def tab_export(color_list, design_list, theme_list):
    st.subheader("Export CSV (available anytime)")

    with st.expander("Filters (optional)", expanded=False):
        status = st.selectbox("Status", ["ALL", "ACCEPTED", "NEEDS_REVIEW", "FAILED"], index=0)
        source_type = st.selectbox("Source Type", ["ALL", "single", "bulk"], index=0)

        theme = st.selectbox("Theme", ["ALL"] + theme_list, index=0)
        design_style = st.selectbox("Design Style", ["ALL"] + design_list, index=0)

        c1, c2 = st.columns(2)
        with c1:
            date_from = st.date_input("Created from", value=None)
        with c2:
            date_to = st.date_input("Created to", value=None)

    filters = {
        "status": status,
        "source_type": source_type,
        "theme": theme,
        "design_style": design_style,
        "date_from": date_from if isinstance(date_from, date) else None,
        "date_to": date_to if isinstance(date_to, date) else None,
    }

    fetch_btn = st.button("Fetch from Database", type="primary")
    if fetch_btn:
        rows = fetch_swatch_records(filters=filters)
        df = pd.DataFrame(rows)

        if df.empty:
            st.info("No records found for these filters.")
            return

        # Pretty formatting for arrays
        if "secondary_colors" in df.columns:
            df["secondary_colors"] = df["secondary_colors"].apply(lambda x: ",".join(x) if isinstance(x, list) else (x or ""))

        st.dataframe(df, use_container_width=True)

        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name=f"swatch_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )


def main():
    header()

    with st.sidebar:
        st.markdown("### Setup")
        if st.button("Initialize DB (run once)", help="Creates tables if they don't exist"):
            try:
                init_db("schema.sql")
                st.success("DB initialized.")
            except Exception as e:
                st.error(f"DB init failed: {e}")

        st.divider()
        st.markdown("### Notes")
        st.write("- Duplicate swatch_id uploads overwrite records and image file.")
        st.write("- Bulk mode auto-accepts and stores directly.")
        st.write("- Export tab works anytime.")

    try:
        color_list, design_list, theme_list = load_options()
    except Exception as e:
        st.error(f"Could not load option CSVs from /data: {e}")
        st.stop()

    tabs = st.tabs(["Single Swatch", "Bulk Upload", "Export CSV"])
    with tabs[0]:
        tab_single(color_list, design_list, theme_list)
    with tabs[1]:
        tab_bulk(color_list, design_list, theme_list)
    with tabs[2]:
        tab_export(color_list, design_list, theme_list)


if __name__ == "__main__":
    main()
