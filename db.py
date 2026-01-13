import os
import json
import uuid
from typing import Any, Dict, List, Optional, Tuple
import psycopg2
import psycopg2.extras


def get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(db_url)


def init_db(schema_sql_path: str = "schema.sql") -> None:
    with get_conn() as conn, conn.cursor() as cur:
        with open(schema_sql_path, "r", encoding="utf-8") as f:
            cur.execute(f.read())
        conn.commit()


def upsert_swatch_record(
    swatch_id: str,
    image_path: Optional[str],
    primary_color: Optional[str],
    secondary_colors: Optional[List[str]],
    design_style: Optional[str],
    theme: Optional[str],
    suitable_for: Optional[str],
    description: Optional[str],
    status: str,
    source_type: str,
    bulk_batch_id: Optional[str],
    prompt_version: str,
    llm_raw_response: Optional[Dict[str, Any]],
    error_message: Optional[str],
) -> None:
    """
    Overwrite behavior:
    - swatch_id is UNIQUE
    - ON CONFLICT(swatch_id) DO UPDATE overwrites all fields with new values
    """
    sql = """
    INSERT INTO swatch_records (
      swatch_id, image_path, primary_color, secondary_colors, design_style, theme, suitable_for, description,
      status, source_type, bulk_batch_id, prompt_version, llm_raw_response, error_message
    )
    VALUES (
      %(swatch_id)s, %(image_path)s, %(primary_color)s, %(secondary_colors)s, %(design_style)s, %(theme)s, %(suitable_for)s, %(description)s,
      %(status)s, %(source_type)s, %(bulk_batch_id)s::uuid, %(prompt_version)s, %(llm_raw_response)s::jsonb, %(error_message)s
    )
    ON CONFLICT (swatch_id) DO UPDATE SET
      image_path = EXCLUDED.image_path,
      primary_color = EXCLUDED.primary_color,
      secondary_colors = EXCLUDED.secondary_colors,
      design_style = EXCLUDED.design_style,
      theme = EXCLUDED.theme,
      suitable_for = EXCLUDED.suitable_for,
      description = EXCLUDED.description,
      status = EXCLUDED.status,
      source_type = EXCLUDED.source_type,
      bulk_batch_id = EXCLUDED.bulk_batch_id,
      prompt_version = EXCLUDED.prompt_version,
      llm_raw_response = EXCLUDED.llm_raw_response,
      error_message = EXCLUDED.error_message,
      updated_at = NOW();
    """

    params = {
        "swatch_id": swatch_id,
        "image_path": image_path,
        "primary_color": primary_color,
        "secondary_colors": secondary_colors,
        "design_style": design_style,
        "theme": theme,
        "suitable_for": suitable_for,
        "description": description,
        "status": status,
        "source_type": source_type,
        "bulk_batch_id": bulk_batch_id,
        "prompt_version": prompt_version,
        "llm_raw_response": json.dumps(llm_raw_response) if llm_raw_response is not None else None,
        "error_message": error_message,
    }

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()


def insert_feedback(
    swatch_id: str,
    original_metadata: Dict[str, Any],
    corrected_metadata: Dict[str, Any],
    correction_notes: Optional[str],
) -> None:
    sql = """
    INSERT INTO llm_feedback (swatch_id, original_metadata, corrected_metadata, correction_notes)
    VALUES (%s, %s::jsonb, %s::jsonb, %s)
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (swatch_id, json.dumps(original_metadata), json.dumps(corrected_metadata), correction_notes))
        conn.commit()


def get_recent_feedback_examples(limit: int = 5) -> List[Dict[str, Any]]:
    sql = """
    SELECT original_metadata, corrected_metadata, correction_notes
    FROM llm_feedback
    ORDER BY created_at DESC
    LIMIT %s
    """
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def create_bulk_batch(run_name: str, total_files: int) -> str:
    batch_id = str(uuid.uuid4())
    sql = """
    INSERT INTO bulk_batches (bulk_batch_id, run_name, total_files, success_count, fail_count, failed_files)
    VALUES (%s::uuid, %s, %s, 0, 0, '{}'::jsonb)
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (batch_id, run_name, total_files))
        conn.commit()
    return batch_id


def finalize_bulk_batch(
    bulk_batch_id: str,
    success_count: int,
    fail_count: int,
    failed_files: Dict[str, str],
    optional_feedback_message: Optional[str],
) -> None:
    sql = """
    UPDATE bulk_batches
    SET success_count=%s,
        fail_count=%s,
        failed_files=%s::jsonb,
        optional_feedback_message=%s,
        ended_at=NOW()
    WHERE bulk_batch_id=%s::uuid
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (success_count, fail_count, json.dumps(failed_files), optional_feedback_message, bulk_batch_id))
        conn.commit()


def fetch_swatch_records(filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Used for Export CSV anytime.
    """
    base = """
    SELECT swatch_id, primary_color, secondary_colors, design_style, theme, suitable_for,
           description, status, source_type, bulk_batch_id, image_path, created_at, updated_at
    FROM swatch_records
    """
    where = []
    params = []

    if filters:
        if filters.get("status") and filters["status"] != "ALL":
            where.append("status = %s")
            params.append(filters["status"])
        if filters.get("source_type") and filters["source_type"] != "ALL":
            where.append("source_type = %s")
            params.append(filters["source_type"])
        if filters.get("theme") and filters["theme"] != "ALL":
            where.append("theme = %s")
            params.append(filters["theme"])
        if filters.get("design_style") and filters["design_style"] != "ALL":
            where.append("design_style = %s")
            params.append(filters["design_style"])
        if filters.get("date_from"):
            where.append("created_at >= %s")
            params.append(filters["date_from"])
        if filters.get("date_to"):
            where.append("created_at <= %s")
            params.append(filters["date_to"])

    if where:
        base += " WHERE " + " AND ".join(where)

    base += " ORDER BY updated_at DESC"

    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(base, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]
