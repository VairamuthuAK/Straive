import os
import re
import pandas as pd
import pandera as pa
from pathlib import Path
from pandera import Column, DataFrameSchema




# ---------- 1. Course dataframe schema ----------
course_schema = DataFrameSchema(
    {
        "Cengage Master Institution ID": Column(pa.Int64, nullable=True),
        "Source URL": Column(pa.String, nullable=True),
        "Course Name": Column(pa.String, nullable=True),
        "Course Description": Column(pa.String, nullable=True),
        "Class Number": Column(pa.String, nullable=True),
        "Section": Column(pa.String, nullable=True),
        "Instructor": Column(pa.String, nullable=True),
        "Enrollment": Column(pa.String, nullable=True),
        "Course Dates": Column(pa.String, nullable=True),
        "Location": Column(pa.String, nullable=True),
        "Textbook/Course Materials": Column(pa.String, nullable=True),
    },
    strict=True,   # no extra columns
    coerce=True,   # try to cast to the right dtype
)
# ---------- 2. Directory dataframe schema ----------
campus_schema = DataFrameSchema(
    {
        "Cengage Master Institution ID": Column(pa.Int64, nullable=True),
        "Source URL": Column(pa.String, nullable=True),
        "Name": Column(pa.String, nullable=True),
        "Title": Column(pa.String, nullable=True),
        "Email": Column(pa.String, nullable=True),
        "Phone Number": Column(pa.String, nullable=True),
    },
    strict=True,
    coerce=True,
)
# ---------- 3. Calendar dataframe schema ----------
calendar_schema = DataFrameSchema(
    {
        "Cengage Master Institution ID": Column(pa.Int64, nullable=True),
        "Source URL": Column(pa.String, nullable=True),
        "Term Name": Column(pa.String, nullable=True),
        "Term Date": Column(pa.String, nullable=True),              # keep as string; parse later if needed
        "Term Date Description": Column(pa.String, nullable=True),
    },
    strict=True,
    coerce=True,
)




def save_df(df: pd.DataFrame, base_name: str, suffix: str) -> str:
    """
    Save DataFrame to CSV as: {base_name}_{suffix}.csv in data/ folder.
    Deletes existing file if present, then saves new data.
    Returns the full file path.
    """
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{base_name}_{suffix}.csv"
    filepath = data_dir / filename
    
    # Delete if exists
    if filepath.exists():
        filepath.unlink()
        print(f"Deleted existing {filename}")
    
    df.to_csv(filepath, index=False)
    print("Successfully saved:", suffix, "data to", filepath)
    return str(filepath)



def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean all cells in a DataFrame:
    - Ensure value is a string (leave non-strings as-is)
    - Fix common encoding issues (latin-1 -> utf-8 attempt)
    - Remove newlines, tabs, and other whitespace
    - Collapse multiple internal spaces to a single space
    - Strip leading/trailing whitespace
    """
    def _clean_cell(x):
        # Leave non-strings untouched
        if not isinstance(x, str):
            return x
        # Try to normalize bad encodings (best-effort)
        try:
            # Encode as latin-1 then decode as utf-8, ignore errors
            x_enc = x.encode("latin-1", errors="ignore")
            x = x_enc.decode("utf-8", errors="ignore")
        except Exception:
            pass
        # Replace any whitespace (space, tab, newline, etc.) sequences with a single space
        x = re.sub(r"\s+", " ", x)
        # Strip leading/trailing whitespace
        return x.strip()
    # Apply to every cell
    return df.applymap(_clean_cell)



# ---------- Helper function to validate by kind ----------
def validate_df(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    """
    Validate a pandas DataFrame against a predefined Pandera schema.

    This helper selects one of three schemas (course, directory, or calendar)
    based on the ``kind`` argument and validates the input DataFrame against
    the corresponding column set and data types. The schemas are configured
    with ``strict=True`` to reject unexpected columns and ``coerce=True`` to
    attempt casting values to the required dtypes during validation.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame to validate.
    kind : str
        Type of dataset to validate. Must be one of:
        ``"course"``, ``"directory"``, or ``"calendar"`` (case-insensitive).

    Returns
    -------
    pd.DataFrame
        The validated DataFrame, potentially with dtypes coerced to match
        the schema definitions.

    Raises
    ------
    pandera.errors.SchemaError
        If the DataFrame does not conform to the selected schema
        (e.g., wrong dtypes, missing/extra columns, or failed coercion).
    ValueError
        If ``kind`` is not one of ``"course"``, ``"directory"``, or
        ``"calendar"``.
    """

    kind = kind.lower()
    if kind == "course":
        return course_schema.validate(df)
    elif kind == "campus":
        return campus_schema.validate(df)
    elif kind == "calendar":
        return calendar_schema.validate(df)
    else:
        raise ValueError("kind must be 'course', 'campus', or 'calendar'")

