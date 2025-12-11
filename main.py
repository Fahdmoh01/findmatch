from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse
from datetime import date, timedelta
from io import BytesIO
import pandas as pd
import time

app = FastAPI(title="Transaction Matcher API")


def load_transactions_from_excel(file_bytes: bytes) -> pd.DataFrame:
    """
    Load the uploaded Excel file into a DataFrame.

    This version assumes NO header row and uses fixed column positions:
        0: date
        1: transaction_id
        2: outcome
        3: value
        4: transaction_type
        5: pan
        6: flag

    If your sheet has a real header row, change header=None to header=0 and
    rename using the real column names instead.
    """
    # If your file has headers, use: header=0
    df = pd.read_excel(BytesIO(file_bytes), header=None)

    df = df.rename(
        columns={
            0: "date",
            1: "transaction_id",
            2: "outcome",
            3: "value",
            4: "transaction_type",
            5: "pan",
            6: "flag",
        }
    )

    # Parse dates
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # Ensure value is numeric (float)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    # Ensure value is numeric (float) and round to 2 decimal places
    # df["value"] = pd.to_numeric(df["value"], errors="coerce")
    # df = df.dropna(subset=["value"])
    # df["value"] = df["value"].round(2) 

    return df


def find_subset_sum(values, target_amount: float, time_limit_seconds: float = 10.0, tol: float = 1e-6):
    """
    Try to find ONE subset of 'values' (floats) that sums exactly to 'target_amount' (float).

    values: list[float]
    target_amount: float
    time_limit_seconds: float -> hard time limit to avoid very long runs
    tol: float -> tolerance for float comparisons

    Returns:
        list of indices of 'values' that sum to target_amount within tolerance,
        or None if no subset found (or time limit exceeded).
    """
    start = time.time()

    # Filter out values that are definitely too big
    indexed_vals = [(i, v) for i, v in enumerate(values) if v > 0 and v <= target_amount + tol]

    # Sort by value descending to help pruning
    indexed_vals.sort(key=lambda x: x[1], reverse=True)
    n = len(indexed_vals)

    solution = None
    found = False

    def backtrack(pos, current_sum, chosen_indices):
        nonlocal solution, found

        # Time guard
        if time.time() - start > time_limit_seconds:
            return

        # Exact match within tolerance
        if abs(current_sum - target_amount) <= tol:
            solution = chosen_indices[:]
            found = True
            return

        # Stop if exceeded target (with tolerance) or no more items or already found
        if current_sum > target_amount + tol or pos >= n or found:
            return

        idx, value = indexed_vals[pos]

        # Choose current item
        chosen_indices.append(idx)
        backtrack(pos + 1, current_sum + value, chosen_indices)
        chosen_indices.pop()

        if found:
            return

        # Skip current item
        backtrack(pos + 1, current_sum, chosen_indices)

    backtrack(0, 0.0, [])

    if not found:
        return None

    # Map back from sorted/indexed list to original indices
    return solution


@app.post("/find-transactions")
async def find_transactions(
    amount: float = Form(..., description="Target amount, e.g., 5522.48"),
    base_date: date = Form(..., description="Base date in YYYY-MM-DD"),
    lookback_days: int = Form(..., description="Number of days to look back from base date"),
    file: UploadFile = File(..., description="Excel file with transaction data"),
):
    """
    Upload an Excel file + Amount + Date + Lookback days.
    Returns an Excel with transactions in the date window whose sum equals the Amount.
    """
    # 1. Read the Excel file
    file_bytes = await file.read()
    try:
        df = load_transactions_from_excel(file_bytes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading Excel file: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="No valid transactions found in file.")

    print(f"[INFO] Total rows in file: {len(df)}")

    # 2. Filter by date window [base_date - lookback_days, base_date]
    start_date = pd.to_datetime(base_date - timedelta(days=lookback_days))
    end_date = pd.to_datetime(base_date)

    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
    df_window = df.loc[mask].copy()

    print(f"[INFO] Rows in date window: {len(df_window)}")

    if df_window.empty:
        raise HTTPException(
            status_code=404,
            detail="No transactions found in the specified date range.",
        )

    # OPTIONAL: Filter by outcome (e.g., only "Success")
    # df_window = df_window[df_window["outcome"] == "Success"]
    # print(f"[INFO] Rows in date window after outcome filter: {len(df_window)}")
    # if df_window.empty:
    #     raise HTTPException(status_code=404, detail="No matching outcome in date range.")

    # 3. Subset-sum on 'value' (float) to match 'amount' (float)
    values = df_window["value"].astype(float).tolist()

    print(f"[INFO] Target amount (float): {amount}")
    print(f"[INFO] Number of candidate transactions: {len(values)}")
    
    indices = find_subset_sum(values, amount, time_limit_seconds=5.0, tol=1e-6)

    if indices is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "No combination of transactions in the date range sums to the specified amount "
                "(within tolerance, or time limit exceeded while searching)."
            ),
        )

    # 4. Extract matching rows
    df_result = df_window.iloc[indices].copy()
    total_found = df_result["value"].sum()
    print(f"[INFO] Sum of matched transactions: {total_found}")

    # 5. Write to in-memory Excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_result.to_excel(writer, index=False, sheet_name="Matches")

    output.seek(0)

    # amount here is still the exact float you entered; we only format it for the filename
    out_filename = f"matches_{base_date.isoformat()}_{amount}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_filename}"'},
    )
