import pandas as pd
from collections import Counter
import numpy as np

def process_csv(job_id, file_path, job_status, job_results):
    try:
     
        chunks = pd.read_csv(file_path, chunksize=10000)
        total_rows = 0
        column_counters = {}
        numeric_sums = {}
        numeric_counts = {}

        for chunk in chunks:
            total_rows += len(chunk)
            for col in chunk.columns:
              
                if col not in column_counters:
                    column_counters[col] = Counter()
                column_counters[col].update(chunk[col].dropna().astype(str))

                if pd.api.types.is_numeric_dtype(chunk[col]):
                    if col not in numeric_sums:
                        numeric_sums[col] = 0.0
                        numeric_counts[col] = 0
                    numeric_sums[col] += chunk[col].sum()
                    numeric_counts[col] += chunk[col].count()

        most_freq = {col: counter.most_common(1)[0][0] for col, counter in column_counters.items()}
        averages = {col: numeric_sums[col] / numeric_counts[col] for col in numeric_sums}

        job_results[job_id] = {
            "total_rows": total_rows,
            "most_frequent_values": most_freq,
            "averages": averages
        }
       
    except Exception as e:
        job_status[job_id] = "failed"
        job_results[job_id] = {"error": str(e)}
