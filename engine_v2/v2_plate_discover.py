"""
v2_plate_discover.py
Task: High-Resolution Field Mapping for getHisPlates (2026-03-26)
"""
import sys
sys.path.append('/usr/local/lib/python3.9/site-packages/pykaipan')
from pykaipan.pykaipan import getHisPlates

def plate_forensics(date_str):
    print(f"--- Forensic Index Mapping for Plates on {date_str} ---")
    try:
        res = getHisPlates(date=date_str)
        p_list = res.get('list', [])
        if p_list and len(p_list) > 0:
            print(f"Total Plates found: {len(p_list)}")
            sample = p_list[0]
            print(f"Sample Record Length: {len(sample)}")
            for i, val in enumerate(sample):
                print(f"  Field [{i}]: {val} (Type: {type(val).__name__})")
        else:
            print("No plate data found.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    plate_forensics("2026-03-26")
