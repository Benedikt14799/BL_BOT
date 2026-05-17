# scratch/analyze_logs.py
import re
import sys
from datetime import datetime

# Configure stdout to be UTF-8 safe on Windows
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

log_file_path = "sync_inventory.log"

def analyze():
    print("Starte Log-Analyse von sync_inventory.log...")
    block_pattern = re.compile(r"Blockade|429|407|NO_USER")
    progress_pattern = re.compile(r"Progress: (\d+)/(\d+) \(ok=(\d+), gefiltert=(\d+), errors=(\d+)\)")
    
    total_lines = 0
    blocks = []
    progress_updates = []
    errors = []
    
    with open(log_file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            total_lines += 1
            if "Blockade" in line or "429" in line or "407" in line or "NO_USER" in line:
                blocks.append((total_lines, line.strip()))
            
            match = progress_pattern.search(line)
            if match:
                progress_updates.append((total_lines, line.strip()))
                
            if "ERROR" in line or "CRITICAL" in line:
                errors.append((total_lines, line.strip()))
                
    # Safe printing helper
    def safe_print(text):
        print(text.encode('utf-8', errors='replace').decode('utf-8'))
        
    safe_print(f"Gesamtzeilen im Log: {total_lines}")
    
    safe_print(f"\nGefundene Blockaden / Proxy-Fehler (Gesamt: {len(blocks)}):")
    for line_num, block in blocks[-15:]:
        safe_print(f"Zeile {line_num}: {block}")
        
    safe_print(f"\nLetzte 15 Progress-Updates:")
    for line_num, prog in progress_updates[-15:]:
        safe_print(f"Zeile {line_num}: {prog}")
        
    safe_print(f"\nLetzte 15 Errors/Criticals:")
    for line_num, err in errors[-15:]:
        safe_print(f"Zeile {line_num}: {err}")

if __name__ == "__main__":
    analyze()
