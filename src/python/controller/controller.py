import json
import time
import subprocess
import os
import sys
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(SCRIPT_DIR, "controller_state.json")
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")

# Ensure log directory exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Intervals in seconds
INTERVAL_30_MIN = 30 * 60
INTERVAL_1_HOUR = 60 * 60
INTERVAL_1_DAY = 24 * 60 * 60

TASKS = [
    {
        "name": "rss_news",
        "command": [sys.executable, "python/rss/fetch_rss_news.py"],
        "interval": INTERVAL_1_HOUR
    },
    {
        "name": "nba_schedule",
        "command": ["Rscript", "R/nba/nba_schedule.r"],
        "interval": INTERVAL_1_HOUR
    },
    {
        "name": "nba_standings",
        "command": ["Rscript", "R/nba/nba_standings.r"],
        "interval": INTERVAL_1_DAY
    },
    {
        "name": "nba_game_log",
        "command": ["Rscript", "R/nba/nba_game_log.r"],
        "interval": INTERVAL_1_DAY
    },
    {
        "name": "nba_players",
        "command": ["Rscript", "R/nba/nba_players.r"],
        "interval": INTERVAL_1_DAY
    }
]

# ==========================================
# HELPERS
# ==========================================
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("Warning: State file corrupted. Starting fresh.")
            return {}
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def run_command(task_name, command):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_file = os.path.join(LOG_DIR, f"{task_name}.log")
    
    print(f"[{timestamp}] Running: {task_name} (Logging to {task_name}.log)")
    
    try:
        with open(log_file, "a") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"START: {timestamp}\n")
            f.write(f"COMMAND: {' '.join(command)}\n")
            f.write(f"{'-'*60}\n")
            f.flush()
            
            # Run and pipe output directly to the log file
            result = subprocess.run(command, stdout=f, stderr=f, check=True)
            
            f.write(f"\n{'-'*60}\n")
            f.write(f"END: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("STATUS: Success\n")
            
        return True
    except subprocess.CalledProcessError as e:
        with open(log_file, "a") as f:
            f.write(f"\nERROR: Command failed with exit code {e.returncode}\n")
        print(f"[{timestamp}] Error in {task_name}: See logs for details.")
        return False
    except Exception as e:
        print(f"[{timestamp}] Critical Error: {str(e)}")
        return False

# ==========================================
# MAIN
# ==========================================
def main():
    # Ensure we are in the project root (parent of python)
    # This assumes the script is located in python/controller/
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up two levels: python/controller -> python -> root
    project_root = os.path.dirname(os.path.dirname(script_dir))
    
    if os.getcwd() != project_root:
        os.chdir(project_root)

    state = load_state()
    now = time.time()
    
    any_run = False

    for task in TASKS:
        name = task["name"]
        interval = task["interval"]
        command = task["command"]
        
        last_run = state.get(name, 0)
        time_since = now - last_run
        
        if time_since >= interval:
            success = run_command(name, command)
            state[name] = now
            any_run = True

    if any_run:
        save_state(state)

if __name__ == "__main__":
    main()
