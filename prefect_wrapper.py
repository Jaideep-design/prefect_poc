# %%
from pathlib import Path
from prefect import flow, task
import pytz
from datetime import datetime

@task(log_prints=True, retries=1)
def run_main():
    # Resolve the path relative to this file, not your local drive
    script_path = Path(__file__).resolve().parent / "SolarAC_solax_mobile_App_push_script.py"

    start_time = datetime.now(pytz.timezone("Asia/Kolkata"))
    print(f"Starting Solax job at {start_time}")

    try:
        with open(script_path) as f:
            code = compile(f.read(), script_path.name, "exec")
            exec(code, {})
    except Exception as e:
        print("Error while running script:", e)
        raise

@flow(name="solax_flow")
def solax_flow():
    run_main()

if __name__ == "__main__":
    solax_flow()

# %%
# # prefect_wrapper.py
# from prefect import flow, task
# import SolarAC_solax_mobile_App_push_script as solax
# import pytz
# from datetime import datetime
# import os

# SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "../SolarAC_solax_mobile_App_push_script.py")
# @task(log_prints=True, retries=1)
# def run_main():
#     solax_start = datetime.now(pytz.timezone("Asia/Kolkata"))
#     print(f"Starting Solax job at {solax_start}")
#     try:
#         if hasattr(solax, "main"):
#             solax.main()
#         else:
#             # If your script doesn't have a main() function, just execute its code
#             with open(SCRIPT_PATH) as f:
#                 code = compile(f.read(), "SolarAC_solax_mobile_App_push_script.py", "exec")
#                 exec(code, {})
#     except Exception as e:
#         print("Error while running script:", e)
#         raise

# @flow(name="Solax Push Script PoC")
# def solax_flow():
#     run_main()

# if __name__ == "__main__":
#     solax_flow()
