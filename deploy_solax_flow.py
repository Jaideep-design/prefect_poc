# -*- coding: utf-8 -*-
"""
Created on Thu Nov  6 11:19:08 2025

@author: Admin
"""

from prefect_wrapper import solax_flow

if __name__ == "__main__":
    solax_flow.deploy(
        name="solax-poc",
        work_pool_name="Default_poc_prefect_managed",  # your Prefect managed work pool
        schedule={"cron": "*/10 * * * *", "timezone": "UTC"},
        description="Solax push job running every 10 minutes via Prefect Cloud",
        tags=["solax", "poc"],
    )
