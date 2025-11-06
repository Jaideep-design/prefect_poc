# -*- coding: utf-8 -*-
"""
Created on Thu Nov  6 11:19:08 2025

@author: Admin
"""
from prefect_github import GitHubRepository
from prefect_wrapper import solax_flow

if __name__ == "__main__":
    # Load your Prefect GitHub storage block
    github_block = GitHubRepository.load("ecozenpoc")

    # Deploy your Solax flow
    solax_flow.deploy(
        name="solax-poc",
        work_pool_name="Default_poc_prefect_managed",  # Your managed work pool
        job_variables={},  # Leave empty for now
        schedule={"cron": "*/10 * * * *", "timezone": "Asia/Kolkata"},  # Every 10 min
        description="Solax push job running every 10 minutes via Prefect Cloud",
        tags=["solax", "poc"],
        storage_block=github_block,  # ✅ Correct argument name
    )

