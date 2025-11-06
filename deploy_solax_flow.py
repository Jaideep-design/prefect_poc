# -*- coding: utf-8 -*-
"""
Created on Thu Nov  6 11:19:08 2025

@author: Admin
"""
from prefect_github import GitHubRepository
from prefect_wrapper import solax_flow

if __name__ == "__main__":
    # Load your GitHub block from Prefect Cloud
    github_block = GitHubRepository.load("ecozenpoc")

    # Deploy the flow to Prefect Cloud
    solax_flow.deploy(
        name="solax-poc",
        work_pool_name="Default_poc_prefect_managed",
        schedule={"cron": "*/10 * * * *", "timezone": "Asia/Kolkata"},
        description="Solax push job running every 10 minutes via Prefect Cloud",
        tags=["solax", "poc"],
        pull_action=github_block,  # ✅ correct for Prefect >=3.1
    )

