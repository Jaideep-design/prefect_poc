# -*- coding: utf-8 -*-
"""
Created on Thu Nov  6 11:19:08 2025

@author: Admin
"""
from prefect_github import GitHubRepository
from prefect_wrapper import solax_flow

if __name__ == "__main__":
    # Load your GitHub block
    github_block = GitHubRepository.load("ecozenpoc")

    # Deploy the flow
    solax_flow.deploy(
        name="solax-poc",
        work_pool_name="Default_poc_prefect_managed",
        schedule={"cron": "*/10 * * * *", "timezone": "Asia/Kolkata"},
        description="Solax push job running every 10 minutes via Prefect Cloud",
        tags=["solax", "poc"],
        pull_steps=[
            {
                "prefect_github.pull_step": {
                    "repository": github_block.repository_url,
                    "reference": github_block.reference or "main",
                    "access_token": None,  # only needed if your repo is private
                }
            }
        ],
    )
