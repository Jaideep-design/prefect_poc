# -*- coding: utf-8 -*-
"""
Created on Thu Nov  6 11:19:08 2025

@author: Admin
"""
from prefect_github import GitHubRepository
from prefect_wrapper import solax_flow  # replace with your actual flow import

if __name__ == "__main__":
    github_block = GitHubRepository.load("ecozenpoc")

    solax_flow.deploy(
        name="solax-poc",
        work_pool_name="Default_poc_prefect_managed",
        schedule={"cron": "*/10 * * * *", "timezone": "Asia/Kolkata"},
        description="Solax push job running every 10 minutes via Prefect Cloud",
        tags=["solax", "poc"],
        pull_steps=[
            {
                "prefect_github.pull_step": {
                    "repository": github_block.repository,  # automatically filled from block
                    "reference": github_block.reference,
                    "access_token": github_block.access_token.get_secret_value() if github_block.access_token else None,
                }
            }
        ],
    )
