# -*- coding: utf-8 -*-
"""
Created on Thu Nov  6 11:19:08 2025

@author: Admin
"""
from prefect_github import GitHubRepository
from prefect_wrapper import solax_flow  # import your flow

if __name__ == "__main__":
    # Load your GitHub block (created in Prefect Cloud > Blocks > GitHub)
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
                    # These are the correct fields for prefect-github >=0.3.0
                    "repository": github_block.url,
                    "reference": github_block.branch or "main",
                    "access_token": (
                        github_block.access_token.get_secret_value()
                        if github_block.access_token
                        else None
                    ),
                }
            }
        ],
    )
