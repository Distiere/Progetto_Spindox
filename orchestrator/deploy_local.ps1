
prefect work-pool create -t process phase2-pool

# Deploy della pipeline con schedule cron
prefect deploy `
  etl/flows/run_pipeline_scheduled.py:phase2_flow `
  -n phase2-scheduled `
  -p phase2-pool `
  --cron "0 2 * * *"
