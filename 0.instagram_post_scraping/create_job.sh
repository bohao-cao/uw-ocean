gcloud run jobs create job-quickstart \
    --image gcr.io/upwork-ocean-classification/logger-job \
    --tasks 1 \
    --set-env-vars file_name=df_buy_insta_accounts_add_on.tsv \
    --set-env-vars sleep=30 \
    --max-retries 1 \
    --region us-central1 \
    --project=upwork-ocean-classification