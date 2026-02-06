name: Upload 3 news to Notion

on:
  schedule:
    - cron: "0 23 * * *"  # UTC 23:00 = KST 08:00
  workflow_dispatch:

jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run script
        env:
          NOTION_TOKEN: ${{ secrets.NOTION_TOKEN }}
          NEWS_DB_ID: ${{ secrets.NEWS_DB_ID }}
          TERMS_DB_ID: ${{ secrets.TERMS_DB_ID }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
        run: |
          python main.py
