## End-to-end Hypercharge Postgres Example - Stock Price Dataset

Features used:
- Init project from yaml
- Ingestion from postgresql source
- SQL execution
- Creation of embeddable React widget

### Initialize Postgres with Stock Price Datasets

Check out [kaggle](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset) for more details on this open source dataset.

#### Pre-processing of the dataset
```bash
# Remove headers from all tables
sed -i '' 1d *.csv

# Combine tables into one csv
INDEX=0
for f in *.csv
do
    if [[ ${INDEX} -le 2 ]]; then
        INDEX=${INDEX}+1
        sed -e "s/^/${f%.*},/" $f >> stock_price.csv
    fi
done
```

## Run

```bash
docker-compose up --build
```

```bash
cargo run
```
