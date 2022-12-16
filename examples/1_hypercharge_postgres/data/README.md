### Kaggle Stock Price Datasets

This test case is based on Kaggle's Stock Price Datasets and pre-processed to be used.
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