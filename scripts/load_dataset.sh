#!/bin/bash
REDUCED=$1
MULTICLIENT=$2

if [ ! -f ~/Downloads/g-coffee-shop-transaction-202307-to-202506.zip ]; then
  curl -L -o ~/Downloads/g-coffee-shop-transaction-202307-to-202506.zip\
    https://www.kaggle.com/api/v1/datasets/download/geraldooizx/g-coffee-shop-transaction-202307-to-202506
fi

unzip -o ~/Downloads/g-coffee-shop-transaction-202307-to-202506.zip -d ~/Downloads

mkdir -p ./data/transactions ./data/transactions_items ./data/users ./data/stores ./data/menu_items

cp -rf ~/Downloads/transactions/* ./data/transactions/ 2>/dev/null || true
cp -rf ~/Downloads/transaction_items/* ./data/transactions_items/ 2>/dev/null || true
cp -rf ~/Downloads/users/* ./data/users/ 2>/dev/null || true
cp -rf ~/Downloads/stores/* ./data/stores/ 2>/dev/null || true
cp -rf ~/Downloads/menu_items/* ./data/menu_items/ 2>/dev/null || true

# Clean up the Downloads directory
rm -rf ~/Downloads/vouchers ~/Downloads/payment_methods ~/Downloads/transactions ~/Downloads/transaction_items ~/Downloads/users ~/Downloads/stores ~/Downloads/menu_items

if [ "$REDUCED" -eq 1 ]; then
  echo "Creating reduced dataset..."
  # Delete 2023 (July-December)
  rm -rf ./data/transactions_items/transaction_items_2023{07,08,09,10,11,12}.csv ./data/transactions/transactions_2023{07,08,09,10,11,12}.csv
  # Delete 2024 (February-December, skip January)
  rm -rf ./data/transactions_items/transaction_items_2024{02,03,04,05,06,07,08,09,10,11,12}.csv ./data/transactions/transactions_2024{02,03,04,05,06,07,08,09,10,11,12}.csv
  # Delete 2025 (February-June, skip January)
  rm -rf ./data/transactions_items/transaction_items_2025{02,03,04,05,06}.csv ./data/transactions/transactions_2025{02,03,04,05,06}.csv
elif [ "$MULTICLIENT" -eq 1 ]; then
    echo "Creating multiclient dataset..."
    mkdir -p ./data1/transactions ./data1/transactions_items ./data1/users ./data1/stores ./data1/menu_items
    mkdir -p ./data2/transactions ./data2/transactions_items ./data2/users ./data2/stores ./data2/menu_items

    cp -r ./data/transactions/transactions_2023{07,08,09}.csv ./data/transactions/transactions_2024{01,02,03,04,05,06}.csv ./data/transactions/transactions_2025{01,02,03}.csv data1/transactions
    cp -r ./data/transactions_items/transaction_items_2023{07,08,09}.csv ./data/transactions_items/transaction_items_2024{01,02,03,04,05,06}.csv ./data/transactions_items/transaction_items_2025{01,02,03}.csv data1/transactions_items

    cp -r ./data/transactions/transactions_2023{10,11,12}.csv ./data/transactions/transactions_2024{07,08,09,10,11,12}.csv ./data/transactions/transactions_2025{04,05,06}.csv data2/transactions
    cp -r ./data/transactions_items/transaction_items_2023{10,11,12}.csv ./data/transactions_items/transaction_items_2024{07,08,09,10,11,12}.csv ./data/transactions_items/transaction_items_2025{04,05,06}.csv data2/transactions_items

    cp -r ./data/menu_items/* data1/menu_items
    cp -r ./data/menu_items/* data2/menu_items

    cp -r ./data/users/* data1/users
    cp -r ./data/users/* data2/users

    cp -r ./data/stores/* data1/stores
    cp -r ./data/stores/* data2/stores
fi
