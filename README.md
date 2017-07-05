Build package
mvn clean install

Download instacart kaggle competition data from https://www.kaggle.com/c/instacart-market-basket-analysis/data
./analyze-instacart-data.sh PATH_TO_DEPARTMENT_DATA PATH_TO_AISLE_DATA PATH_TO_PRODUCT_DATA PATH_TO_ORDER_DATA, PATH_TO_TRAIN_DATA
