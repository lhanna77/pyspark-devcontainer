#!/bin/bash

jupyter nbconvert --execute ad_works_import_parquet.ipynb --to notebook 

echo "ad_works_import_parquet.ipynb ran!"

jupyter nbconvert --execute football_corners.ipynb --to notebook 

echo "football_corners.ipynb ran!"

