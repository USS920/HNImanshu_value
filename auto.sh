et -e

cd /app

echo "===== START $(date) ====="

echo "Downloading CSV files..."

curl -o nifty500_valuation.csv https://csvhsv.s3.ap-south-1.amazonaws.com/web/nifty500_valuation.csv

curl -o niftymicrocap250_valuation.csv https://csvhsv.s3.ap-south-1.amazonaws.com/web/niftymicrocap250_valuation.csv

echo "Files downloaded"

python build.py

echo "===== END $(date) ====="
