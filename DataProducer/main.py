import yfinance
import boto3
import json
from datetime import datetime, timedelta
# event, context
def lambda_handler():
    start_date = (datetime.today() - timedelta(days=10)).strftime('%Y-%m-%d')
    end_date = datetime.today().strftime('%Y-%m-%d')
    interval = '1h'
    company_arr = ['fb', 'adbe', 'nvda', 'tsla']

    fh = boto3.client('firehose', region_name="us-east-1")

    for company in company_arr:
        download = yfinance.Ticker(company).history(start=start_date, end=end_date, interval=interval)

        for index, rows in download.iterrows():
            as_jsonstr = json.dumps({"high": rows.High, "low": rows.Low, "ts": str(index), 'name': company}) #, indent=1, separators=(',', ': ')
            print(as_jsonstr)
            fh.put_record(DeliveryStreamName="PUT-S3-YahooFinance", Record={"Data": as_jsonstr.encode('utf-8')})

    return {
        'statusCode': 200,
        'body': json.dumps(f'Done! Recorded')
    }

lambda_handler()

