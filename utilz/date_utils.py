from datetime import datetime

def get_ingestion_date_str(timestamp):
    date_time = datetime.fromtimestamp(float(timestamp))
    date_time = date_time.strftime("%Y-%m-%d")
    return date_time

def get_ingestion_date_str_milliseconds(timestamp):
    date_time = datetime.fromtimestamp(int(timestamp)/1000)
    date_time = date_time.strftime("%Y-%m-%d")
    return date_time

if __name__ == "__main__":
    print(get_ingestion_date_str("238032000000"))