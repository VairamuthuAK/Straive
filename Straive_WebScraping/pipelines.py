from itemadapter import ItemAdapter
import pandas as pd
from .utils import *
from scrapy import signals
import os
import boto3
from datetime import datetime
from botocore.exceptions import ClientError


class StraiveWebscrapingPipeline:

    def __init__(self, settings):
        self.settings = settings

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        crawler.signals.connect(pipeline.spider_closed, signal=signals.spider_closed)
        return pipeline

    def process_item(self, item, spider):
        return item

    # 🔹 SINGLE S3 UPLOAD FUNCTION
    def upload_to_s3(self, file_path, file_name, month , institution_id):
        
        bucket_name = self.settings.get("BUCKET_NAME")
        s3_folder = self.settings.get("S3_FOLDER")

        aws_key = self.settings.get("AWS_ACCESS_KEY_ID")
        aws_secret = self.settings.get("AWS_SECRET_ACCESS_KEY")


        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret
        )

        s3_key = f"{s3_folder}/{month}/{institution_id}/{file_name}"

        try:
            # Check if file exists
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)

            # Delete if exists
            s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
            print(f"🗑️ Deleted existing S3 file: {s3_key}")

        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                print(f"❌ S3 error: {e}")
                return

        # Upload file
        with open(file_path, "rb") as f:
            s3_client.upload_fileobj(f, bucket_name, s3_key)

        print(f"☁️ Uploaded to S3: s3://{bucket_name}/{s3_key}")

    # 🔹 MAIN LOGIC
    def spider_closed(self, spider):
        institution_id = getattr(spider, "institution_id", None)
        
        if not institution_id:
            raise ValueError("Institution ID not found in spider")
        mode = spider.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Parse scrape mode
        if mode == 'course':
            target_types = ['course']
        elif mode == 'directory':
            target_types = ['campus']
        elif mode == 'calendar':
            target_types = ['calendar']
        elif mode in ['course_directory', 'directory_course']:
            target_types = ['course', 'campus']
        elif mode in ['course_calendar', 'calendar_course']:
            target_types = ['course', 'calendar']
        elif mode in ['directory_calendar', 'calendar_directory']:
            target_types = ['campus', 'calendar']
        else:
            target_types = ['course', 'campus', 'calendar']

        print(f"🔍 Processing mode '{mode}' → {target_types}")

        base_path = os.getcwd()
        data_path = os.path.join(base_path, "data")
        final_data_path = os.path.join(base_path, "final_data")

        if not os.path.exists(data_path):
            print(f"❌ Data folder not found: {data_path}")
            return


        os.makedirs(final_data_path, exist_ok=True)

        # 🔹 DELETE OLD FINAL FILES
        for filename in os.listdir(final_data_path):
            if filename.endswith("_final.csv") and str(institution_id) in filename:
                if any(t in filename.lower() for t in target_types):
                    os.remove(os.path.join(final_data_path, filename))
                    print(f"🗑️ Deleted local final file: {filename}")

        # 🔹 PROCESS NEW FILES
        for filename in os.listdir(data_path):
            if not filename.endswith(".csv") or str(institution_id) not in filename:
                continue

            filename_lower = filename.lower()
            if not any(t in filename_lower for t in target_types):
                print(f"⏭️ Skipping {filename}")
                continue

            print(f"✅ Processing {filename}")

            file_path = os.path.join(data_path, filename)
            filename_wo_ext = filename.replace(".csv", "")
            # df = pd.read_csv(file_path, dtype={"Section": str})
            cols = ["Class Number", "Section"]
            # df = pd.read_csv(file_path, dtype={col: str for col in cols})
            df = pd.read_csv(file_path,keep_default_na=False, dtype={col: str for col in cols})

            df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]

            cleaned_df = clean_dataframe(df)

            if 'campus' in filename_lower:
                cleaned_df = validate_df(cleaned_df, "campus")
            elif 'course' in filename_lower:
                cleaned_df = validate_df(cleaned_df, "course")
            elif 'calendar' in filename_lower:
                cleaned_df = validate_df(cleaned_df, "calendar")

            cleaned_df = cleaned_df.drop_duplicates()
            
            current_date = datetime.now().strftime("%Y%m%d")
            month = datetime.now().strftime("%Y%m")
            s3_output_file_name = f"{filename_wo_ext}_{current_date}.csv"
            local_final_data_path = os.path.join(final_data_path, s3_output_file_name)
            cleaned_df.to_csv(local_final_data_path, index=False)
            print(f"✅ Saved local: {s3_output_file_name}")

            # 🔹 UPLOAD TO S3
            # self.upload_to_s3(local_final_data_path, s3_output_file_name , month ,institution_id)




