import os
import scripts.constants as c
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame, functions as F

# I want to explore an approach using PySpark, even though it is not ideal to run in the Airflow environment. 
# The goal of this approach is to exemplify the use of Spark in a scenario where the amount of data is larger and this code would be executed by a cluster.
# I will also make some transformations to the data that I think are important before sending it to the database and address some issues with the data.
class BaseProcessor:
    """
    Centralize methods used for session initialization, file reading, and file writing.
    """
    db_url = f"jdbc:postgresql://{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}"
    db_properties = {
        "user": c.postgres_user,
        "password": c.postgres_password,
        "driver": "org.postgresql.Driver"
    }

    base_source_path:str = "dags/scripts/data_examples/"
    source_file_name:str = None
    source_read_options = {
        "header": True,
        "multiLine": True,
        "sep": ",",
        "escape": "\\"
    }

    def __init__(self) -> None:
        self.spark:SparkSession = SparkSession.builder \
            .appName("Clever Job Processor") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()

    @property
    def full_source_path(self) -> str:
        """Full path to the file to be loaded into the DataFrame."""
        if not self.source_file_name:
            raise Exception("Source file name not set.")
        
        return os.path.join(self.base_source_path, self.source_file_name)

    @property
    def target_table_name(self) -> str:
        """Name of the table containing the data when exported to the database."""
        return self.source_file_name.split(".")[0]

    def _load_data(self) -> SparkDataFrame:
        """Load data from file into spark dataframe."""
        return self.spark.read.csv(self.full_source_path, **self.source_read_options)

    def _transform_data(self, df:SparkDataFrame) -> SparkDataFrame:
        """
        Perform operations on the DataFrame before sending it to the database.
        :param df: DataFrame to be processed.
        """
        return self._load_data()

    def _export_data(self, df:SparkDataFrame) -> None:
        """Export the provided DataFrame to the database."""
        df.write.jdbc(
            url=self.db_url,
            table=self.target_table_name,
            mode="overwrite",
            properties=self.db_properties
        )
    
    def run(self) -> None:
        """Execute the data transformation process and send the DataFrame to the database."""
        raw_data = self._load_data()
        processed_data = self._transform_data(df=raw_data)
        self._export_data(df=processed_data)
    

class FMCSABaseProcessor(BaseProcessor):

    def _transform_data(self, df: SparkDataFrame) -> SparkDataFrame:
        transformed = super()._transform_data(df)
        parse_timestamp = lambda c: F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
        
        return transformed.withColumns(
            {
                "date_created": parse_timestamp("date_created"),
                "date_updated": parse_timestamp("date_updated"),
            }
        )


class FMCSAComplaintsProcessor(FMCSABaseProcessor):
    source_file_name = 'fmcsa_complaints.csv'


class FMCSASaferDataProcessor(FMCSABaseProcessor):
    source_file_name = 'fmcsa_safer_data.csv'
    
    def _transform_data(self, df: SparkDataFrame) -> SparkDataFrame:
        transformed = super()._transform_data(df)
        date_columns = ["oos_date", "mcs_150_form_date", "carrier_safety_rating_rating_date", "carrier_safety_rating_review_date"]
       
        return transformed.withColumns(
            {
                **{c:F.to_date(c, "yyyy-MM-dd") for c in date_columns},
                "cargo_types": F.from_json("cargo_types")
            }
        )


class FMCSACompanySnapshotProcessor(FMCSABaseProcessor):
    source_file_name = 'fmcsa_company_snapshot.csv'


class FMCSACompaniesProcessor(FMCSABaseProcessor):
    source_file_name = 'fmcsa_companies.csv'


class GoogleReviewsProcessor(BaseProcessor):
    source_file_name = 'customer_reviews_google.csv'

    def _transform_data(self, df: SparkDataFrame) -> SparkDataFrame:
        transformed = super()._transform_data(df)
        return transformed.withColumns(
            {
                "reviews": F.col("reviews").cast("int"),
                "author_reviews_count": F.col("author_reviews_count").cast("int"),
                "owner_answer_timestamp_datetime_utc": F.to_timestamp("owner_answer_timestamp_datetime_utc", "MM/dd/yyyy HH:mm"),
                "review_rating": F.col("review_rating").cast("int"),
                "review_datetime_utc": F.to_timestamp("review_datetime_utc", "MM/dd/yyyy HH:mm"),
                "review_likes": F.col("review_likes").cast("int"),
            }
        )


class GoogleMapsCompanyProfilesProcessor(BaseProcessor):
    source_file_name = 'company_profiles_google_maps.csv'
    
    def _transform_data(self, df: SparkDataFrame) -> SparkDataFrame:
        transformed = super()._transform_data(df)
        str_to_bool_col = lambda c: F.when(F.col(c).isNull(), F.lit(None)).otherwise(F.when(F.col(c)=="false", F.lit(False)).otherwise(F.lit(True)))
        
        return transformed.withColumns(
            {
                "subtypes": F.split("subtypes", r",\s"),
                "latitude": F.col("latitude").cast("double"),
                "longitude": F.col("longitude").cast("double"),
                "area_service": str_to_bool_col("area_service"),
                "rating": F.col("rating").cast("float"),
                "reviews_tags": F.split("reviews_tags", r",\s"),
                "photos_count": F.col("photos_count").cast("int"),
                "verified": str_to_bool_col("verified"),
                "reviews": F.col("reviews").cast("int"),
            }
        )
