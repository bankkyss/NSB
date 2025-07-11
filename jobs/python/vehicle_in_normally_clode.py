import logging
import argparse
import requests
import redis
from uuid import uuid4, uuid5
from datetime import datetime, timedelta
import json
from typing import Dict, Any, Optional, Iterator
from contextlib import contextmanager
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructType, StructField, TimestampType

# Configuration constants
DEFAULT_SPEED_THRESHOLD = 300  # km/h
DEFAULT_REDIS_TTL = 1200  # seconds
DEFAULT_REQUEST_TIMEOUT = 10  # seconds
DEFAULT_FETCHSIZE = 10000

class VehicleSpeedAnalyzer:
    """Main class for vehicle speed analysis with ghost detection."""
    
    def __init__(self, args):
        self.args = args
        self.logger = self._setup_logger()
        self.spark = self._create_spark_session()
        self.processing_timestamp = datetime.now()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        spark = (
            SparkSession.builder
            .appName("VehicleSpeedAnalysisWithKafkaOSRM")
            .config("spark.jars.packages", 
                   "org.postgresql:postgresql:42.2.23,"
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    @contextmanager
    def _get_redis_client(self):
        """Context manager for Redis connection."""
        client = None
        try:
            client = redis.Redis(
                host=self.args.redis_host,
                port=self.args.redis_port,
                db=self.args.redis_db,
                password=self.args.redis_password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True
            )
            # Test connection
            client.ping()
            yield client
        except Exception as e:
            self.logger.error(f"Redis connection failed: {e}")
            yield None
        finally:
            if client:
                client.close()
    
    def load_data_from_postgres(self):
        """Load data from PostgreSQL with proper error handling."""
        dbtable_query = (
            f"(SELECT * FROM {self.args.postgres_table} "
            f"WHERE event_time >= NOW() - INTERVAL '{self.args.lookback_hours} hours' "
            f"ORDER BY car_id, event_time) AS filtered_data"
        )
        
        jdbc_url = f"jdbc:postgresql://{self.args.postgres_host}:{self.args.postgres_port}/{self.args.postgres_db}"
        
        try:
            df = self.spark.read.format("jdbc").options(
                url=jdbc_url,
                dbtable=dbtable_query,
                user=self.args.postgres_user,
                password=self.args.postgres_password,
                driver="org.postgresql.Driver",
                fetchsize=str(DEFAULT_FETCHSIZE),
                numPartitions="4"  # Optimize partitioning
            ).load()
            
            record_count = df.count()
            self.logger.info(f"Loaded {record_count} records from PostgreSQL.")
            
            if record_count == 0:
                self.logger.warning("No data loaded from PostgreSQL.")
                return None
                
            return df.cache()
            
        except Exception as e:
            self.logger.error(f"Failed to load data from PostgreSQL: {e}", exc_info=True)
            raise
    
    def prepare_data_with_previous_events(self, df):
        """Prepare data with previous event information."""
        vehicle_window = Window.partitionBy("car_id").orderBy("event_time")
        
        # Add previous event data
        df_with_prev = df
        for col_name in df.columns:
            df_with_prev = df_with_prev.withColumn(
                f"prev_{col_name}", 
                F.lag(col_name).over(vehicle_window)
            )
        
        # Filter for valid consecutive events
        df_filtered = (df_with_prev
            .filter(F.col("prev_car_id").isNotNull())
            .filter(
                (F.col("event_time") - F.col("prev_event_time")) <= 
                F.expr(f"INTERVAL {self.args.time_threshold_seconds} SECONDS")
            )
            .cache()
        )
        
        self.logger.info(f"Filtered to {df_filtered.count()} consecutive event pairs.")
        return df_filtered
    
    def get_osrm_distance(self, lat1: float, lon1: float, lat2: float, lon2: float, 
                         redis_client, cache_key: str) -> Optional[float]:
        """Get distance from OSRM API with caching."""
        try:
            # Check cache first
            if redis_client:
                cached_distance = redis_client.get(cache_key)
                if cached_distance:
                    return float(cached_distance)
            
            # Make OSRM API call
            request_url = (
                f"{self.args.osrm_server_url}/route/v1/driving/"
                f"{lon1},{lat1};{lon2},{lat2}?overview=false"
            )
            
            response = requests.get(request_url, timeout=DEFAULT_REQUEST_TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            if data.get('code') == 'Ok' and data.get('routes'):
                distance_meters = data['routes'][0]['distance']
                distance_km = distance_meters / 1000.0
                
                # Cache the result
                if redis_client:
                    redis_client.setex(cache_key, DEFAULT_REDIS_TTL, distance_km)
                
                return distance_km
                
        except Exception as e:
            self.logger.warning(f"OSRM API call failed: {e}")
            
        return None
    
    def process_partition(self, iterator: Iterator) -> Iterator[Dict[str, Any]]:
        """Process partition for distance and speed calculation."""
        with self._get_redis_client() as redis_client:
            for row in iterator:
                row_dict = row.asDict()
                
                # Extract coordinates
                lat1, lon1 = row_dict.get('prev_gps_latitude'), row_dict.get('prev_gps_longitude')
                lat2, lon2 = row_dict.get('gps_latitude'), row_dict.get('gps_longitude')
                cam1, cam2 = row_dict.get('prev_camera_name'), row_dict.get('camera_name')
                
                distance_km = None
                
                # Calculate distance if coordinates are available
                if all(v is not None for v in [lat1, lon1, lat2, lon2, cam1, cam2]):
                    cache_key = f"osrm_dist:{':'.join(sorted([str(cam1), str(cam2)]))}"
                    distance_km = self.get_osrm_distance(lat1, lon1, lat2, lon2, redis_client, cache_key)
                
                # Calculate time difference and speed
                time_delta_seconds = 0
                speed_kmh = 0.0
                
                if row_dict.get('event_time') and row_dict.get('prev_event_time'):
                    time_delta_seconds = (
                        row_dict['event_time'] - row_dict['prev_event_time']
                    ).total_seconds()
                    
                    if time_delta_seconds > 0 and distance_km is not None:
                        time_delta_hours = time_delta_seconds / 3600.0
                        speed_kmh = round(distance_km / time_delta_hours, 2)
                
                # Add calculated fields
                row_dict.update({
                    'distance_km': distance_km,
                    'time_delta_seconds': time_delta_seconds,
                    'speed_kmh': speed_kmh
                })
                
                yield row_dict
    
    def calculate_speeds(self, df):
        """Calculate speeds using OSRM API and Redis caching."""
        # Define output schema
        output_schema = StructType(df.schema.fields + [
            StructField("distance_km", FloatType(), True),
            StructField("time_delta_seconds", FloatType(), True),
            StructField("speed_kmh", FloatType(), True)
        ])
        
        # Process data using mapPartitions
        results_rdd = df.rdd.mapPartitions(self.process_partition)
        results_df = self.spark.createDataFrame(results_rdd, schema=output_schema)
        
        # Add UUIDs for tracking
        namespace_uuid = uuid4()
        namespace_broadcast = self.spark.sparkContext.broadcast(namespace_uuid)
        
        def generate_uuid(plate: str) -> str:
            return str(uuid5(namespace_broadcast.value, plate)) if plate else str(uuid4())
        
        uuid_udf = F.udf(generate_uuid, StringType())
        
        results_df_with_uuid = results_df.withColumn(
            "record_uuid", uuid_udf(F.col("license_plate"))
        ).withColumn(
            "prev_record_uuid", uuid_udf(F.col("prev_license_plate"))
        )
        
        return results_df_with_uuid
    
    def write_to_kafka(self, df, topic: str):
        """Write DataFrame to Kafka topic."""
        if df.rdd.isEmpty():
            self.logger.info(f"No records to write to Kafka topic: {topic}")
            return
        
        record_count = df.count()
        self.logger.info(f"Writing {record_count} records to Kafka topic: {topic}")
        
        try:
            (df.select(F.col("value").cast(StringType()))
             .write
             .format("kafka")
             .option("kafka.bootstrap.servers", self.args.kafka_brokers)
             .option("topic", topic)
             .option("kafka.acks", "all")  # Ensure reliability
             .option("kafka.retries", "3")
             .save())
            
            self.logger.info(f"Successfully wrote to Kafka topic: {topic}")
            
        except Exception as e:
            self.logger.error(f"Failed to write to Kafka topic {topic}: {e}", exc_info=True)
            raise
    
    def create_log_events(self, high_speed_df):
        """Create individual log events for each detection."""
        # Current events
        current_events = high_speed_df.select(
            F.col("record_uuid"),
            F.col("car_id"),
            F.col("license_plate"),
            F.col("province"),
            F.col("vehicle_brand"),
            F.col("vehicle_color"),
            F.col("camera_name"),
            F.col("camera_id"),
            F.col("event_time"),
            F.col("event_date"),
            F.col("gps_latitude"),
            F.col("gps_longitude"),
            F.col("created_at"),
            F.lit("ghost_detection").alias("event_type"),
            F.lit("current").alias("event_position")
        )
        
        # Previous events
        previous_events = high_speed_df.select(
            F.col("prev_record_uuid").alias("record_uuid"),
            F.col("prev_car_id").alias("car_id"),
            F.col("prev_license_plate").alias("license_plate"),
            F.col("prev_province").alias("province"),
            F.col("prev_vehicle_brand").alias("vehicle_brand"),
            F.col("prev_vehicle_color").alias("vehicle_color"),
            F.col("prev_camera_name").alias("camera_name"),
            F.col("prev_camera_id").alias("camera_id"),
            F.col("prev_event_time").alias("event_time"),
            F.col("prev_event_date").alias("event_date"),
            F.col("prev_gps_latitude").alias("gps_latitude"),
            F.col("prev_gps_longitude").alias("gps_longitude"),
            F.col("prev_created_at").alias("created_at"),
            F.lit("ghost_detection").alias("event_type"),
            F.lit("previous").alias("event_position")
        )
        
        # Union events
        all_events = current_events.unionByName(previous_events)
        
        # Create JSON payload
        log_event_df = all_events.select(
            F.to_json(F.struct(*all_events.columns)).alias("value")
        )
        
        return log_event_df
    
    def create_alert_events(self, high_speed_df):
        """Create alert events with aggregated information."""
        alert_df = high_speed_df.select(
            F.to_json(
                F.struct(
                    F.col("license_plate"),
                    F.array("prev_record_uuid", "record_uuid").alias("record_uuid_list"),
                    F.array("prev_camera_id", "camera_id").alias("camera_ids"),
                    F.array("prev_camera_name", "camera_name").alias("camera_names"),
                    F.array("prev_event_time", "event_time").alias("event_times"),
                    F.array("prev_gps_latitude", "gps_latitude").alias("gps_latitudes"),
                    F.array("prev_gps_longitude", "gps_longitude").alias("gps_longitudes"),
                    F.col("distance_km"),
                    F.col("time_delta_seconds"),
                    F.col("speed_kmh"),
                    F.lit("ghost_detection").alias("alert_type"),
                    F.current_timestamp().alias("alert_timestamp")
                )
            ).alias("value")
        )
        
        return alert_df
    
    def run(self):
        """Main execution method."""
        try:
            self.logger.info(f"Starting vehicle speed analysis with args: {vars(self.args)}")
            
            # Load data
            df = self.load_data_from_postgres()
            if df is None:
                return
            
            # Prepare data with previous events
            df_with_prev = self.prepare_data_with_previous_events(df)
            if df_with_prev.rdd.isEmpty():
                self.logger.warning("No consecutive events found. Stopping execution.")
                return
            
            # Calculate speeds
            results_df = self.calculate_speeds(df_with_prev)
            
            # Filter high-speed events
            high_speed_df = results_df.filter(
                F.col("speed_kmh") > DEFAULT_SPEED_THRESHOLD
            ).cache()
            
            high_speed_count = high_speed_df.count()
            self.logger.info(f"Found {high_speed_count} high-speed events (speed > {DEFAULT_SPEED_THRESHOLD} km/h).")
            
            if high_speed_count > 0:
                # Create and send log events
                log_events = self.create_log_events(high_speed_df)
                self.write_to_kafka(log_events, self.args.kafka_log_event_topic)
                
                # Create and send alert events
                alert_events = self.create_alert_events(high_speed_df)
                self.write_to_kafka(alert_events, self.args.kafka_alerts_topic)
            
            self.logger.info("Processing completed successfully.")
            
        except Exception as e:
            self.logger.error(f"Processing failed: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()
            self.logger.info("Spark session stopped.")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Vehicle Speed Analysis with Kafka Alerts using OSRM and Redis Cache"
    )
    
    # PostgreSQL parameters
    parser.add_argument('--postgres-host', type=str, required=True, help='PostgreSQL host')
    parser.add_argument('--postgres-port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--postgres-db', type=str, required=True, help='PostgreSQL database name')
    parser.add_argument('--postgres-user', type=str, required=True, help='PostgreSQL username')
    parser.add_argument('--postgres-password', type=str, required=True, help='PostgreSQL password')
    parser.add_argument('--postgres-table', type=str, default='vehicle_events', help='PostgreSQL table name')
    
    # Kafka parameters
    parser.add_argument('--kafka-brokers', type=str, required=True, 
                       help='Kafka bootstrap servers (e.g., "host1:9092,host2:9092")')
    parser.add_argument('--kafka-alerts-topic', type=str, default='alerts_topic', 
                       help='Kafka topic for high-speed alerts')
    parser.add_argument('--kafka-log-event-topic', type=str, default='log_event_topic', 
                       help='Kafka topic for event logs')
    
    # Analysis parameters
    parser.add_argument('--lookback-hours', type=int, default=24, 
                       help='Hours to look back for data analysis')
    parser.add_argument('--time-threshold-seconds', type=int, default=300, 
                       help='Time threshold in seconds for filtering interesting events')
    
    # OSRM parameters
    parser.add_argument('--osrm-server-url', type=str, default="http://router.project-osrm.org", 
                       help='URL of the OSRM backend server')
    
    # Redis parameters
    parser.add_argument('--redis-host', type=str, default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-db', type=int, default=0, help='Redis database number')
    parser.add_argument('--redis-password', type=str, help='Redis password (if required)')
    
    return parser.parse_args()

def main():
    """Main entry point."""
    args = parse_arguments()
    analyzer = VehicleSpeedAnalyzer(args)
    analyzer.run()

if __name__ == "__main__":
    main()