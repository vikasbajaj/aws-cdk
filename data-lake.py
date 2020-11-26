from aws_cdk import (
    aws_s3 as s3,
    aws_glue as glue,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
    aws_kinesis as data_stream,
    aws_kinesisfirehose as delivery_stream,
    core
)

# S3 -> (upload taxi trip data) -> Glue Crawler -> Glue catalog -> Glue ETL -> Parquet -> S3 -> Catalog -> Athena

class GlueCatalogStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # create a S3 bucket to hold raw data
        raw_bucket = s3.Bucket(self, 'new_york_taxi_yellow_tripdata_2020-03_raw')
        
        # Ingestion 1 - upload data to the raw bucket
        s3deploy.BucketDeployment(self, 'DeployFiles',
              sources=[s3deploy.Source.asset('../data/')], 
              destination_bucket=raw_bucket, 
              destination_key_prefix='trip');
        
        # Ingestion 2 - Kinesis
        kds = data_stream.Stream(self,'data_stream',shard_count=1)

        delivery_stream_role=iam.Role(self,
                                    'kdfdelivery_stream_role_role',
                                    assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'))

        delivery_stream_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('AmazonKinesisFullAccess'));

        delivery_stream_role.add_to_policy(iam.PolicyStatement(
                                            effect=iam.Effect.ALLOW,
                                            resources=[raw_bucket.bucket_arn],
                                            actions=["s3:*"]))

        s3_dest_config = delivery_stream.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                                        bucket_arn=raw_bucket.bucket_arn,
                                        buffering_hints=delivery_stream.CfnDeliveryStream.BufferingHintsProperty(interval_in_seconds=60,size_in_m_bs=128),
                                        role_arn=delivery_stream_role.role_arn,
                                        compression_format='UNCOMPRESSED',
                                        s3_backup_mode='Disabled')

        stream_source_config = delivery_stream.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                                            kinesis_stream_arn=kds.stream_arn,
                                             role_arn=delivery_stream_role.role_arn)

        kfirehose=delivery_stream.CfnDeliveryStream(self,
                                                    'kfirehose',
                                                    delivery_stream_name='deliverystream',
                                                    delivery_stream_type='KinesisStreamAsSource',
                                                    extended_s3_destination_configuration=s3_dest_config,
                                                    kinesis_stream_source_configuration=stream_source_config)

        # create a Glue DB
        glue_raw_db = glue.Database(self, "NewYorkTaxiRawDatabase",
            database_name="new_york_taxi_raw_database"
        )
        
        # create policies for Glue Crawler
        glue_crawler_policy = iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:GetObject",],
                    resources=[f'{raw_bucket.bucket_arn}/*'])
        glue_crawler_document = iam.PolicyDocument()
        glue_crawler_document.add_statements(glue_crawler_policy)
        
        glue_crawler_role = iam.Role(
            self,
            "glue_crawler_role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')],
            inline_policies=[glue_crawler_document],
        )
        
        # create Glue Crawler
        glue_crawler = glue.CfnCrawler(
            self, 'glue_crawler',
            database_name=glue_raw_db.database_name,
            role=glue_crawler_role.role_arn,
            targets={
                "s3Targets": [{ "path": f'{raw_bucket.bucket_name}/trip'}]
            },
        )
        
        # create a S3 bucket to hold transformed Parquet file
        processed_bucket = s3.Bucket(self, 'new_york_taxi_yellow_tripdata_2020-03_processed')
        
        # create Glue ETL job to convert CSV to Parquet
        # glueJobName = 'glue_etl'
        # glueETLJob = glue.CfnJob(
        #     self,
        #     glueJobName,
        #     command = glue.CfnJob.JobCommandProperty(
        #         name = glueJobName,
        #         python_version= '3',
        #         script_location = config_bucket_arn + "/code/gluejob.py"
        #     ),
        #     role=glueJobRole.role_arn,
        #     glue_version='1.0',
        #     max_retries=0,
        #     timeout=30,
        #     security_configuration=glueSecurityConfiguration.ref,
        #     default_arguments={ '--job-bookmark-option': 'job-bookmark-enable' },
        #     description="glue job to convert CSV to Parquet"
        # )
        
        # create a processed DB 
        glue.Database(self, "NewYorkTaxiProcessedDatabase",
            database_name="new_york_taxi_processed_database"
        )
        
        # TODO: create table using crawlers
