from aws_cdk import (
    aws_iam as iam,
    aws_glue as glue,
    aws_s3 as s3,
    aws_kinesis as data_stream,
    aws_kinesisfirehose as delivery_stream,
    aws_athena as athena,
    core
)



#KDS -----> KDF -------> S3 -------> Glue Crawler (data catalog) -------> Glue Job ----(Transaformation + Load) ----> save the processed to S3 ---> Athena/Redshift/QuickSight
#BUCKET='yellowtaxicdk'

class CdkworkshopStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        s3bucket = s3.Bucket(self, 'vika-yy')
        kds = data_stream.Stream(self,'data_stream',shard_count=1)

        delivery_stream_role=iam.Role(self,
                                    'kdfdelivery_stream_role_role',
                                    assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'))
        delivery_stream_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('AmazonKinesisFullAccess'));
        delivery_stream_role.add_to_policy(iam.PolicyStatement(
                                            effect=iam.Effect.ALLOW,
                                            resources=[s3bucket.bucket_arn],
                                            actions=["s3:*"]))

        #s3bucket = s3.Bucket(self, 'vika-yy',bucket_name='yellowtaxicdk-input')
        s3_dest_config = delivery_stream.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                                        bucket_arn=s3bucket.bucket_arn,
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
        glue_role=iam.Role(self, 
                          'glue_role', 
                           assumed_by=iam.ServicePrincipal('glue.amazonaws.com'))
        glue_role.add_to_policy(iam.PolicyStatement(
                                effect=iam.Effect.ALLOW,
                                resources=[s3bucket.bucket_arn],
                                actions=["s3:*"]))
        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))
        
        bucket_name=s3bucket.bucket_name
        glue_crawler = glue.CfnCrawler(
                                self, 'glue_crawler',
                                database_name='yellow-taxis',
                                role=glue_role.role_arn,
                                #targets={"s3Targets": [{"path": f'{BUCKET}/input/'}]}
                                targets={"s3Targets": [{"path": f'{bucket_name}/input/'}]}
                            )
    
        
    
    

       
