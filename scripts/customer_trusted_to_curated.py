import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1704906390348 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kamal-data-lake/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1704906390348",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1704906433212 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kamal-data-lake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1704906433212",
)

# Script generated for node Join
Join_node1704907036422 = Join.apply(
    frame1=CustomerTrusted_node1704906433212,
    frame2=AccelerometerTrusted_node1704906390348,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1704907036422",
)

# Script generated for node Drop Fields
DropFields_node1704907073505 = DropFields.apply(
    frame=Join_node1704907036422,
    paths=["user", "z", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1704907073505",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1704970849212 = DynamicFrame.fromDF(
    DropFields_node1704907073505.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1704970849212",
)

# Script generated for node Amazon S3
AmazonS3_node1704907138121 = glueContext.getSink(
    path="s3://kamal-data-lake/customer/curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=["email"],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1704907138121",
)
AmazonS3_node1704907138121.setCatalogInfo(
    catalogDatabase="stedi-lakehosue-kamal", catalogTableName="customer_trusted"
)
AmazonS3_node1704907138121.setFormat("json")
AmazonS3_node1704907138121.writeFrame(DropDuplicates_node1704970849212)
job.commit()
