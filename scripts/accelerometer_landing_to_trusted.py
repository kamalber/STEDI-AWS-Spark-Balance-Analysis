import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1704891056704 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kamal-data-lake/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1704891056704",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1704891101689 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kamal-data-lake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1704891101689",
)

# Script generated for node Join
Join_node1704891149643 = Join.apply(
    frame1=AccelerometerLanding_node1704891056704,
    frame2=CustomerTrusted_node1704891101689,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1704891149643",
)

# Script generated for node Drop Fields
DropFields_node1704891182942 = DropFields.apply(
    frame=Join_node1704891149643,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1704891182942",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1704891223745 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1704891182942,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kamal-data-lake/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1704891223745",
)

job.commit()
