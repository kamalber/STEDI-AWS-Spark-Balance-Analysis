import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1704882531156 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kamal-data-lake/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1704882531156",
)

# Script generated for node Privacy Filter
PrivacyFilter_node1704887333976 = Filter.apply(
    frame=CustomerLanding_node1704882531156,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1704887333976",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1704882596093 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node1704887333976,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kamal-data-lake/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1704882596093",
)

job.commit()
