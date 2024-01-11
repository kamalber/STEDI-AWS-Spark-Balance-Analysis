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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1704891056704 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kamal-data-lake/step_tariner/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1704891056704",
)

# Script generated for node Customer Curated
CustomerCurated_node1704891101689 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://kamal-data-lake/customer/curated/"]},
    transformation_ctx="CustomerCurated_node1704891101689",
)

# Script generated for node Join
Join_node1704891149643 = Join.apply(
    frame1=StepTrainerLanding_node1704891056704,
    frame2=CustomerCurated_node1704891101689,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1704891149643",
)

# Script generated for node Drop Fields
DropFields_node1704891182942 = DropFields.apply(
    frame=Join_node1704891149643,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "`.serialNumber`",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1704891182942",
)

# Script generated for node Step-Trainer Trusted
StepTrainerTrusted_node1704891223745 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1704891182942,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kamal-data-lake/step_tariner/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1704891223745",
)

job.commit()
