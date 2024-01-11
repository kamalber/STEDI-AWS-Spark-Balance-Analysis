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

# Script generated for node Step-Tariner Trusted
StepTarinerTrusted_node1704973709884 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kamal-data-lake/step_tariner/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTarinerTrusted_node1704973709884",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1704973661412 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kamal-data-lake/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1704973661412",
)

# Script generated for node Join
Join_node1704973808191 = Join.apply(
    frame1=AccelerometerTrusted_node1704973661412,
    frame2=StepTarinerTrusted_node1704973709884,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1704973808191",
)

# Script generated for node Drop Fields
DropFields_node1704973834341 = DropFields.apply(
    frame=Join_node1704973808191,
    paths=["user"],
    transformation_ctx="DropFields_node1704973834341",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1704973928159 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1704973834341,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kamal-data-lake/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1704973928159",
)

job.commit()
