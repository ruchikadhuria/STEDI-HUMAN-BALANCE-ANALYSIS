import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_landing
customer_landing_node1783429751602 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customer_landing_node1783429751602")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from customer_landing
where sharewithresearchasofdate is not null;
'''
SQLQuery_node1783429768737 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":customer_landing_node1783429751602}, transformation_ctx = "SQLQuery_node1783429768737")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783429768737, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783429188796", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1783430281269 = glueContext.getSink(path="s3://stedi-data-lake-ruchika/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1783430281269")
customer_trusted_node1783430281269.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customer_trusted_node1783430281269.setFormat("glueparquet", compression="snappy")
customer_trusted_node1783430281269.writeFrame(SQLQuery_node1783429768737)
job.commit()
