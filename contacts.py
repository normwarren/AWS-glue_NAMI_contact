import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "nami-db", table_name = "namicontacts", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "nami-db", table_name = "namicontacts", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("entries id", "long", "entries id", "long"), ("date submitted", "string", "date submitted", "string"), ("ip address", "string", "ip address", "string"), ("subject", "string", "subject", "string"), ("sender name", "string", "sender name", "string"), ("sender email", "string", "sender email", "string"), ("emailed to", "string", "emailed to", "string"), ("date", "string", "date", "string"), ("first name", "string", "first name", "string"), ("last name", "string", "last name", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("family_attended_nami", "string", "family_attended_nami", "string"), ("which_class", "string", "which_class", "string"), ("diagnosis", "string", "diagnosis", "string"), ("other_diagnosis", "string", "other_diagnosis", "string"), ("how_hear_about_nami", "string", "how_hear_about_nami", "string"), ("city_driven_from_to_attend", "string", "city_driven_from_to_attend", "string"), ("most_critical_issue", "string", "most_critical_issue", "string"), ("hope_to_get_out_of_class", "string", "hope_to_get_out_of_class", "string"), ("relation_to", "string", "relation_to", "string"), ("support_group_interest", "string", "support_group_interest", "string"), ("address", "string", "address", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("entries id", "long", "entries id", "long"), ("date submitted", "string", "date submitted", "string"), ("ip address", "string", "ip address", "string"), ("subject", "string", "subject", "string"), ("sender name", "string", "sender name", "string"), ("sender email", "string", "sender email", "string"), ("emailed to", "string", "emailed to", "string"), ("date", "string", "date", "string"), ("first name", "string", "first name", "string"), ("last name", "string", "last name", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("family_attended_nami", "string", "family_attended_nami", "string"), ("which_class", "string", "which_class", "string"), ("diagnosis", "string", "diagnosis", "string"), ("other_diagnosis", "string", "other_diagnosis", "string"), ("how_hear_about_nami", "string", "how_hear_about_nami", "string"), ("city_driven_from_to_attend", "string", "city_driven_from_to_attend", "string"), ("most_critical_issue", "string", "most_critical_issue", "string"), ("hope_to_get_out_of_class", "string", "hope_to_get_out_of_class", "string"), ("relation_to", "string", "relation_to", "string"), ("support_group_interest", "string", "support_group_interest", "string"), ("address", "string", "address", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "MySQL_dest", connection_options = {"dbtable": "namicontacts", "database": "destination_demo"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "MySQL_dest", connection_options = {"dbtable": "namicontacts", "database": "destination_demo"}, transformation_ctx = "datasink4")
job.commit()