import boto3
import json
import pandas as pd
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import monotonically_increasing_id
import io
import openpyxl

# Inputs

format = "iceberg"
catalog_name = "glue_catalog"
database_name = "stage"
iceberg_s3_path = "s3://caylent-poc-datalake/datalake/"

# Initialize Glue job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('medical_comprehend')

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", iceberg_s3_path)

# Hardcoded S3 paths
input_s3_path = 's3://caylent-poc-medical-comprehend/example/input/'
output_s3_txt_path = 's3://caylent-poc-medical-comprehend/example/output/'
output_s3_comprehend_path = 's3://caylent-poc-medical-comprehend/example/results/'

# Step 1: Read the CSV file(s) from S3 and add row number column
df = spark.read.format('csv').option('header', 'true').load(input_s3_path)
# Add _row_number column (starting from 1)
df = df.withColumn('_row_number', monotonically_increasing_id() + 1)

# Convert DataFrame to RDD and assign row numbers
rdd_with_index = df.rdd.zipWithIndex().map(lambda x: (x[0], x[1] + 1))  # Row number starts from 1

# Function to write each row as a text file to S3 (executed on worker nodes)
def write_to_s3_partition(rows):
    s3_client = boto3.client('s3')
    bucket_name = output_s3_txt_path.replace('s3://', '').split('/')[0]
    prefix = '/'.join(output_s3_txt_path.replace('s3://', '').split('/')[1:])
    
    for row, row_number in rows:
        file_content = '\n'.join([f'{col}: {row[col]}' for col in row.__fields__])
        # Use row number for filename
        file_name = f'row_{row_number}.txt'
        s3_key = f'{prefix}{file_name}' if prefix else file_name
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=file_content)

# Apply the function in parallel using RDD
rdd_with_index.foreachPartition(write_to_s3_partition)
print(f'Each row has been successfully written to individual text files in the "{output_s3_txt_path}" folder.')

# Step 2: Verify text files exist in S3 before processing
s3 = boto3.client('s3')
input_bucket = output_s3_txt_path.replace('s3://', '').split('/')[0]
input_prefix = '/'.join(output_s3_txt_path.replace('s3://', '').split('/')[1:])

# List objects to confirm text files are present
response = s3.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)
if not response.get('Contents'):
    print(f"No text files found in {output_s3_txt_path}. Exiting job.")
    job.commit()
    exit(1)

# Step 3: Process text files with Comprehend Medical APIs
comprehend_medical = boto3.client('comprehendmedical')

# Define Comprehend Medical APIs
api_configs = [
    {'api': 'detect_entities_v2', 'method': 'detect_entities_v2'},
    {'api': 'detect_phi', 'method': 'detect_phi'},
    {'api': 'infer_icd10_cm', 'method': 'infer_icd10_cm'},
    {'api': 'infer_rx_norm', 'method': 'infer_rx_norm'},
    {'api': 'infer_snomed_ct', 'method': 'infer_snomed_ct'}
]

# Process each text file with all Comprehend Medical APIs
for obj in response.get('Contents', []):
    file_key = obj['Key']
    if file_key.endswith('.txt'):
        # Read the text file from S3
        try:
            file_obj = s3.get_object(Bucket=input_bucket, Key=file_key)
            file_content = file_obj['Body'].read().decode('utf-8')
        except Exception as e:
            print(f"Error reading {file_key} from S3: {str(e)}")
            continue

        # Extract the row number from the file_key to use as the subfolder name
        row_number = file_key.split('/')[-1].replace('.txt', '').replace('row_', '')
        row_subfolder = f"row_{row_number}/"

        # Call each Comprehend Medical API and save results in the row-specific subfolder
        for config in api_configs:
            try:
                # Dynamically call the API method
                api_method = getattr(comprehend_medical, config['method'])
                result = api_method(Text=file_content)

                # Prepare the output JSON file
                output_file_key = f"{row_subfolder}{config['api']}.json"
                output_path = f"{output_s3_comprehend_path}{output_file_key}"

                # Upload the result to the row-specific subfolder
                s3.put_object(Bucket=output_s3_comprehend_path.replace('s3://', '').split('/')[0],
                              Key='/'.join(output_path.replace('s3://', '').split('/')[1:]),
                              Body=json.dumps(result, indent=4))
            except Exception as e:
                print(f"Error processing {file_key} with {config['api']}: {str(e)}")

# Step 4: Parse JSON outputs and combine into Excel file
for obj in response.get('Contents', []):
    file_key = obj['Key']
    if file_key.endswith('.txt'):
        # Extract row number and subfolder
        row_number = file_key.split('/')[-1].replace('.txt', '').replace('row_', '')
        row_subfolder = f"row_{row_number}/"
        
        # Initialize lists for combined data
        entities = []
        traits = []
        attributes = []
        icd_concepts = []
        snomed_concepts = []
        rxnorm_concepts = []

        # Load CSV row as CaseMetadata
        csv_row = df.filter(df['_row_number'] == int(row_number)).collect()
        if not csv_row:
            print(f"No CSV row found for row_number {row_number}. Skipping Excel generation.")
            continue
        csv_data = csv_row[0].asDict()
        df_case_metadata = pd.DataFrame([csv_data])

        # Process each API output JSON
        for config in api_configs:
            json_file_key = f"{row_subfolder}{config['api']}.json"
            json_path = f"{output_s3_comprehend_path}{json_file_key}"
            try:
                json_obj = s3.get_object(Bucket=output_s3_comprehend_path.replace('s3://', '').split('/')[0],
                                         Key='/'.join(json_path.replace('s3://', '').split('/')[1:]))
                data = json.loads(json_obj['Body'].read().decode('utf-8'))

                # Parse Entities, Traits, Attributes
                for entity in data.get('Entities', []):
                    entity_record = {
                        'Id': entity.get('Id'),
                        'Text': entity.get('Text'),
                        'Category': entity.get('Category'),
                        'Type': entity.get('Type'),
                        'Score': entity.get('Score'),
                        'BeginOffset': entity.get('BeginOffset'),
                        'EndOffset': entity.get('EndOffset')
                    }
                    entities.append(entity_record)

                    for trait in entity.get('Traits', []):
                        traits.append({
                            'EntityId': entity.get('Id'),
                            'TraitName': trait.get('Name'),
                            'TraitScore': trait.get('Score')
                        })

                    for attribute in entity.get('Attributes', []):
                        attributes.append({
                            'EntityId': entity.get('Id'),
                            'AttributeId': attribute.get('Id'),
                            'AttributeText': attribute.get('Text'),
                            'AttributeCategory': attribute.get('Category', ''),
                            'AttributeType': attribute.get('Type'),
                            'Score': attribute.get('Score'),
                            'RelationshipScore': attribute.get('RelationshipScore', ''),
                            'RelationshipType': attribute.get('RelationshipType', ''),
                            'BeginOffset': attribute.get('BeginOffset'),
                            'EndOffset': attribute.get('EndOffset')
                        })

                    # Parse ICD10CMConcepts (from infer_icd10_cm)
                    if config['api'] == 'infer_icd10_cm':
                        for icd in entity.get('ICD10CMConcepts', []):
                            icd_concepts.append({
                                'EntityId': entity.get('Id'),
                                'ICDCode': icd.get('Code'),
                                'ICDDescription': icd.get('Description'),
                                'ICDScore': icd.get('Score')
                            })

                    # Parse SNOMEDCTConcepts (from infer_snomed_ct)
                    if config['api'] == 'infer_snomed_ct':
                        for snomed in entity.get('SNOMEDCTConcepts', []):
                            snomed_concepts.append({
                                'EntityId': entity.get('Id'),
                                'SNOMEDCode': snomed.get('Code'),
                                'SNOMEDDescription': snomed.get('Description'),
                                'SNOMEDScore': snomed.get('Score')
                            })

                    # Parse RxNormConcepts (from infer_rx_norm)
                    if config['api'] == 'infer_rx_norm':
                        for rxnorm in entity.get('RxNormConcepts', []):
                            rxnorm_concepts.append({
                                'EntityId': entity.get('Id'),
                                'RxNormCode': rxnorm.get('Code'),
                                'RxNormDescription': rxnorm.get('Description'),
                                'RxNormScore': rxnorm.get('Score')
                            })

            except Exception as e:
                print(f"Error reading or parsing {json_path}: {str(e)}")
                continue

        # Create DataFrames
        df_entities = pd.DataFrame(entities).drop_duplicates(subset=['Id'])
        df_traits = pd.DataFrame(traits).drop_duplicates(subset=['EntityId', 'TraitName', 'TraitScore'])
        df_attributes = pd.DataFrame(attributes).drop_duplicates(subset=['EntityId', 'AttributeId', 'AttributeText'])
        df_icd_concepts = pd.DataFrame(icd_concepts)
        df_snomed_concepts = pd.DataFrame(snomed_concepts)
        df_rxnorm_concepts = pd.DataFrame(rxnorm_concepts)

        # Write to Excel in the row-specific subfolder
        excel_file_key = f"{row_subfolder}combined_medical_data.xlsx"
        excel_path = f"{output_s3_comprehend_path}{excel_file_key}"
        with io.BytesIO() as output:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_entities.to_excel(writer, sheet_name='Entities', index=False)
                df_traits.to_excel(writer, sheet_name='Traits', index=False)
                df_attributes.to_excel(writer, sheet_name='Attributes', index=False)
                df_icd_concepts.to_excel(writer, sheet_name='ICD10CMConcepts', index=False)
                df_snomed_concepts.to_excel(writer, sheet_name='SNOMEDCTConcepts', index=False)
                df_rxnorm_concepts.to_excel(writer, sheet_name='RxNormConcepts', index=False)
                df_case_metadata.to_excel(writer, sheet_name='CaseMetadata', index=False)
            s3.put_object(Bucket=output_s3_comprehend_path.replace('s3://', '').split('/')[0],
                          Key='/'.join(excel_path.replace('s3://', '').split('/')[1:]),
                          Body=output.getvalue())
            
            # Write as iceberg table
            
            iceberg_tables = {
                'entities': df_entities,
                'traits': df_traits,
                'attributes': df_attributes,
                'icd_concepts': df_icd_concepts,
                'snomed_concepts': df_snomed_concepts,
                'rxnorm_concepts': df_rxnorm_concepts
                }
                
            for table_name, df in iceberg_tables.items():
                if not df.empty:
                    for column in df_case_metadata.columns:
                        df[column] = df_case_metadata[column].loc[0]
    
                    iceberg_full_table = catalog_name + "." + database_name + "." + table_name
                    spark_df = spark.createDataFrame(df)
                    spark_df.writeTo(iceberg_full_table).using(format).createOrReplace()
            
print(f'Comprehend Medical results and combined Excel files have been written to row-specific subfolders in "{output_s3_comprehend_path}".')

# Commit the Glue job
job.commit()