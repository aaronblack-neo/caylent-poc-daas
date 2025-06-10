

def test_loading_data(glue_context):
    from etl.etl_manager import EtlManager

    landing_bucket_name = "caylent-poc-dl-landing"
    raw_bucket_name = "caylent-poc-dl-raw"

    etl_manager = EtlManager(glue_context, landing_bucket_name, raw_bucket_name)

    # Test processing a specific table
    table = "accession_data"
    etl_manager.process_landing_data(table)