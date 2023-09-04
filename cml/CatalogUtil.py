import boto3

class CatalogUtil():
    '''
    API to interact with Iceberg catalog in Cloudera Data Platform
    Lots of util methods for getting all or the latest metadata, file counts, and more. 
    
    bucket: CDP Env bucket e.g. "go01-demo"
    boto3.resource = always use "s3"
    
    '''
    
    def __init__(self, CATALOG_NAME, TABLE_NAME, BUCKET)
        self.CATALOG_NAME = CATALOG_NAME
        self.CATALOG_PATH = "warehouse/tablespace/external/hive/{}.db/".format(CATALOG_NAME)
        self.TABLE_NAME = TABLE_NAME
        self.TABLE_PATH = self.CATALOG_PATH + self.TABLE_NAME
        self.s3 = boto3.resource('s3')
        self.my_bucket = s3.Bucket(BUCKET)

    def get_all_metadata_files(self):
        '''method to get the full path of all metdata files in the metadata layer
            by metadata files we mean just iceberg metadata files in json format,
            excluding manifest files and manifest lists which will have assigned methods.
        '''
        for object_summary in my_bucket.objects.filter(Prefix=self.TABLE_PATH):
            #print(object_summary.key)
            metadata_file = object_summary.key

        print("Metadata File Path: {}".format(metadata_file))

        return NotImplemented

    def get_latest_metadata_file():
        '''method to get the full path of the latest metdata file in the metadata layer
        by metadata files we mean just iceberg metadata files in json format,
        excluding manifest files and manifest lists which will have assigned methods.
        '''

        return NotImplemented

    def get_all_manifest_lists():
        '''method to get the full path of all manifest lists in the metadata layer
        The manifest list name is prefixed by the "snap" string and is in avro format.

        '''

        return NotImplemented

    def get_latest_manifest_list():
        '''method to get the full path of the latest manifest list in the metadata layer
        The manifest list name is prefixed by the "snap" string and is in avro format.

        '''

        return NotImplemented

    def get_all_manifest_files():
        '''method to get the full path of all metdata files in the metadata layer
            manifest files are in avro format and are not prefixed by a string "snap"
        '''

        return NotImplemented

    def get_latest_manifest_file():
        '''method to get the full path of the latest manifest list in the metadata layer
        manifest files are in avro format and are not prefixed by a string "snap"

        '''

        return NotImplemented

    def get_all_metadata_layer_files():
        '''method to get the full path of all metdata files in the metadata layer
            includes iceberg metadata files, manifest lists, and manifest files.
        '''

        return NotImplemented

    def get_all_data_layer_files():
        '''method to get the full path of all files in the data layer
            includes data files, delete files, puffin files.
        '''
        
        return NotImplemented
    
    def get_all_delete_files():
        '''method to get the full path of all delete files in the data layer
        '''
        
        return NotImplemented

    def count_data_layer_files():
        '''method to count all files in the data layer
            includes data files, delete files, puffin files.
        '''
        
        return NotImplemented
    
    def count_metadata_layer_files():
        '''method to obtain a count of all metadata files in metadata layer
        includes iceberg metadata files, manifest lists, and manifest files.
        '''

        return NotImplemented

    def count_metadata_files():
        '''method to obtain a count of all metadata files in metadata layer
        by metadata files we mean just iceberg metadata files in json format,
        excluding manifest files and manifest lists which will have assigned methods.
        '''

        return NotImplemented

    def count_manifest_lists():
        '''method to obtain a count of all manifest lists in metadata layer
        '''

        return NotImplemented

    def count_manifest_files():
        '''method to obtain a count of all manifest files in metadata layer
        '''

        return NotImplemented
    
    def count_delete_files():
        '''method to count delete files in metadata layer
        '''
        
        return NotImplemented