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
        
        self.TABLE_METADATA_PATH = self.CATALOG_PATH + self.TABLE_NAME + '/metadata'
        self.TABLE_DATA_PATH = self.CATALOG_PATH + self.TABLE_NAME + '/data'
        
        self.s3 = boto3.resource('s3')
        self.my_bucket = s3.Bucket(BUCKET)

    def get_all_metadata_files(self):
        '''method to get the full path of all metdata files in the metadata layer
            by metadata files we mean just iceberg metadata files in json format,
            excluding manifest files and manifest lists which will have assigned methods.
        '''
        
        file_list = []
        
        for object_summary in my_bucket.objects.filter(Prefix=self.TABLE_METADATA_PATH):
            if '.json' in object_summary.key:
                file_list.append(object_summary.key)
                print("Metadata File Path: {}".format(object_summary.key))
        
        return file_list

    def get_latest_metadata_file(self):
        '''method to get the full path of the latest metdata file in the metadata layer
        by metadata files we mean just iceberg metadata files in json format,
        excluding manifest files and manifest lists which will have assigned methods.
        '''
        
        file_list = []
        
        for object_summary in my_bucket.objects.filter(Prefix=self.TABLE_METADATA_PATH):
            if '.json' in object_summary.key:
                file_list.append(object_summary.key)
                print("Metadata File Path: {}".format(object_summary.key))
        
        return file_list[-1]

    def get_all_manifest_lists(self):
        '''method to get the full path of all manifest lists in the metadata layer
        The manifest list name is prefixed by the "snap" string and is in avro format.
        '''
        
        file_list = []
        
        for object_summary in my_bucket.objects.filter(Prefix=self.TABLE_METADATA_PATH):
            if 'snap' in object_summary.key:
                file_list.append(object_summary.key)
                print("Metadata File Path: {}".format(object_summary.key))
        
        return file_list

    def get_latest_manifest_list(self):
        '''method to get the full path of the latest manifest list in the metadata layer
        The manifest list name is prefixed by the "snap" string and is in avro format.

        '''

        return NotImplemented

    def get_all_manifest_files(self):
        '''method to get the full path of all metdata files in the metadata layer
            manifest files are in avro format and are not prefixed by a string "snap"
        '''
        
        file_list = []

        for object_summary in my_bucket.objects.filter(Prefix=self.TABLE_METADATA_PATH):
            #print(object_summary.key)
            if 'avro' in object_summary.key and 'snap' not in object_summary.key:
                file_list.append(object_summary.key)
                print("Metadata File Path: {}".format(object_summary.key))

        return file_list

    def get_all_metadata_layer_files(self):
        '''method to get the full path of all metdata files in the metadata layer
            includes iceberg metadata files, manifest lists, and manifest files.
        '''
        
        file_list = []
        
        for object_summary in my_bucket.objects.filter(Prefix=self.TABLE_METADATA_PATH):
            file_list.append(object_summary.key)
            print("Metadata File Path: {}".format(object_summary.key))
        
        return file_list

    def get_all_data_layer_files(self):
        '''method to get the full path of all files in the data layer
            includes data files, delete files, puffin files.
        '''
        file_list = []

        for object_summary in my_bucket.objects.filter(Prefix=self.TABLE_DATA_PATH):
            file_list.append(object_summary.key)
            print("Data File Path: {}".format(object_summary.key))

        return file_list
    
    def get_all_delete_files(self):
        '''method to get the full path of all delete files in the data layer
        '''
        
        return NotImplemented

    def count_data_layer_files(self):
        '''method to count all files in the data layer
            includes data files, delete files, puffin files.
        '''
        file_list = self.get_all_data_layer_files()
        print("Total number of files in the Data Layer: {}".format(len(file_list)))
        
        return len(file_list)
    
    def count_metadata_layer_files(self):
        '''method to obtain a count of all metadata files in metadata layer
        includes iceberg metadata files, manifest lists, and manifest files.
        '''
        
        file_list = self.get_all_metadata_layer_files()
        print("Total number of files in the Metadata Layer: {}".format(len(file_list)))
        
        return len(file_list)

    def count_metadata_files(self):
        '''method to obtain a count of all metadata files in metadata layer
        by metadata files we mean just iceberg metadata files in json format,
        excluding manifest files and manifest lists which will have assigned methods.
        '''

        file_list = self.get_all_metadata_files()
        print("Total Number of Metadata Files: {}".format(len(file_list)))
        
        return len(file_list)

    def count_manifest_lists(self):
        '''method to obtain a count of all manifest lists in metadata layer
        '''

        file_list = self.get_all_manifest_lists()
        print("Total Number of Manifest Lists: {}".format(len(file_list)))
        
        return len(file_list)

    def count_manifest_files(self):
        '''method to obtain a count of all manifest files in metadata layer
        '''

        file_list = self.get_all_manifest_files()
        print("Total Number of Manifest Files: {}".format(len(file_list)))
        
        return len(file_list)
    
    def count_delete_files(self):
        '''method to count delete files in metadata layer
        '''
        
        return NotImplemented