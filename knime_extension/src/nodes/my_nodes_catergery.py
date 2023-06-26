# lingbo
# from typing import Callable

import knime_extension as knext
# import util.knime_utils as knut


__category = knext.category(
    path="/community/opendata",
    level_id="my_nodes_catergery",
    name="Dataverse",
    description="Nodes that for testing and future exploration.",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon/Geolab/DvGlobalDOIlink.png",
    after="opendataset",
)

# Root path for all node icons in this file
__NODE_ICON_PATH = "icons/icon/Geolab/"



@knext.parameter_group(label="Base Settings")
class URLSettings:
    """
    Base URL settings for the Dataverse instance. For example, https://dataverse.harvard.edu
    """
    base_url = knext.StringParameter(
        "Dataverse Server URL",
        "The base URL of the Dataverse instance. For example, https://dataverse.harvard.edu",
        default_value="https://dataverse.harvard.edu",
    )


############################################
# DataVerse Search Node
############################################



@knext.node(
    name="Dataverse Search",
    node_type=knext.NodeType.SOURCE,
    icon_path=__NODE_ICON_PATH + "DvSearch.png",
    category=__category,
)
@knext.output_table(
    name="Output Table",
    description="Output table of DataVerse Search",
)
class DataVerseSearch:
    """
    Dataverse Search. Search for datasets, files, or dataverses by keyword.
    By default, it will return the first 1000 results.
    Notice that this node may take a long time to run.
    """

    base_url = URLSettings()

    # input parameters
    search_term = knext.StringParameter(
        "Search Term",
        "The search term or terms. Using “title:data” will search only the “title” field. “*” can be used as a wildcard either alone or adjacent to a term (i.e. “bird*”). ",
        default_value="mobility",
    )

    search_type = knext.StringParameter(
        "Search Type",
        "Can be either “Dataverse”, “dataset”, or “file”. ",
        enum=["dataset", "file", "dataverse", "all"],
        default_value="dataset",
    )

    def configure(self, configure_context):

        return None

    def execute(self, exec_context: knext.ExecutionContext):
        # get the search term
        # pandarallel.initialize(progress_bar=False, nb_workers=15)

        search_term = self.search_term

        start = 0
        type_ = self.search_type
        per_page = 1000
        url = (
            "%s/api/search?q=%s&start=%d&type=%s&per_page=%d"
            % (self.base_url.base_url, search_term, start, type_, per_page)
        )
        import requests
        r = requests.get(url)
        data = r.json()
        pages = data["data"]["total_count"] // per_page + 1

        # Get the data all pages from the API and save it to a dataframe

        urls = []
        for start in range(pages):
            temp = {}
            url = (
                "%s/api/search?q=%s&start=%d&type=%s&per_page=%d"
                % (self.base_url.base_url, search_term, start, type_, per_page)
            )
            temp["url"] = url
            temp["query"] = search_term
            urls.append(temp)
        import pandas as pd
        urls = pd.DataFrame(urls)

        def get_data(url):
            import requests
            import pandas as pd

            r = requests.get(url)
            data = r.json()
            return pd.DataFrame(data["data"]["items"])

        urls["data"] = urls["url"].apply(get_data)
        df = pd.concat(urls["data"].values, ignore_index=True)

        def float_list(x):
            try:
                f = ";".join([str(i) for i in x])
                return f
            except:
                return x

        df[
            [
                "subjects",
                "contacts",
                "authors",
                "keywords",
                "producers",
                "relatedMaterial",
                "geographicCoverage",
                "dataSources",
            ]
        ] = df[
            [
                "subjects",
                "contacts",
                "authors",
                "keywords",
                "producers",
                "relatedMaterial",
                "geographicCoverage",
                "dataSources",
            ]
        ].applymap(
            float_list
        )
        df.drop("publications", axis=1, inplace=True)

        # return the results as a table
        return knext.Table.from_pandas(df)


############################################
# Dataverse Query Data Files Source Node
############################################


@knext.node(
    name="Dataverse Get Dataset Contents (Source)",
    node_type=knext.NodeType.SOURCE,
    icon_path=__NODE_ICON_PATH + "DvDOIsource.png",
    category=__category,
)
@knext.output_table(
    name="Output Table",
    description="Dataset Contents",
)
class DataVerseQueryDataFilesSource:
    """
    Get dataset contents (data files) from a Dataverse DOI.
    """

    base_url = URLSettings()

    # input parameters
    global_doi = knext.StringParameter(
        "Global DOI",
        "The global DOI of the dataset. ",
        default_value="doi:10.7910/DVN/ZAKKCE",
    )

    def configure(self, configure_context):

        return None

    def execute(self, exec_context: knext.ExecutionContext):

        global_doi = self.global_doi
        from pyDataverse.api import NativeApi
        api = NativeApi(self.base_url.base_url)
        dataset = api.get_dataset(global_doi)
        files_list = dataset.json()["data"]["latestVersion"]["files"]
        import pandas as pd
        df = pd.json_normalize(files_list)
        df = df.fillna(method="bfill")

        return knext.Table.from_pandas(df)


############################################
# Dataverse Query Data Files Node
############################################


@knext.node(
    name="Dataverse Get Dataset Contents",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path=__NODE_ICON_PATH + "DvGlobalDOIlink.png",
    category=__category,
)
@knext.input_table(
    name="Input Table",
    description="A Dataverse search result table",
)
@knext.output_table(
    name="Output Table",
    description="Dataset Contents",
)
class DataVerseQueryDataFiles:
    """
    Get dataset contents (data files) from a Dataverse search result table. 
    Noting that the search result table must contain a column with the global DOI of the dataset.
    Now only works for one dataset at a time.
    """

    base_url = URLSettings()

    # input parameters
    global_doi_column = knext.ColumnParameter(
        "Global DOI Column",
        "The column containing the global DOI of the dataset. ",
    )

    def configure(self, configure_context, input_schema):

        return None

    def execute(self, exec_context: knext.ExecutionContext, input_table):

        global_doi = input_table.to_pandas()[self.global_doi_column].values[0]
        from pyDataverse.api import NativeApi
        api = NativeApi(self.base_url.base_url)
        dataset = api.get_dataset(global_doi)
        files_list = dataset.json()["data"]["latestVersion"]["files"]
        import pandas as pd
        df = pd.json_normalize(files_list)
        df = df.fillna(method="bfill")

        return knext.Table.from_pandas(df)


############################################
# Dataverse Read Data File Node
############################################


@knext.node(
    name="Dataverse Data File Reader",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path=__NODE_ICON_PATH + "DvFileReader.png",
    category=__category,
)
@knext.input_table(
    name="Input Table",
    description="Dataset contents table",
)
@knext.output_table(
    name="Output Table",
    description="Table of the slected data file",
)
class DataVerseReadDataFile:
    """
    Read a data file from a Dataverse dataset.
    The input table must contain a column with the DataFile ID of the dataset.
    The output table is a table of the selected data file.
    Now only works for one file at a time.
    Only works for tabular data files and geo data files (Like geojson, shapefile, etc).
    """

    base_url = URLSettings()

    # input parameters
    dataFile_id_column = knext.ColumnParameter(
        "DataFile ID Column",
        "The column containing the DataFile ID of the dataset. ",
    )

    is_geo = knext.BoolParameter(
        "Is Geo",
        "Is the file a geo file? Check it if the file is a geo file.",
        default_value=False,
    )

    def configure(self, configure_context, input_schema):

        return None

    def execute(self, exec_context: knext.ExecutionContext, input_table):

        
        from pyDataverse.api import DataAccessApi
        data_api = DataAccessApi(self.base_url.base_url)

        file_id = input_table.to_pandas()[self.dataFile_id_column].values[0]
        response = data_api.get_datafile(file_id)
        import io
        content_ = io.BytesIO(response.content)

        if self.is_geo:
            # FIXME: shoudn't use geopandas
            import geopandas as gp
            df = gp.read_file(content_)
            df.reset_index(inplace=True, drop=True)

        else:
            import pandas as pd
            df = pd.read_csv(content_, encoding="utf8", sep="\t")
        return knext.Table.from_pandas(df)


############################################
# Dataverse Replace Data File Node
############################################


@knext.node(
    name="Dataverse Data File Replacer",
    node_type=knext.NodeType.SINK,
    icon_path=__NODE_ICON_PATH + "DvReplace.png",
    category=__category,
)
@knext.input_table(
    name="Input Table",
    description="Input table of Dataverse Replace Data File",
)
class DataVerseReplaceDataFile:
    """
    Replace a Data File in a Dataverse dataset from a local file.
    The input table must contain a column with the DataFile ID of the dataset.
    The input table must contain a column with the DataFile Name of the dataset.
    Now only works for one file at a time.
    Note: The file to be uploaded must be in the same format as the original file.
    If you want to publish the dataset after replacing the file, you need to connect the "Dataverse Publisher" node.
    """

    base_url = URLSettings()

    # input parameters
    dataFile_id_column = knext.ColumnParameter(
        "DataFile ID Column",
        "The column containing the DataFile ID of the dataset. ",
    )

    dataFile_name_column = knext.ColumnParameter(
        "DataFile Name Column",
        "The column containing the DataFile Name of the dataset. ",
    )

    upload_file_path = knext.StringParameter(
        "Upload File Path", "The path to the file to be uploaded. "
    )

    API_TOKEN = knext.StringParameter(
        "API Token", "The API Token for the Dataverse. "
    )

    def configure(self, configure_context, input_schema):

        return None

    def execute(self, exec_context: knext.ExecutionContext, input_table):

        from pyDataverse.api import NativeApi
        api = NativeApi(self.base_url.base_url, self.API_TOKEN)
        json_str = """{"description":"My description.","categories":["Data"],"forceReplace":true}"""

        def check_need_replace(x, file_list):
            a = False
            if x.endswith(".tab"):
                if x.split(".")[0] in file_list:
                    a = True
            return a

        df = input_table.to_pandas()
        file_list = [
            file_name.split(".")[0] for file_name in os.listdir(self.upload_file_path)
        ]
        need_upload = df[
            df[self.dataFile_name_column].map(
                lambda x: check_need_replace(x, file_list)
            )
        ]

        for k, v in need_upload.iterrows():
            remote_file_id = v["dataFile.id"]
            local_file_path = input_path + v["dataFile.filename"].replace(
                ".tab", ".csv"
            )
            for i in range(60):
                test = api.replace_datafile(
                    remote_file_id, local_file_path, json_str, False
                )
                if json.loads(test.content)["status"] != "ERROR":
                    break
                if (
                    json.loads(test.content)["message"]
                    == "Error! You may not replace a file with a file that has duplicate content."
                ):
                    break
                # print(json.loads(test.content))
                time.sleep(1)
                print("retry %d" % (i + 1))

        return None


############################################
# Dataverse Publish Node
############################################


@knext.node(
    name="Dataverse Publisher",
    node_type=knext.NodeType.SINK,
    icon_path=__NODE_ICON_PATH + "DvPublish.png",
    category=__category,
)
class DataVersePublish:
    """
    Dataverse Publisher.
    Note: The dataset must be in draft mode.
    """

    base_url = URLSettings()

    # input parameters
    dataset_doi = knext.StringParameter(
        "Dataset DOI",
        "The DOI of the dataset to be published. ",
    )

    API_TOKEN = knext.StringParameter(
        "API Token", "The API Token for the Dataverse. "
    )

    def configure(self, configure_context):

        return None

    def execute(self, exec_context: knext.ExecutionContext):

        from pyDataverse.api import NativeApi
        api = NativeApi(self.base_url.base_url, self.API_TOKEN)
        api.publish_dataset(pid=self.dataset_doi, release_type="major")
        return None
