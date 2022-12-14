# lingbo
# from typing import Callable
import pandas as pd
import geopandas as gp
import knime_extension as knext
import util.knime_utils as knut
import requests
from pandarallel import pandarallel
from pyDataverse.api import NativeApi, DataAccessApi
from pyDataverse.models import Dataverse
import io
import numpy as np
from shapely.geometry import Polygon

__category = knext.category(
    path="/community/opendata",
    level_id="my_nodes_catergery",
    name="my_nodes_catergery",
    description="Nodes that for testing and future exploration.",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon/GeolabCategroy.png",
    after="opendataset",
)

# Root path for all node icons in this file
__NODE_ICON_PATH = "icons/icon/Geolab/"

############################################
# Harvard DataVerse Search Node
############################################


@knext.node(
    name="Harvard DataVerse Search",
    node_type=knext.NodeType.SOURCE,
    icon_path=__NODE_ICON_PATH + "DvSearch.png",
    category=__category,
)
@knext.output_table(
    name="Output Table",
    description="Output table of Harvard DataVerse Search",
)
class HarvardDataVerseSearch:
    """
    Harvard DataVerse Search
    """

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
        pandarallel.initialize(progress_bar=False, nb_workers=15)

        search_term = self.search_term

        start = 0
        type_ = self.search_type
        per_page = 1000
        url = (
            "https://dataverse.harvard.edu/api/search?q=%s&start=%d&type=%s&per_page=%d"
            % (search_term, start, type_, per_page)
        )
        r = requests.get(url)
        data = r.json()
        pages = data["data"]["total_count"] // per_page + 1

        # Get the data all pages from the API and save it to a dataframe

        urls = []
        for start in range(pages):
            temp = {}
            url = (
                "https://dataverse.harvard.edu/api/search?q=%s&start=%d&type=%s&per_page=%d"
                % (search_term, start, type_, per_page)
            )
            temp["url"] = url
            temp["query"] = search_term
            urls.append(temp)
        urls = pd.DataFrame(urls)

        def get_data(url):
            import requests
            import pandas as pd

            r = requests.get(url)
            data = r.json()
            return pd.DataFrame(data["data"]["items"])

        urls["data"] = urls["url"].parallel_apply(get_data)
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
# Harvard DataVerse Query Data Files Source Node
############################################


@knext.node(
    name="Harvard DataVerse GlobalDOI Search",
    node_type=knext.NodeType.SOURCE,
    icon_path=__NODE_ICON_PATH + "DvDOIsource.png",
    category=__category,
)
@knext.output_table(
    name="Output Table",
    description="Output table of Harvard DataVerse Query Data Files",
)
class HarvardDataVerseQueryDataFilesSource:
    """
    Harvard DataVerse Query Data Files
    """

    # input parameters
    global_doi = knext.StringParameter(
        "Global DOI",
        "The global DOI of the dataset. ",
        default_value="doi:10.7910/DVN/ZAKKCE",
    )

    def configure(self, configure_context):

        return None

    def execute(self, exec_context: knext.ExecutionContext):

        base_url = "https://dataverse.harvard.edu/"
        global_doi = self.global_doi
        api = NativeApi(base_url)
        data_api = DataAccessApi(base_url)
        dataset = api.get_dataset(global_doi)
        files_list = dataset.json()["data"]["latestVersion"]["files"]
        df = pd.json_normalize(files_list)
        df = df.fillna(method="bfill")

        return knext.Table.from_pandas(df)


############################################
# Harvard DataVerse Query Data Files Node
############################################


@knext.node(
    name="Harvard DataVerse GlobalDOI Link ",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path=__NODE_ICON_PATH + "DvGlobalDOIlink.png",
    category=__category,
)
@knext.input_table(
    name="Input Table",
    description="Input table of Harvard DataVerse Query Data Files",
)
@knext.output_table(
    name="Output Table",
    description="Output table of Harvard DataVerse Query Data Files",
)
class HarvardDataVerseQueryDataFiles:
    """
    Harvard DataVerse Query Data Files
    """

    # input parameters
    global_doi_column = knext.ColumnParameter(
        "Global DOI Column",
        "The column containing the global DOI of the dataset. ",
    )

    def configure(self, configure_context, input_schema):

        return None

    def execute(self, exec_context: knext.ExecutionContext, input_table):

        base_url = "https://dataverse.harvard.edu/"
        global_doi = input_table.to_pandas()[self.global_doi_column].values[0]
        api = NativeApi(base_url)
        data_api = DataAccessApi(base_url)
        dataset = api.get_dataset(global_doi)
        files_list = dataset.json()["data"]["latestVersion"]["files"]
        df = pd.json_normalize(files_list)
        df = df.fillna(method="bfill")

        return knext.Table.from_pandas(df)


############################################
# Harvard DataVerse Read Data File Node
############################################


@knext.node(
    name="Harvard DataVerse DataID Reader",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path=__NODE_ICON_PATH + "DvFileReader.png",
    category=__category,
)
@knext.input_table(
    name="Input Table",
    description="Input table of Harvard DataVerse Read Data File",
)
@knext.output_table(
    name="Output Table",
    description="Output table of Harvard DataVerse Read Data File",
)
class HarvardDataVerseReadDataFile:
    """
    Harvard DataVerse Read Data File
    """

    # input parameters
    dataFile_id_column = knext.ColumnParameter(
        "DataFile ID Column",
        "The column containing the DataFile ID of the dataset. ",
    )

    is_geo = knext.BoolParameter(
        "Is Geo",
        "Is the file a geo file?",
        default_value=False,
    )

    def configure(self, configure_context, input_schema):

        return None

    def execute(self, exec_context: knext.ExecutionContext, input_table):

        base_url = "https://dataverse.harvard.edu/"
        api = NativeApi(base_url)
        data_api = DataAccessApi(base_url)

        file_id = input_table.to_pandas()[self.dataFile_id_column].values[0]
        response = data_api.get_datafile(file_id)
        content_ = io.BytesIO(response.content)

        if self.is_geo:
            df = gp.read_file(content_)
            df.reset_index(inplace=True, drop=True)

        else:
            df = pd.read_csv(content_, encoding="utf8", sep="\t")
        return knext.Table.from_pandas(df)


############################################
# Harvard DataVerse Replace Data File Node
############################################


@knext.node(
    name="Harvard DataVerse Data File Replacer",
    node_type=knext.NodeType.SINK,
    icon_path=__NODE_ICON_PATH + "DvReplace.png",
    category=__category,
)
@knext.input_table(
    name="Input Table",
    description="Input table of Harvard DataVerse Replace Data File",
)
class HarvardDataVerseReplaceDataFile:
    """
    Harvard DataVerse Replace Data File
    """

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
        "API Token", "The API Token for the Harvard DataVerse. "
    )

    def configure(self, configure_context, input_schema):

        return None

    def execute(self, exec_context: knext.ExecutionContext, input_table):

        base_url = "https://dataverse.harvard.edu/"
        api = NativeApi(base_url, self.API_TOKEN)
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
# Harvard DataVerse Publish Node
############################################


@knext.node(
    name="Harvard DataVerse Publish",
    node_type=knext.NodeType.SINK,
    icon_path=__NODE_ICON_PATH + "DvPublish.png",
    category=__category,
)
class HarvardDataVersePublish:
    """
    Harvard DataVerse Publish
    """

    # input parameters
    dataset_doi = knext.StringParameter(
        "Dataset DOI",
        "The DOI of the dataset to be published. ",
    )

    API_TOKEN = knext.StringParameter(
        "API Token", "The API Token for the Harvard DataVerse. "
    )

    def configure(self, configure_context):

        return None

    def execute(self, exec_context: knext.ExecutionContext):

        base_url = "https://dataverse.harvard.edu/"
        api = NativeApi(base_url, self.API_TOKEN)
        api.publish_dataset(pid=self.dataset_doi, release_type="major")
        return None


############################################
# Create Grid Node
############################################


@knext.node(
    name="Create Grid",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path=__NODE_ICON_PATH + "CreateGrid.png",
    category=__category,
)
@knext.input_table(
    name="Input Table",
    description="Input table of Create Grid",
)
@knext.output_table(
    name="Output Table",
    description="Output table of Create Grid",
)
class CreateGrid:
    """
    Create Grid
    """

    geo_col = knext.ColumnParameter(
        "Geometry Column",
        "The column containing the geometry of the dataset. ",
        column_filter=knut.is_geo,
        include_row_key=False,
        include_none_column=False,
    )

    grid_length = knext.IntParameter(
        "Grid Length",
        "The length in meters of the grid. ",
        default_value=100,
    )

    def configure(self, configure_context, input_schema):

        return None

    def execute(self, exec_context: knext.ExecutionContext, input_table):

        gdf = gp.GeoDataFrame(input_table.to_pandas(), geometry=self.geo_col)

        xmin, ymin, xmax, ymax = gdf.total_bounds
        width = self.grid_length
        height = self.grid_length
        rows = int(np.ceil((ymax - ymin) / height))
        cols = int(np.ceil((xmax - xmin) / width))
        XleftOrigin = xmin
        XrightOrigin = xmin + width
        YtopOrigin = ymax
        YbottomOrigin = ymax - height
        polygons = []
        for i in range(cols):
            Ytop = YtopOrigin
            Ybottom = YbottomOrigin
            for j in range(rows):
                polygons.append(
                    Polygon(
                        [
                            (XleftOrigin, Ytop),
                            (XrightOrigin, Ytop),
                            (XrightOrigin, Ybottom),
                            (XleftOrigin, Ybottom),
                        ]
                    )
                )
                Ytop = Ytop - height
                Ybottom = Ybottom - height
            XleftOrigin = XleftOrigin + width
            XrightOrigin = XrightOrigin + width

        grid = gp.GeoDataFrame({"geometry": polygons}, crs=gdf.crs)

        return knext.Table.from_pandas(grid)
