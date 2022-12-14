# The root category of all knime-dataverse-extension categories
import knime_extension as knext

# This defines the root knime-dataverse-extension KNIME category that is displayed in the node repository
category = knext.category(
    path="/community",
    level_id="opendata", # this is the id of the category in the node repository #FIXME: 
    name="knime-dataverse-extension",
    description="some open data nodes especially for dataverse",
    # starting at the root folder of the extension_module parameter in the knime.yml file
    icon="icons/icon/knime-dataverse-extension.png",
)


# need import node files here
import nodes.my_nodes_catergery
