"""
Kaltura Metadata Utility Script

This script facilitates the management of custom metadata associated with entries on the Kaltura media platform.
It provides a set of operations that interface with the Kaltura API, enabling users to retrieve, update, and maintain
the consistency of metadata. The script ensures that metadata fields are structured and ordered according to the rules
defined in a Metadata Profile's XSD (XML Schema Definition).

Usage Example:

```bash
python kaltura_metadata_xml_util.py PARTNER_ID "API_ADMIN_SECRET" METADATA_PROFILE_ID "ENTRY_ID"
```

Prerequisites:
- A Kaltura account with administrative privileges.
- Access to the Kaltura admin secret and partner ID for API authentication.
- The Kaltura Python client libraries installed in your Python environment.
- Basic understanding of XML and XSD structures.

Main Features:
- Fetch and parse the XSD of a Metadata Profile to understand the structure of metadata expected.
- Generate a new XML metadata template based on the profile's XSD, which can be used as a starting point for new entries.
- Retrieve existing metadata for an entry and present it in a structured format that aligns with the profile's XSD.
- Validate and add new metadata values to an entry, ensuring that the values meet the constraints set by the profile's XSD.
- Remove empty or redundant metadata elements that do not contain any data.
- Update existing metadata entries with new or modified values, and apply these updates to the Kaltura platform.

Usage and Extension:
- To use the script, one must pass the partner ID, admin secret, metadata profile ID, and the entry ID as arguments.
- The script can be extended to handle bulk updates by looping over multiple entries.
- Additional functions can be implemented to support more complex metadata operations, such as conditional updates, or synchronization with external data sources.
- Users can extend the error handling capabilities to provide more granular feedback.
- To facilitate ease of use, consider adding an interactive command-line interface or integrating with a web-based UI.
"""

import sys
import logging
import argparse
from lxml import etree as ET
from typing import List, Any, Optional
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import KalturaSessionType, KalturaFilterPager
from KalturaClient.Plugins.Metadata import ( KalturaMetadataFilter, KalturaMetadataProfile, KalturaMetadata, 
                                           KalturaMetadataObjectType )
from KalturaClient.exceptions import KalturaException

# Configuration Constants
SERVICE_URL = "https://cdnapi-ev.kaltura.com/"
SESSION_TYPE = KalturaSessionType.ADMIN
SESSION_DURATION = 86400
SESSION_PRIVILEGES = '*,disableentitlement'
SCRIPT_USER_ID = "metadata-tester"
XSD_NAMESPACE_URL = 'http://www.w3.org/2001/XMLSchema'
XSD_NAMESPACE = {'xsd': XSD_NAMESPACE_URL}

# Helper functions and classes
class MetadataUtils:
    @staticmethod
    def parse_xsd(xsd_string: str) -> ET.Element:
        """
        Parses an XSD string into an XML tree structure.
        """
        try:
            parser = ET.XMLParser(resolve_entities=False)
            return ET.fromstring(xsd_string, parser=parser)
        except ET.XMLSyntaxError as e:
            logging.error(f"Error parsing XSD: {e}")
            raise

    @staticmethod
    def build_metadata_template(xsd_root: ET.Element) -> ET.Element:
        """
        Constructs a metadata XML template based on the provided XSD root element.
        """
        metadata_element = ET.Element('metadata')
        for xsd_elem in xsd_root.findall(".//xsd:element", XSD_NAMESPACE):
            if xsd_elem.get('name') != 'metadata':
                ET.SubElement(metadata_element, xsd_elem.get('name')).text = ''
        return metadata_element

    @staticmethod
    def get_metadata_template_with_values(metadata_item: KalturaMetadata, xsd_root: ET.Element) -> ET.Element:
        """
        Generates a metadata XML template filled with values from an existing metadata item.
        """
        template_tree = MetadataUtils.build_metadata_template(xsd_root)
        item_tree = ET.fromstring(metadata_item.xml)

        for template_elem in list(template_tree):  # Use list to avoid modification issues during iteration
            field_name = template_elem.tag
            corresponding_item_elem = item_tree.find(f'.//{field_name}')
            
            if corresponding_item_elem is not None and corresponding_item_elem.text:
                template_elem.text = corresponding_item_elem.text
            else:
                xsd_elem = xsd_root.find(f".//xsd:element[@name='{field_name}']", XSD_NAMESPACE)
                minOccurs = xsd_elem.get('minOccurs') if xsd_elem is not None else None
                if minOccurs == '0':
                    template_tree.remove(template_elem)

        # Reconstruct metadata_xml if it becomes empty
        if not list(template_tree):
            return MetadataUtils.build_metadata_template(xsd_root)

        return template_tree

    @staticmethod
    def pretty_print_element(element: ET.Element) -> str:
        return ET.tostring(element, pretty_print=True, encoding='unicode')
    
    @staticmethod
    def is_field_multi_valued(field_name: str, xsd_root: ET.Element) -> bool:
        """
        Determines whether a field is multi-valued based on the XSD.
        """
        xsd_element = xsd_root.find(f".//xsd:element[@name='{field_name}']", namespaces=XSD_NAMESPACE)
        if xsd_element is not None:
            return xsd_element.get('maxOccurs') not in (None, '1')
        else:
            logging.warning(f"XSD does not define field '{field_name}'.")
            return False

    @staticmethod
    def get_restriction_values(field_name: str, xsd_root: ET.Element) -> List[str]:
        """
        Retrieves a list of allowed values for a field based on the XSD restrictions.
        """
        field_type_element = xsd_root.find(
            f".//xsd:element[@name='{field_name}']/xsd:simpleType", namespaces=XSD_NAMESPACE
        )
        if field_type_element is None:
            field_type_element = xsd_root.find(
                f".//xsd:element[@name='{field_name}']/../xsd:simpleType", namespaces=XSD_NAMESPACE
            )

        if field_type_element is not None:
            restriction = field_type_element.find('xsd:restriction', namespaces=XSD_NAMESPACE)
            if restriction is not None:
                return [enum.get('value') for enum in restriction.findall('xsd:enumeration', namespaces=XSD_NAMESPACE)]
        return []
    
    @staticmethod
    def find_position_for_new_element(metadata_element: ET.Element, field_name: str, xsd_root: ET.Element) -> Optional[int]:
        """
        Finds the position where the new element should be inserted in the metadata element.
        """
        # Assume the first sequence is where the metadata fields should be ordered
        sequence = xsd_root.find('.//xsd:complexType/xsd:sequence', XSD_NAMESPACE)
        if sequence is not None:
            for index, element in enumerate(sequence.findall('xsd:element', XSD_NAMESPACE)):
                if element.get('name') == field_name:
                    return index
        return None
    
    @staticmethod
    def remove_empty_elements(parent: ET.Element, field_name: str) -> None:
        """
        Removes all empty elements with the given field name from the parent element.
        """
        for element in parent.findall(f".//{field_name}"):
            if element.text is None or not element.text.strip():
                parent.remove(element)
    
    @staticmethod
    def add_value_to_metadata(metadata_element: ET.Element, field_name: str, value: Any, xsd_root: ET.Element):
        """
        Adds or updates a value for a specific field within the metadata, ensuring compliance with the XSD.
        """
        if metadata_element is None:
            raise ValueError("The metadata element provided is None.")

        multi_valued = MetadataUtils.is_field_multi_valued(field_name, xsd_root)
        restriction_values = MetadataUtils.get_restriction_values(field_name, xsd_root)

        if restriction_values and value not in restriction_values:
            raise ValueError(f"Value '{value}' is not allowed for field '{field_name}' based on the XSD restrictions.")

        existing_elements = metadata_element.findall(f".//{field_name}")
        
        # Check if the field is multi-valued as per XSD and adjust processing accordingly
        if multi_valued:
            if not any(elem.text == str(value) for elem in existing_elements):
                new_value_element = ET.Element(field_name)
                new_value_element.text = str(value)
                insert_position = MetadataUtils.find_insert_position(metadata_element, field_name, xsd_root)
                metadata_element.insert(insert_position, new_value_element)
        else:
            if existing_elements:
                existing_elements[0].text = str(value)
            else:
                new_value_element = ET.Element(field_name)
                new_value_element.text = str(value)
                insert_position = MetadataUtils.find_insert_position(metadata_element, field_name, xsd_root)
                metadata_element.insert(insert_position, new_value_element)

        if multi_valued:
            MetadataUtils.remove_empty_elements(metadata_element, field_name)
            
    @staticmethod
    def find_insert_position(metadata_element: ET.Element, field_name: str, xsd_root: ET.Element) -> int:
        """
        Determines the correct position to insert a new element within the metadata structure.
        """
        sequence = xsd_root.find('.//xsd:complexType/xsd:sequence', XSD_NAMESPACE)
        if sequence is not None:
            position = 0
            for xsd_elem in sequence.findall('xsd:element', XSD_NAMESPACE):
                if xsd_elem.get('name') == field_name:
                    return position
                if metadata_element.find(f".//{xsd_elem.get('name')}") is not None:
                    position += len(metadata_element.findall(f".//{xsd_elem.get('name')}"))
        return position

class KalturaMetadataManager:
    def __init__(self, partner_id: int, admin_secret: str):
        self.client = self._create_client(partner_id, admin_secret)

    def _create_client(self, partner_id: int, admin_secret: str) -> KalturaClient:
        config = KalturaConfiguration(partner_id)
        config.serviceUrl = SERVICE_URL
        client = KalturaClient(config)
        ks = client.generateSessionV2(
            admin_secret, SCRIPT_USER_ID, SESSION_TYPE,
            partner_id, SESSION_DURATION, SESSION_PRIVILEGES)
        client.setKs(ks)
        return client

    def fetch_metadata_profile(self, profile_id: int) -> str:
        """
        Fetches the XSD string of a metadata profile from the Kaltura platform.
        """
        try:
            metadata_profile: KalturaMetadataProfile = self.client.metadata.metadataProfile.get(profile_id)
            return metadata_profile.xsd
        except KalturaException as e:
            logging.error("Error fetching metadata profile: %s", e)
            raise

    def check_metadata_exists(self, entry_id: str, profile_id: int) -> bool:
        """
        Checks if metadata already exists for a given entry and profile ID.
        """
        filter = KalturaMetadataFilter()
        filter.metadataProfileIdEqual = profile_id
        filter.metadataObjectTypeEqual = KalturaMetadataObjectType.ENTRY
        filter.objectIdEqual = entry_id
        pager = KalturaFilterPager()

        result = self.client.metadata.metadata.list(filter, pager).objects
        return len(result) > 0, result[0] if result else None

    def create_or_get_metadata(self, entry_id: str, profile_id: int, xsd_root: ET.Element) -> ET.Element:
        """
        Creates a new or retrieves existing metadata XML for an entry based on its profile.
        """
        metadata_exists, metadata_item = self.check_metadata_exists(entry_id, profile_id)
        if metadata_exists and metadata_item:
            metadata_xml = MetadataUtils.get_metadata_template_with_values(metadata_item, xsd_root)
        else:
            metadata_xml = MetadataUtils.build_metadata_template(xsd_root)
            self.populate_default_values(metadata_xml, xsd_root, skip_optional=True)
        return metadata_xml

    def populate_default_values(self, metadata_xml: ET.Element, xsd_root: ET.Element, skip_optional: bool = False) -> None:
        """
        Populates default values for list types based on XSD enumeration restrictions.
        If an empty value is not allowed, the first value in the enumeration is selected.
        """
        for xsd_elem in xsd_root.findall(".//xsd:element", XSD_NAMESPACE):
            name = xsd_elem.get('name')
            minOccurs = xsd_elem.get('minOccurs')
            is_optional = minOccurs == '0'

            # Skip metadata root element
            if name == 'metadata':
                continue

            metadata_element = metadata_xml.find(f".//{name}")
            # Check if metadata_element is in the XML. If not, it was optional and already removed.
            if metadata_element is None:
                continue

            # Populate with default value if restrictions exist and it's not multi-valued
            restriction_values = MetadataUtils.get_restriction_values(name, xsd_root)
            if restriction_values and not MetadataUtils.is_field_multi_valued(name, xsd_root):
                first_value = restriction_values[0]
                if not metadata_element.text or not metadata_element.text.strip():
                    metadata_element.text = first_value
            elif is_optional and skip_optional:
                # For optional fields with no default value, set to empty if skipping
                metadata_element.text = ''
    
    def update_metadata(self, metadata_id: int, xml: str) -> KalturaMetadata:
        """
        Updates an existing metadata entry with new XML content.
        """
        try:
            return self.client.metadata.metadata.update(metadata_id, xml)
        except KalturaException as e:
            logging.error(f"Error updating metadata: {e}")
            raise

    def add_metadata(self, profile_id: int, object_type: KalturaMetadataObjectType, object_id: str, xml: str) -> KalturaMetadata:
        """
        Adds new metadata to an entry in the Kaltura platform.
        """
        try:
            return self.client.metadata.metadata.add(profile_id, object_type, object_id, xml)
        except KalturaException as e:
            logging.error(f"Error adding metadata: {e}")
            raise

    def apply_metadata_to_entry(self, entry_id: str, profile_id: int, xml: str) -> KalturaMetadata:
        """
        Applies metadata updates to a specific entry, either by adding or updating.
        """
        metadata_exists, metadata_item = self.check_metadata_exists(entry_id, profile_id)
        if metadata_exists:
            return self.update_metadata(metadata_item.id, xml)
        else:
            return self.add_metadata(profile_id, KalturaMetadataObjectType.ENTRY, entry_id, xml)
        

def parse_arguments() -> argparse.Namespace:
    """
    Parses and returns command-line arguments necessary for script execution.
    """
    parser = argparse.ArgumentParser(description='Kaltura Metadata Utility Script')
    parser.add_argument('partner_id', type=int, help='Kaltura partner ID')
    parser.add_argument('admin_secret', help='Kaltura admin secret')
    parser.add_argument('profile_id', type=int, help='Metadata profile ID')
    parser.add_argument('entry_id', help='Entry ID to update metadata for')
    return parser.parse_args()


def main():
    # Configure logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    args = parse_arguments()

    # instantiate
    kaltura_manager = KalturaMetadataManager(args.partner_id, args.admin_secret)
    
    # parse the schema
    xsd_string = kaltura_manager.fetch_metadata_profile(args.profile_id)
    xsd_root = MetadataUtils.parse_xsd(xsd_string)
    
    # create a metadata template or fetch an existing metadata item xml from the API
    metadata_xml = kaltura_manager.create_or_get_metadata(args.entry_id, args.profile_id, xsd_root)
    
    try:
        # make updates to specific fields
        logging.debug("Metadata XML before update: %s", ET.tostring(metadata_xml, encoding='unicode'))
        MetadataUtils.add_value_to_metadata(metadata_xml, 'Email', 'someone@test.com', xsd_root)
        logging.debug("Metadata XML after Email update: %s", ET.tostring(metadata_xml, encoding='unicode'))
        MetadataUtils.add_value_to_metadata(metadata_xml, 'Email', 'someone@example.com', xsd_root) # will override the previous value
        logging.debug("Metadata XML after 2nd Email update: %s", ET.tostring(metadata_xml, encoding='unicode'))
        MetadataUtils.add_value_to_metadata(metadata_xml, 'Format', 'Go-Pro camera', xsd_root)
        logging.debug("Metadata XML after Format update: %s", ET.tostring(metadata_xml, encoding='unicode'))
        MetadataUtils.add_value_to_metadata(metadata_xml, 'Categories', 'Testimonials', xsd_root) 
        logging.debug("Metadata XML after Categories/Testimonials update: %s", ET.tostring(metadata_xml, encoding='unicode'))
        MetadataUtils.add_value_to_metadata(metadata_xml, 'Categories', 'Nature party', xsd_root) 
        logging.debug("Metadata XML after Categories/Nature party update: %s", ET.tostring(metadata_xml, encoding='unicode'))
        
        logging.debug("Metadata updated successfully.")
        
    except ValueError as e:
        logging.error("Error updating metadata: %s", e)
    
    try:
        # add or update the metadata item to the entry
        updated_metadata = kaltura_manager.apply_metadata_to_entry(args.entry_id, args.profile_id, ET.tostring(metadata_xml, encoding='unicode'))
        logging.debug(f"Metadata for entry {args.entry_id} has been upsert.")
        
    except KalturaException as e:
        logging.error("Error applying metadata to entry: %s", e)
    
    # pretty print the xml
    pretty_xml = MetadataUtils.pretty_print_element(metadata_xml)
    logging.debug("applying metadata to entry ID %s :", args.entry_id)
    print(pretty_xml)

if __name__ == '__main__':
    main()
