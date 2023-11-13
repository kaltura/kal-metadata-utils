# Kaltura Metadata Utility Script

The Kaltura Metadata Utility Script is a Python tool designed to manage custom metadata for media entries on the Kaltura media platform. 
It interfaces with the Kaltura API, enabling users to fetch, update, and validate metadata in alignment with a Metadata Profile's XML Schema Definition (XSD).

## Features

* Metadata Retrieval and Parsing: Fetches and parses metadata profiles from Kaltura.
* Metadata Template Generation: Generates XML templates based on XSD for new entries.
* Metadata Updating: Validates and updates metadata entries, ensuring XSD compliance.
* Detailed Logging: Provides comprehensive logs for monitoring and troubleshooting.

## Prerequisites

* A Kaltura account with administrative privileges.
* Access to Kaltura's admin secret and partner ID.
* Python environment with Kaltura Python client libraries installed.
* Basic understanding of XML and XSD structures.

## Usage

Run the script with the required arguments: partner ID, admin secret, metadata profile ID, and entry ID.

```bash
python kaltura_metadata_xml_util.py PARTNER_ID API_ADMIN_SECRET METADATA_PROFILE_ID ENTRY_ID
```

## Configuration

Update the script's configuration constants (e.g., SERVICE_URL, SESSION_TYPE) as per your Kaltura environment setup.

## Documentation

Each function within the script includes inline documentation providing insights into its purpose and usage.

## Logging
Logging is set up to capture essential information, errors, and debugging messages. Modify the logging level in the script as needed.

## Contributions
Contributions to the script are welcome. Please ensure that any modifications maintain the alignment with Kaltura's API and XSD structures.

## License

This script is released under the [MIT License](https://opensource.org/license/mit/).  

## Support

This script is provided "as-is" without warranty or support. Use it at your own risk. If you encounter any issues, please report them in the repository's issues section.
