# OutParser

A simple utility to quickly parse a folder containing multiple Outlook .msg files into a friendly JSON file, also extracting attachments into a dedicated folder.
This is in essence just a wrapper around the `extract-msg` library with some parsing logic to fix common issues and facilitate data analysis.

## Usage

```bash
$ pip install -r requirements.txt
$ python3 outparser.py <DIRECTORY> [flags]
```

For additional parsing options read the `--help`.
