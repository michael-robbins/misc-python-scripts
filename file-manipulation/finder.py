#!/usr/bin/env python

import io
import os
import csv
import glob
import zipfile
import argparse
import datetime


def build_file_glob(prefix=None, file_date=None, postfix=None, extension=None):
    """
    Takes various parts of a file name and builds a 'glob' that will match those parts
    :param prefix: 
    :param file_date: 
    :param postfix: 
    :param extension: 
    :return: 
    """
    base = "*"

    if prefix is not None:
        base = "{0}*".format(prefix)

    if file_date is not None:
        base += "{0}*".format(file_date)

    if postfix is not None:
        base += postfix

    if extension is not None:
        base += extension

    return base


def locate_files(folder, file_glob):
    """
    Takes a folder and a glob and yields files that match the glob
    :param folder: 
    :param file_glob: 
    :return: 
    """
    for file in glob.glob(os.path.join(folder, file_glob)):
        yield file


def match_file(reader, match_conditions=None, v=0):
    """
    Takes a file object and match conditions, and will return a all rows matching our conditions!
    :param reader: 
    :param match_conditions: 
    :return: 
    """

    header = list()
    matches = list()

    if v:
        print("Match conditions: {0}".format(match_conditions))

    if not match_conditions or len(match_conditions) == 0:
        return header, matches

    for i, row in enumerate(reader):
        if i == 0:
            header = row
            continue

        for column, condition in match_conditions:
            data = row[column]

            if condition(data):
                if v:
                    print("Match for row: {0}".format(row))

                matches.append(row)

    if v and len(matches) > 0:
        print("Returning matches: {0}".format(matches))

    return header, matches


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", action="count")

    # Options to determine which files to process
    parser.add_argument("--file-folder", required=True, help="Folder the files live in")
    parser.add_argument(
        "--file-prefix",
        default=None,
        help="Prefix we use to determine which files to process",
    )
    parser.add_argument(
        "--file-postfix",
        default=None,
        help="Postfix we use to determine which files to process",
    )
    parser.add_argument(
        "--file-extension",
        default=".zip",
        help="The file extension of the files we want to process",
    )
    parser.add_argument(
        "--file-date", default=None, help="Single date we will attempt to find"
    )
    parser.add_argument(
        "--file-start-date",
        default=None,
        help="Start of the date range we will look for",
    )
    parser.add_argument(
        "--file-end-date", default=None, help="End of the date range we will look for"
    )

    # Options to determine how we process each file
    parser.add_argument(
        "--file-delimiter",
        default=",",
        help="Delimiter we use to break a line apart into columns",
    )
    parser.add_argument(
        "--match-column",
        required=True,
        type=int,
        help="Column we will use to perform a match on (zero indexed)",
    )
    parser.add_argument(
        "--match-comparison",
        required=True,
        help="Type of comparison we perform, eg. '=', '!=', '>' or '<'",
    )
    parser.add_argument(
        "--match-value",
        required=True,
        help="Value we will perform the comparison operator on",
    )

    # Options to determine the output file format
    parser.add_argument(
        "--output-folder",
        required=True,
        default=None,
        help="Folder where we will write the output file to",
    )
    parser.add_argument(
        "--output-columns",
        default=None,
        help="Which columns of the input files we will output, ZERO BASED. Eg. 0,1,2,5-6,2-3",
    )
    parser.add_argument(
        "--output-delimiter",
        default=",",
        help="Delimiter we use to combine the columns into a line",
    )

    args = parser.parse_args()

    # Ensure the delimiters are sane
    if args.file_delimiter == "csv":
        args.file_delimiter = ","
    elif args.file_delimiter == "tsv":
        args.file_delimiter = "\t"
    elif len(args.file_delimiter) > 1:
        parser.error(
            "Delimiter must be a single character. Eg. ',', you provided >{0}<".format(
                args.file_delimiter
            )
        )

    if args.output_delimiter == "csv":
        args.output_delimiter = ","
    elif args.output_delimiter == "tsv":
        args.output_delimiter = "\t"
    elif len(args.output_delimiter) > 1:
        parser.error(
            "Delimiter must be a single character. Eg. ',', you provided >{0}<".format(
                args.file_delimiter
            )
        )

    # Ensure the timestamp column number is sane (positive integer)
    if args.match_column <= 0:
        parser.error("--match-column needs to be a positive integer")

    # Ensure the column spec is valid
    if args.output_columns is not None:
        for column_spec in args.output_columns.split(","):
            if "-" in column_spec:
                range_start, range_end = map(lambda x: int(x), column_spec.split("-"))
                if range_start >= range_end:
                    parser.error(
                        "--output-columns has a bad range in it: {0}".format(
                            column_spec
                        )
                    )

    # Build the comparison function
    comparators = {
        "==": lambda match: lambda value: value == match,
        "!=": lambda match: lambda value: value != match,
        ">": lambda match: lambda value: value > match,
        "<": lambda match: lambda value: value < match,
    }

    if args.match_comparison not in comparators:
        parser.error("--match-comparison needs to be '==', '!=', '>' or '<'")

    # Build up a 'config' object that will contain everything the sampling function will need
    config = {
        "match_conditions": [
            (args.match_column, comparators[args.match_comparison](args.match_value))
        ]
    }

    # Build up a list of dates that we need to look for
    dates = []
    date_format = "%Y%m%d"

    if args.file_date is not None:
        dates.append(datetime.datetime.strptime(args.file_date, date_format))
    elif args.file_start_date is not None and args.file_end_date is not None:
        dates = []

        start_date = datetime.datetime.strptime(args.file_start_date, date_format)
        end_date = datetime.datetime.strptime(args.file_end_date, date_format)

        while start_date <= end_date:
            dates.append(start_date)
            start_date += datetime.timedelta(days=1)
    else:
        parser.error(
            "You need to provide either --file-date OR (--file-start-date AND --file-end-date)"
        )

    # Loop over each found file and process it
    csv_header = []
    csv_matches = []

    if args.v:
        print("Processing with dates: {0}".format(dates))

    for date in dates:
        # Attempt to build the 'glob' that will return all our files for the given date
        filename_glob = build_file_glob(
            args.file_prefix,
            date.strftime("%Y%m%d"),
            args.file_postfix,
            args.file_extension,
        )

        if args.v:
            print(
                "Looking for file glob '{0}' for date {1}".format(filename_glob, date)
            )

        for zip_filename in locate_files(args.file_folder, filename_glob):
            print("Processing: {0}".format(zip_filename))
            # The name of the CSV file within the ZIP is the name of the zip file with the file extension swapped
            csv_filename = os.path.basename(zip_filename.replace(".zip", ".csv"))

            with zipfile.ZipFile(zip_filename) as zip_file:
                with zip_file.open(csv_filename) as csv_file:
                    csv_reader = csv.reader(
                        io.TextIOWrapper(csv_file), delimiter=args.file_delimiter
                    )
                    file_header, file_matches = match_file(
                        csv_reader, v=args.v, **config
                    )

                    if not csv_header:
                        csv_header = file_header

                    if csv_header != file_header:
                        print("WARNING: CSV Headers are different between ZIP files?")

                    csv_matches.extend(file_matches)

    if args.v and len(csv_matches) > 0:
        print("Total matches: {0}".format(csv_matches))

    # Build the output filename
    output_filename = "Matches"

    if args.file_prefix is not None:
        output_filename += "_{0}".format(args.file_prefix)

    if args.file_date is not None:
        output_filename += "_{0}".format(args.file_date)
    elif args.file_start_date is not None and args.file_end_date is not None:
        output_filename += "_{0}-{1}".format(args.file_start_date, args.file_end_date)

    if args.file_postfix is not None:
        output_filename += "_{0}".format(args.file_postfix)

    output_filename += ".csv"
    output_filename = os.path.join(args.output_folder, output_filename)
    print("Writing to: {0}".format(output_filename))

    # Write out to the output file
    with open(output_filename, "wt") as output_file:
        writer = csv.writer(
            output_file, delimiter=args.output_delimiter, lineterminator="\n"
        )

        # Write the header
        if args.output_columns is not None:
            new_csv_header = []

            for column_spec in args.output_columns.split(","):
                if "-" in column_spec:
                    range_start, range_end = map(
                        lambda x: int(x), column_spec.split("-")
                    )
                    new_csv_header.extend(csv_header[range_start:range_end])
                else:
                    new_csv_header.append(csv_header[column_spec])

            writer.writerow(new_csv_header)
        else:
            writer.writerow(csv_header)

        # Write out all matches
        for match_row in csv_matches:
            if args.output_columns is not None:
                new_match_row = []

                for column_spec in args.output_columns.split(","):
                    if "-" in column_spec:
                        range_start, range_end = map(
                            lambda x: int(x), column_spec.split("-")
                        )
                        new_match_row.extend(match_row[range_start:range_end])
                    else:
                        new_match_row.append(match_row[column_spec])

                writer.writerow(new_match_row)
            else:
                writer.writerow(match_row)
