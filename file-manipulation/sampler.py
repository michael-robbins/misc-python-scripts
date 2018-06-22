#!/usr/bin/env python3

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
    for i in glob.glob(os.path.join(folder, file_glob)):
        yield i


def sample_file(reader, timestamp_column, timestamp_format, timestamp_delta):
    """
    Takes a file object and a few details, and will return a sample of the rows in the file matching our delta
    :param reader: 
    :param timestamp_column: 
    :param timestamp_format: 
    :param timestamp_delta: 
    :return: 
    """
    header = list()
    samples = list()

    for i, row in enumerate(reader):
        if i == 0:
            header = row
            continue

        timestamp_cell = row[timestamp_column]
        timestamp = datetime.datetime.strptime(timestamp_cell, timestamp_format)

        # Remove the last character from the last column as it's the linebreak '\n'
        row[-1] = row[-1][:-1]

        # If we have not saved a sample yet
        if len(samples) == 0:
            samples.append((timestamp, row))
            # print("Appending our first sample at {0}".format(timestamp.strftime("%H:%M:%S")))

        # If the last timestamp we sampled + the delta is less than the current timestamp, sample this row
        if samples[-1][0] + timestamp_delta < timestamp:
            samples.append((timestamp, row))
            # print("Appending our next sample at {0}".format(timestamp.strftime("%H:%M:%S")))

    return header, samples


def build_output_filename(args, prefix="Samples", ext=".csv"):
    """
    Given the cmd args, a prefix and extension return a valid consistent filename
    :param args:
    :param prefix:
    :param ext:
    :return:
    """
    parts = [prefix]

    if args.file_prefix is not None:
        parts.append(str(args.file_prefix))

    if args.file_date is not None:
        parts.append(str(args.file_date))
    elif args.file_start_date is not None and args.file_end_date is not None:
        parts.append("{0}-{1}".format(args.file_start_date, args.file_end_date))

    if args.file_postfix is not None:
        parts.append(str(args.file_postfix))

    output_filename = "_".join(parts) + ext

    return os.path.join(args.output_folder, output_filename)


def map_column_spec_to_row(column_spec, row):
    """
    Maps the provided column spec to a row, returns what the column spec dictates
    Eg.
        For a given input row: ['a', 'b', 'c', 'd', 'e', 'f']

        '1,2,3'   => ['a', 'b', 'c']
        '1,3-5'   => ['a', 'c', 'd', 'e']
        '1-3,5,2' => ['a', 'b', 'c', 'e', 'b']
    :param column_spec:
    :param row:
    :return:
    """
    new_row = []

    for token in column_spec.split(","):
        if "-" in token:
            range_start, range_end = map(int, token.split("-"))
            new_row.append(row[range_start:range_end])
        else:
            new_row.append(row[int(token)])

    return new_row


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
        "--timestamp-column",
        required=True,
        type=int,
        help="Column we will use to extract that rows timestamp, ZERO BASED",
    )
    parser.add_argument(
        "--timestamp-format",
        default="%I:%M:%S %p",
        help="What format the datetime column is, default to: 12hour:min:secs AM",
    )
    parser.add_argument(
        "--timestamp-delta", default="00:15:00", help="HH:MM:SS that we will sample at"
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

    # Parse the delta
    hours, minutes, seconds = map(lambda x: int(x), args.timestamp_delta.split(":"))

    # Ensure the timestamp column number is sane (positive integer)
    if args.timestamp_column <= 0:
        parser.error("--timestamp-column needs to be a positive integer")

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

    # Build up a 'config' object that will contain everything the sampling function will need
    config = {
        "timestamp_delta": datetime.timedelta(
            hours=hours, minutes=minutes, seconds=seconds
        ),
        "timestamp_format": args.timestamp_format,
        "timestamp_column": args.timestamp_column,
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

    # Open the output file
    output_filename = build_output_filename(args)
    print("Writing to: {0}".format(output_filename))

    # Write out to the output file
    with open(output_filename, "wt") as output_file:
        writer = csv.writer(
            output_file, delimiter=args.output_delimiter, lineterminator="\n"
        )

        # Loop over each found file and process it
        csv_header = []
        header_written = False

        for date in dates:
            # Attempt to build the 'glob' that will return all our files for the given date
            filename_glob = build_file_glob(
                args.file_prefix,
                date.strftime("%Y%m%d"),
                args.file_postfix,
                args.file_extension,
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
                        file_header, file_samples = sample_file(csv_reader, **config)

                        if not header_written:
                            csv_header = file_header

                            if args.output_columns is not None:
                                writer.writerow(
                                    map_column_spec_to_row(
                                        args.output_columns, csv_header
                                    )
                                )
                            else:
                                writer.writerow(csv_header)

                            header_written = True

                        if csv_header != file_header:
                            print(
                                "WARNING: CSV Headers are different between ZIP files?"
                            )

                        # Write out all samples
                        for sample_row in file_samples:
                            if args.output_columns is not None:
                                writer.writerow(
                                    map_column_spec_to_row(
                                        args.output_columns, sample_row
                                    )
                                )
                            else:
                                writer.writerow(sample_row)
