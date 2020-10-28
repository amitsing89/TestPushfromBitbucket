import sys
import codecs
import random
import itertools
import argparse
import logging

from avro import schema, datafile, io

from fastavro import iter_avro

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)


class AvroFile(argparse.FileType):
    """
    Creates objects that can be passed to the type argument of 
    `ArgumentParser.add_argument()`, from the `argparse` module.

    A created object is an Avro DataFileReader that is used for reading records.
    """

    def __init__(self):
        super(AvroFile, self).__init__(mode='r')

    def __call__(self, string):
        fp = super(AvroFile, self).__call__(string)
        return iter_avro(fp)


def take_first(it, n):
    """
    Take the first `n` items from `it`. If `n` is None, does nothing.
    """
    if not n is None:
        return itertools.islice(it, n)

    return it


def cat_sample(args):
    """
    Get a sample of the records of the file and print the records on the 
    standard output.
    """
    ftotal, fsample = itertools.tee(args.infile)

    total = 0
    for _ in take_first(args.infile, args.n):
        total += 1

    sample_itms = random.sample(range(total), args.sample_size)

    for n, itm in enumerate(fsample):
        if n in sample_itms:
            print n


def sample(args):
    """
    Select a random sample of the records from the input files
    and write them into an output file.

    This command assumes that the all the input files have the same
    schema.
    
    Arguments:
        infiles: Input files
        outfile: Output file
        sample_ratio: Ratio of records selected (0 <= ratio <= 1).
        codec:   Compression codec for the output
    
    """
    # Get the schema from the first file.
    json_schema = args.infiles[0].meta[datafile.SCHEMA_KEY]
    writers_schema = schema.parse(json_schema)

    rec_writer = io.DatumWriter()
    writer = datafile.DataFileWriter(args.outfile, rec_writer,
                                     writers_schema=writers_schema, codec=args.out_codec)

    for infile in args.infiles:
        try:
            for record in infile:
                if args.sample_ratio >= random.random():
                    writer.append(record)
        except:
            print >> sys.stderr, "Error reading file. Skipping", infile
            logging.exception('Error reading input file: %s' % infile)
            continue


def count(args):
    """
    Count the number of records in the file.
    """
    n = 0
    for infile in args.infiles:
        for _ in take_first(infile, args.n):
            print infile.next()
            n += 1
            # try:
            #     a = infile.next()
            #     print "CHECK",a
            # except Exception as e:
            #     print "EXC",e
            # finally:
            #     n+=1

    print "%d records" % n


def keys(args):
    """
    List the available keys in the file.
    """
    for infile in args.infiles:
        try:
            r = infile.next()
            print ",".join(r.keys())
        except StopIteration:
            print >> sys.stderr, "No records"


def cat_keys(args):
    """
    Print the contents of the values for certain keys of the record.
    """
    keys = args.keys.split(',')
    for infile in args.infiles:
        for r in take_first(infile, args.n):
            print ",".join([unicode(r.get(k)) for k in keys])


def cat(args):
    """
    Concatenates files and stores the result on an output file.

    It asumes that all the input files have the same schema
    """

    # Get the schema from the first file.
    json_schema = args.infiles[0].meta[datafile.SCHEMA_KEY]
    writers_schema = schema.parse(json_schema)

    rec_writer = io.DatumWriter()
    writer = datafile.DataFileWriter(args.outfile, rec_writer,
                                     writers_schema=writers_schema, codec=args.out_codec)

    for infile in args.infiles:
        try:
            for record in infile:
                writer.append(record)
        except:
            print >> sys.stderr, "Error reading file. Skipping", infile
            logging.exception('Error reading input file: %s' % infile)
            continue

    writer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, help='Read only the first N records')
    subparsers = parser.add_subparsers()

    commands = {
        'count': count,
        'keys': keys
    }

    for name, fun in commands.items():
        subparser = subparsers.add_parser(name, help=fun.__doc__)
        subparser.set_defaults(func=fun)
        subparser.add_argument('infiles', type=AvroFile(), nargs='+')

    subparser = subparsers.add_parser('cat_keys', help=cat_keys.__doc__)
    subparser.set_defaults(func=cat_keys)
    subparser.add_argument('keys', help='A list of keys (comma separated)')
    subparser.add_argument('infiles', type=AvroFile(), nargs='+')

    subparser = subparsers.add_parser('cat_sample', help=cat_sample.__doc__)
    subparser.set_defaults(func=sample)
    subparser.add_argument('sample_size', type=int, help='Sample size')
    subparser.add_argument('infile', type=AvroFile())

    subparser = subparsers.add_parser('sample', help=sample.__doc__)
    subparser.set_defaults(func=sample)
    subparser.add_argument('--out-codec', default='null')
    subparser.add_argument('sample_ratio', type=float, help='Sample ratio')
    subparser.add_argument('outfile', type=argparse.FileType('wb'))
    subparser.add_argument('infiles', type=AvroFile(), nargs='+')

    subparser = subparsers.add_parser('cat', help=cat.__doc__)
    subparser.set_defaults(func=cat)
    subparser.add_argument('--out-codec', default='null')
    subparser.add_argument('outfile', type=argparse.FileType('wb'))
    subparser.add_argument('infiles', type=AvroFile(), nargs='+')

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
