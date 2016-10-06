#!/usr/bin/python2.7
"""Push munki repo directories to s3 with cache and storage class metadata."""
import subprocess
import os
import sys
from Foundation import CFPreferencesCopyAppValue


def read_preference(key, bundle):
    """Read a preference key from a preference domain."""
    value = CFPreferencesCopyAppValue(key, bundle)
    return value


def aws_push(source, destination, cache_age, storage_class):
    """Sync a directory to an s3 bucket with a cache control length."""
    cmd = ['aws', 's3', 'sync', source, destination, '--cache-control',
           'max-age=' + str(cache_age), '--storage-class', storage_class]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    out, err = process.communicate()
    return out, err


def main():
    """Sync munki repo directories."""
    # Read preferences from domain
    bundle = 'com.github.aaronburchfield.pushrepo'
    s3_bucket = os.path.join('s3://', read_preference('bucket', bundle))
    default_class = read_preference('storage_class',
                                    bundle) or 'REDUCED_REDUNDANCY'
    repo_path = read_preference('repo_path', bundle)
    default_age = read_preference('default_age', bundle) or 86400

    if not os.path.isdir(repo_path):
        print "Can't locate repo: %s" % repo_path
        sys.exit(1)

    # Read each subdirectory of the munki repo
    subdirs = os.listdir(repo_path)
    for directory in subdirs:
        # Skip non-munkirepo directories
        if directory.startswith('.'):
            continue

        source = os.path.join(repo_path, directory)
        destination = os.path.join(s3_bucket, directory)
        # Check preferences for cache age and storage class for this directory
        cache_age = read_preference((directory + '_age'),
                                    bundle) or default_age
        storage_class = read_preference((directory + '_storage'),
                                        bundle) or default_class
        out, err = aws_push(source, destination, cache_age, storage_class)


if __name__ == '__main__':
    main()
