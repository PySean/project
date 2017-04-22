#!/usr/bin/perl

while (<STDIN>) {
    if (/.*shuffle\s*bytes=(\d*)/) {
        print "$1\n";
    }
}
