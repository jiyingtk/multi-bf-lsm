#!/usr/bin/python
import sys
import getopt
def process(a):
    c = 0.618**a*(1-0.618**a)/(1-0.618**(2*a))
    print " a=" + str(a) + " c=" + str(c)
def main():
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print __doc__
            sys.exit(0)
    # process arguments
    for arg in args:
        process(float(arg)) # process() is defined elsewhere

if __name__ == "__main__":
    main()
    

