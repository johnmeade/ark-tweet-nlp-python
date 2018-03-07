#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Simple Python wrapper for runTagger.sh script for CMU's Tweet Tokeniser
and Part of Speech tagger: http://www.ark.cs.cmu.edu/TweetNLP/

POS tags are represented with a single ASCII symbol. In brief:

* __Nominal__
  `N` common noun
  `O` pronoun (personal/WH; not possessive)
  `^` proper noun
  `S` nominal + possessive
  `Z` proper noun + possessive
* __Other open-class words__
  `V` verb incl. copula, auxiliaries
  `A` adjective
  `R` adverb
  `!` interjection
* __Other closed-class words__
  `D` determiner
  `P` pre- or postposition, or subordinating conjunction
  `&` coordinating conjunction
  `T` verb particle
  `X` existential _there_, predeterminers
* __Twitter/online-specific__
  `#` hashtag (indicates topic/category for tweet)
  `@` at-mention (indicates another user as a recipient of a tweet)
  `~` discourse marker, indications of continuation of a message across multiple tweets
  `U` URL or email address
  `E` emoticon
* __Miscellaneous__
  `$` numeral
  `,` punctuation
  `G` other abbreviations, foreign words, possessive endings, symbols, garbage
* __Other Compounds__
  `L` nominal + verbal (e.g. _i'm_), verbal + nominal (_let's_, _lemme_)
  `M` proper noun + verbal
  `Y` `X` + verbal

Modified August 2017 by John Meade
"""


import shlex, pexpect
from time import time, sleep


printhead = '[ {:^15} ] '.format( 'Tagger' )
def p( msg ): print( printhead + msg )


class TweetTagger:


    def __init__( self, java_opts='-XX:ParallelGCThreads=2 -Xmx500m', jarpath='ark-tweet-nlp-0.3.2/ark-tweet-nlp-0.3.2.jar' ):
        # NOTE default java options are directly lifted from original
        # java implementation. Example of the executed command:
        #   java -XX:ParallelGCThreads=2 -Xmx500m -jar vendor/ark-tweet-nlp-0.3.2/ark-tweet-nlp-0.3.2.jar --output-format conll
        self.cmd = ' '.join([ 'java', java_opts, '-jar', jarpath, '--output-format', 'conll' ])
        self.proc = pexpect.spawn( self.cmd, echo=False )
        self.proc.expect('Listening on stdin for input\.  \(\-h for help\)')


    def kill( self ):
        self.proc.kill( 1 )


    def __enter__( self ):
        return self


    def __exit__( self, typ, value, traceback ):
        self.kill()


    def _parse_raw_result( self, raw_result ):
        """Parse the tab-delimited returned lines, modified from:
        https://github.com/brendano/ark-tweet-nlp/blob/master/scripts/show.py
        """
        rows = raw_result.split('\r\n')
        for line in rows:
            line = line.strip()  # remove '\n'
            if len(line) > 0:
                if line.count( '\t' ) == 2:
                    parts = line.split( '\t' )
                    tokens = parts[0]
                    tags = parts[1]
                    confidence = float( parts[2] )
                    yield tokens, tags, confidence


    def batch( self, tweets ):
        """Call runTagger.sh on a list of tweets, parse the result, return lists of tuples of (term, type, confidence)"""

        # remove carriage returns and newlines, as they are interpretted as
        # tweet separators by the tagger
        tweets_cleaned = [ tw.replace('\n', ' ').replace('\r', ' ') for tw in tweets ]
        message = "\n".join( tweets_cleaned )

        # force UTF-8 encoding (from internal unicode type) to avoid .communicate encoding error as per:
        # http://stackoverflow.com/questions/3040101/python-encoding-for-pipe-communicate
        # message = message.encode( 'utf-8' )

        # print(message, file=self.proc.stdin, flush=True)
        self.proc.write( (message + '\n\n').encode('utf-8') )
        # the output of the tagger will terminate with 4 newlines => use this
        # to detect batch completion
        try:
            self.proc.expect( '\r\n\r\n\r\n', timeout=30 )
        except:
            p('Exception while tagging tweets')
            return []

        # parse into a list of strings, ie a result for each input message
        raw = self.proc.before.strip().decode('utf-8')
        # occassionally there is a header to trim off...
        raw = ''.join( raw.split('Detected text input format') ).strip()
        # avoid missing result for empty lines?
        # pos_result = pos_result.replace( "\n\n", "\n\n\n" )
        # split messages by double carriage returns
        raw_results = raw.split( '\r\n\r\n' )
        # parse each raw result into it's PoS tags
        return [ list( self._parse_raw_result( r ) ) for r in raw_results ]


if __name__ == "__main__":
    with TweetTagger( jarpath='vendor/ark-tweet-nlp-0.3.2.jar' ) as tw_tag:
        print( "\nTweet PoS demo (first call will be slow while Java is booting up)")

        def demo(tweets):
            print( '\nProcessing: ' + str( tweets ) )
            ti = time()
            res = tw_tag.batch( tweets )
            tf = time()
            print( 'Results: ' + str( res ) )
            print( 'Took: {} seconds'.format( tf - ti ) )

        demo([ 'this is a message', 'and a second message' ])
        demo([ 'this is a third message', 'and a fourth message' ])
        demo([ 'this is a fifth message', '' ])
