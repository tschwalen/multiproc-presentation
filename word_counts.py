import os
import time
import string
import collections
from functools import partial

PUNCTUATION = string.punctuation + '”“’‘'
TEXT_FILE_DIR = 'books/'
CHUNK_SIZE = 100

def word_filter(word: str):
    return word.translate(str.maketrans('', '', PUNCTUATION)).lower()

def count_chunk(chunk):
    import collections
    chunk = chunk.replace('—', ' ').replace('-', ' ')
    return collections.Counter( ( word_filter(word) for word in chunk.split() ) )

def single_process_wordcount():
    files = os.listdir(TEXT_FILE_DIR)
    word_freqs = collections.Counter()

    for filename in files:
        init_time = time.time()
        with open(TEXT_FILE_DIR + filename, 'r', encoding="utf8") as file:
            lines = file.readlines(CHUNK_SIZE)
            while lines:
                word_freqs.update( count_chunk( ' '.join(lines) ) )
                lines = file.readlines(CHUNK_SIZE)
        delta = time.time() - init_time
        print("Processing file '{}' took {}".format(filename, delta))

    print(word_freqs)

if __name__ == '__main__':
    import sys
    program_arguments = [ arg for arg in sys.argv[1:] if not arg.startswith('-') ]

    assert( len(program_arguments) <= 1 )

    if 'multi' in program_arguments:
        pass
    else:
        single_process_wordcount()

