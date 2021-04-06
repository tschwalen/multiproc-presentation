import os
import time
import string
import collections
import multiprocessing
from functools import partial, reduce

PUNCTUATION = string.punctuation + '”“’‘'
TEXT_FILE_DIR = 'books/'
CHUNK_SIZE = 100


def word_filter(word: str):
    """ Removes all the punctuation from the passed word """
    return word.translate(str.maketrans('', '', PUNCTUATION)).lower()

def count_chunk(chunk: str):
    """ given a string chunk, returns a counter of word frequencies"""
    import collections
    chunk = chunk.replace('—', ' ').replace('-', ' ')
    return collections.Counter( ( word_filter(word) for word in chunk.split() ) )

def single_process_wordcount():
    """ gives us a word count for all files in TEXT_FILE_DIR, but using only one process/thread """ 
    print("\n-- Single-process example --\n")
    total_time = time.time()

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

    total_time = time.time() - total_time
    print("Total elapsed time: {}".format(total_time) )
    return word_freqs

def wordcount_data():
    """ chunk generator for the multiprocessed verion of the word counter """
    files = os.listdir(TEXT_FILE_DIR)
    for filename in files:
        init_time = time.time()
        with open(TEXT_FILE_DIR + filename, 'r', encoding="utf8") as file:
            lines = file.readlines(CHUNK_SIZE)
            while lines:
                yield ' '.join(lines)
                lines = file.readlines(CHUNK_SIZE)
        delta = time.time() - init_time
        print("Processing file '{}' took {}".format(filename, delta))


def multi_process_wordcount(num_workers=4):
    """ 
        gives us a word count for all files in TEXT_FILE_DIR, but utilizes multiple threads/processes 
        using the multiprocessing module 
    """ 

    print("\n-- Multi-process example --\n")
    total_time = time.time()

    pool = multiprocessing.Pool(num_workers)
    work_queue = wordcount_data()
    result = pool.map(count_chunk, work_queue)
    word_freqs = collections.Counter()
    for r in result:
        word_freqs.update(r)
    
    pool.close()
    total_time = time.time() - total_time
    print("Total elapsed time: {}".format(total_time) )
    return word_freqs



if __name__ == '__main__':
    import sys
    program_arguments = [ arg for arg in sys.argv[1:] if not arg.startswith('-') ]

    assert( len(program_arguments) <= 1 )

    if 'multi' in program_arguments:
        multi_process_wordcount()
    elif 'test' in program_arguments:
        spw = single_process_wordcount()
        mpw = multi_process_wordcount()
        print( "Identical results!" if spw == mpw else "Results differ!")
    else:
        single_process_wordcount()

