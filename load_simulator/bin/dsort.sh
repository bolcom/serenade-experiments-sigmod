#!/bin/bash

sort_serenade_training_data() {
  # Sort the large training file for serenade on the 3rd column (Time).
  # We use an external sorting algorithm to minimize memory.
  # bk
  #
  if [ $# != 2 ]; then
    echo This functions needs an input file and an output file as parameters.
    exit -1
  fi
  ORIGINAL_FILE=$1
  SORTED_FILE=$2
  if [ ! -f "$ORIGINAL_FILE" ]; then
    echo "$ORIGINAL_FILE not found."
    exit -1
  fi
  tmp_dir=$(mktemp -d -t serenade-XXXXXXXXXX)
  CHUNK_FILE_PREFIX=${tmp_dir}/$(basename -- $ORIGINAL_FILE).split.
  SORTED_CHUNK_FILES=$CHUNK_FILE_PREFIX*.sorted

  #Cleanup any leftover files
  rm -f $SORTED_CHUNK_FILES > /dev/null
  rm -f $CHUNK_FILE_PREFIX* > /dev/null

  echo Splitting $ORIGINAL_FILE into chunks ...
  split -l 500000 $ORIGINAL_FILE $CHUNK_FILE_PREFIX

  echo Sorting all chunks ...
  for file in $CHUNK_FILE_PREFIX*; do
      sort -n -k 3 $file > $file.sorted
  done

  echo Merging chunks to $SORTED_FILE ...
  sort -n -k 3 -m $SORTED_CHUNK_FILES > $SORTED_FILE

  #Cleanup any leftover files
  rm -f $SORTED_CHUNK_FILES > /dev/null
  rm -f $CHUNK_FILE_PREFIX* > /dev/null
}

sort_serenade_training_data $1 $2