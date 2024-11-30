#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;


/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_file) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

    char msgToOutputFile[MAX_WRITE_SIZE] = "";
    strcat(msgToOutputFile, "[(");
      for (size_t i = 0; i < num_pairs; i++){
        strcat(msgToOutputFile, keys[i]);
        char *value = read_pair(kvs_table, keys[i]);
        if (value != NULL) {
          strcat(msgToOutputFile, ",");
          strcat(msgToOutputFile, value);
        }
        else {
          strcat(msgToOutputFile, ",KVSMISSING");
        }
        free(value);
      }

      strcat(msgToOutputFile, ")]");
      write(output_file, msgToOutputFile, strlen(msgToOutputFile));
      return 0;
    
}


int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      fprintf(stderr, "(%s,KVSMISSING)", keys[i]);
    }
  }

  return 0;
}

void kvs_show(int output_file) { 
  char msgToOutputFile[MAX_WRITE_SIZE];
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *currentNode = kvs_table->table[i];
    while (currentNode != NULL) {
      sprintf(msgToOutputFile, "(%s, %s)\n", currentNode->key, currentNode->value);
      write(output_file, msgToOutputFile, strlen(msgToOutputFile));
      currentNode = currentNode->next; // Move to the next node
    }
  }
}

int kvs_backup() {
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}
