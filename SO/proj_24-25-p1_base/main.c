#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>


int kvs_run(int fd_input, int fd_output) {
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(fd_input)) { // colocar um fopen por exemplo
      case CMD_WRITE:
        num_pairs = parse_write(fd_input, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          break;
        }

        if (kvs_write(num_pairs, keys, values)) { //FIXME: adicionar fd_output
          fprintf(stderr, "Failed to write pair\n");
        }

        break;

      case CMD_READ:
        num_pairs = parse_read_delete(fd_input, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          break;
        }

        if (kvs_read(num_pairs, keys, fd_output)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(fd_input, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          break;
        }

        if (kvs_delete(num_pairs, keys)) { //FIXME: adicionar fd_output
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(fd_output);
        break;

      case CMD_WAIT:
        if (parse_wait(fd_input, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          break;
        }

        if (delay > 0) {
          printf("Waiting...\n");
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:

        if (kvs_backup()) {
          fprintf(stderr, "Failed to perform backup.\n");
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
       printf(
            "Available commands:\n"
            "  WRITE [(key,value),(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n"
        ); 

        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        return 0;
    }
    return 0;
  }
}
int main(int argc, char *argv[]) {
  if (argc > 3 || argc < 2) {
    fprintf(stderr, "Usage: %s <directory_path> <max_concurrent_backups>\n", argv[0]);
    return 1;
  }

  const char *directory_path = argv[1];
  //int max_concurrent_backups = atoi(argv[2]);

  DIR *dir = opendir(directory_path);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory\n");
    return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0 && strcmp(strrchr(entry->d_name, '.'), ".job") == 0) {
      
      char *ponto = strrchr(entry->d_name, '.');
      if (ponto == NULL || strcmp(ponto, ".job") != 0)
      {
        continue;
      }
      
      char job_file_path[MAX_FILE_PATH_SIZE];
      snprintf(job_file_path, sizeof(job_file_path), "%s/%s", directory_path, entry->d_name);

      int job_file = open(job_file_path, O_RDONLY);
      if (job_file == -1) {
          fprintf(stderr, "Error opening file: %s\n", job_file_path);
          closedir(dir);
          return 1;
      }
          // Check if its the file has .job extension
      

      ponto = strrchr(entry->d_name, '.');
      *ponto = '\0';

      char output_file_path[MAX_FILE_PATH_SIZE];
      snprintf(output_file_path, MAX_FILE_PATH_SIZE, "%s/%s.out", directory_path, entry->d_name);

      int output_file = open(output_file_path, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR | S_IROTH | S_IRGRP);
  

      kvs_run(job_file, output_file);

      close(output_file);
      close(job_file);
    }
  }

  closedir(dir);
  kvs_terminate();
  return 0;
}
