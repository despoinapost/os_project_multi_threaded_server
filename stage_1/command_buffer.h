#ifndef __COMMAND_BUFFER_H__
#define __COMMAND_BUFFER_H__

#include <pthread.h>
#include "babble_config.h"
#include "babble_types.h"

typedef struct command_buffer{
    command_t *commands[BABBLE_PRODCONS_SIZE]; //buffer of commands size BABBLE_PRODCONS_SIZE = 4 
    unsigned int out; //position of the next command to be removed from the buffer
    unsigned int in; //position of the next command to be added to the buffer
    unsigned int count; //number of commands currently in the buffer
    pthread_mutex_t mutex; //protects the data when multiple threads access the buffer concurrenctly
    pthread_cond_t not_empty; //notify the executor (consumer) that there is a new command in the buffer
    pthread_cond_t not_full; //notify a communication thread (producer) that there is space in the buffer to add a new command
}command_buffer_t;

int command_buffer_init(command_buffer_t *buffer);
int command_buffer_put(command_buffer_t *buffer, command_t *next_command);
int command_buffer_get(command_buffer_t *buffer, command_t **next_command);

#endif