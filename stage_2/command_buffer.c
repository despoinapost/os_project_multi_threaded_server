#include <stdio.h>

#include "command_buffer.h"

// Initialize the command buffer before the communication threads start producing commands 
// and the executor thread starts consuming them.

int command_buffer_init(command_buffer_t *buffer)
{
    if(buffer == NULL){
        fprintf(stderr, "Error -- Command buffer is NULL.\n");
        return -1;
    }

    buffer->out = 0;
    buffer->in = 0;
    buffer->count = 0;

    int error;
    error = pthread_mutex_init(&buffer->mutex, NULL);
    if(error != 0){
        fprintf(stderr, "Error -- Could not initialize buffer mutex. \n");
        return -1;
    }

    error = pthread_cond_init(&buffer->not_empty, NULL);
    if(error != 0){
        fprintf(stderr, "Error -- Could not initialize not_empty condition variable. \n");
        pthread_mutex_destroy(&buffer->mutex);
        return -1;
    }

    error = pthread_cond_init(&buffer->not_full, NULL);
    if(error != 0){
        fprintf(stderr, "Error -- Could not initialize not_full condition variable. \n");
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return -1;
    }

    return 0;
}

//Communication threads (producers) call this function.
//Insert a command into the buffer. 
int command_buffer_put(command_buffer_t *buffer, command_t *next_command)
{
    if(buffer == NULL || next_command == NULL){
        fprintf(stderr, "Error -- Buffer or command is NULL.\n");
        return -1;
    }

    pthread_mutex_lock(&buffer->mutex);
    
    while(buffer->count == BABBLE_PRODCONS_SIZE){
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    buffer->commands[buffer->in] = next_command;
    buffer->in = (buffer->in + 1) % BABBLE_PRODCONS_SIZE;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

//Executor thread (consumer) calls this function.
//Remove a command from the buffer.
int command_buffer_get(command_buffer_t *buffer, command_t **next_command)
{
    if(buffer == NULL || next_command == NULL){
        fprintf(stderr, "Error -- Buffer or command is NULL.\n");
        return -1;
    }

    pthread_mutex_lock(&buffer->mutex);
    

    while(buffer->count == 0){
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    *next_command = buffer->commands[buffer->out];
    buffer->out = (buffer->out + 1) % BABBLE_PRODCONS_SIZE;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}
