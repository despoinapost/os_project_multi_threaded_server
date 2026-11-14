#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "babble_timeline.h"
#include "babble_server.h"
#include "babble_communication.h"


timeline_t* timeline_create(unsigned long client_key)
{
    timeline_t* tm= malloc(sizeof(timeline_t));
    tm->youngest = 0;
    tm->count_recent_adds = 0;
    tm->key = client_key;
    pthread_mutex_init(&tm->mutex, NULL);
    
    return tm;
}

void timeline_free(timeline_t *timeline)
{
    free(timeline);
}


time_t timeline_insert(timeline_t *tm, client_bundle_t *publisher, char *msg)
{
    struct timespec tt;
    
    //maybe we need lock here
    pthread_mutex_lock(&tm->mutex);
    publication_t *pub= &tm->circular_buffer[tm->youngest];
    
    clock_gettime(CLOCK_REALTIME, &tt);
    
    memset(pub->msg, 0, BABBLE_PUBLICATION_MSIZE);    
    strncpy(pub->msg, msg, BABBLE_PUBLICATION_MSIZE);

    pub->date = tt.tv_sec - server_start;
    snprintf(pub->msg, BABBLE_BUFFER_SIZE,"    %s[%ld]: %s\n", publisher->client_name, pub->date, msg);
    
    /* shifting the index */
    tm->youngest = (tm->youngest + 1) % BABBLE_TIMELINE_MSIZE;

    tm->count_recent_adds++;

    pthread_mutex_unlock(&tm->mutex);
    //maybe we need unlock here
    return pub->date;
}



//maybe we need lock here 
void timeline_generate_summary(timeline_t *tm, answer_t **answer)
{
    answer_t *the_answer = NULL;
    unsigned int index_first = 0;

    the_answer = alloc_answer(tm->key);
    //because each time we allocate answer for the specific client
    //we don't need to use lock inside alloc_answer()
    
    /* the first msg of the answer is the number of publications since
     * the last call to timeline */    

    pthread_mutex_lock(&tm->mutex);
    add_msg_to_answer(the_answer, sizeof(unsigned int), &tm->count_recent_adds);

    /* compute the index of the first msg to add to the timeline */
    if(tm->count_recent_adds >= BABBLE_TIMELINE_MSIZE){
        index_first = tm->youngest;
        
        /* deal with the corner case where the buffer is full */
        add_msg_to_answer(the_answer, BABBLE_BUFFER_SIZE, &tm->circular_buffer[index_first]);
        
        index_first = (index_first + 1) % BABBLE_TIMELINE_MSIZE;
    }
    else{
        index_first = (BABBLE_TIMELINE_MSIZE + tm->youngest - tm->count_recent_adds) % BABBLE_TIMELINE_MSIZE;
    }
    
    /* add all new msgs in the timeline */
    while(index_first != tm->youngest ){
        add_msg_to_answer(the_answer, BABBLE_BUFFER_SIZE, &tm->circular_buffer[index_first]);

        index_first = (index_first + 1) % BABBLE_TIMELINE_MSIZE;
    }

    tm->count_recent_adds = 0;
    
    pthread_mutex_unlock(&tm->mutex);
    *answer = the_answer;
}
