#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "babble_registration.h"

client_bundle_t *registration_table[MAX_CLIENT];
int nb_registered_clients;

pthread_rwlock_t reg_lock = PTHREAD_RWLOCK_INITIALIZER;

void registration_init(void)
{
    nb_registered_clients=0;

    //Initialize the registration table to NULL
    memset(registration_table, 0, MAX_CLIENT * sizeof(client_bundle_t*));
}

//Reading the registration table
client_bundle_t* registration_lookup(unsigned long key)
{
    pthread_rwlock_rdlock(&reg_lock);
    
    int i=0;
    client_bundle_t *c = NULL;

    for(i=0; i< nb_registered_clients; i++){
        if(registration_table[i]->key == key){
            c = registration_table[i];
            break; // found
        }
    }

    pthread_rwlock_unlock(&reg_lock);
        
    return c;
}

//Writing to the registration table
int registration_insert(client_bundle_t* cl)
{    
    if(nb_registered_clients == MAX_CLIENT){
        fprintf(stderr, "ERROR: MAX NUMBER OF CLIENTS REACHED\n");
        return -1;
    }
    
    pthread_rwlock_wrlock(&reg_lock);

    /* lookup to find if key already exists */
    int i=0;
    for(i=0; i< nb_registered_clients; i++){
        if(registration_table[i]->key == cl->key){
            break; // found
        }
    }
    
    if(i != nb_registered_clients){
        fprintf(stderr, "Error -- id % ld already in use\n", cl->key);
        pthread_rwlock_unlock(&reg_lock);
        return -1;
    }

    /* insert cl */
    registration_table[nb_registered_clients]=cl;
    nb_registered_clients++;

    pthread_rwlock_unlock(&reg_lock);

    return 0;
}

//Removing a client from the registration table
client_bundle_t* registration_remove(unsigned long key)
{
    pthread_rwlock_wrlock(&reg_lock);

    int i=0;
    for(i=0; i<nb_registered_clients; i++){
        if(registration_table[i]->key == key){
            break; // found
        }
    }

    if(i == nb_registered_clients){
        fprintf(stderr, "Error -- no client found\n");
        pthread_rwlock_unlock(&reg_lock);
        return NULL;
    }
    
    client_bundle_t* cl= registration_table[i];

    nb_registered_clients--;
    registration_table[i] = registration_table[nb_registered_clients];
    
    pthread_rwlock_unlock(&reg_lock);
    return cl;
}
