#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>

#include "babble_server.h"
#include "babble_types.h"
#include "babble_utils.h"
#include "babble_communication.h"
#include "babble_server_answer.h"
#include "fastrand.h"
#include "command_buffer.h"

command_buffer_t command_buffer;

/* to activate random delays in the processing of messages */
int random_delay_activated;

static void display_help(char *exec)
{
    printf("Usage: %s -p port_number -r [activate_random_delays]\n", exec);
}


static int parse_command(char* str, command_t *cmd)
{
    char *name = NULL;
    
    /* start by cleaning the input */
    str_clean(str);
    
    /* get command id */
    cmd->cid=str_to_command(str, &cmd->answer_expected);
    
    switch(cmd->cid){
    case LOGIN:
        if(str_to_payload(str, cmd->msg, BABBLE_ID_MSIZE)){
            name = get_name_from_key(cmd->key);
            fprintf(stderr,"Error from [%s]-- invalid LOGIN -> %s\n", name, str);
            free(name);
            return -1;
        }
        break;
    case PUBLISH:
        if(str_to_payload(str, cmd->msg, BABBLE_PUBLICATION_MSIZE)){
            name = get_name_from_key(cmd->key);
            fprintf(stderr,"Warning from [%s]-- invalid PUBLISH -> %s\n", name, str);
            free(name);
            return -1;
        }
        break;
    case FOLLOW:
        if(str_to_payload(str, cmd->msg, BABBLE_ID_MSIZE)){
            name = get_name_from_key(cmd->key);
            fprintf(stderr,"Warning from [%s]-- invalid FOLLOW -> %s\n", name, str);
            free(name);
            return -1;
        }
        break;
    case TIMELINE:
        cmd->msg[0]='\0';
        break;
    case FOLLOW_COUNT:
        cmd->msg[0]='\0';
        break;
    case RDV:
        cmd->msg[0]='\0';
        break;    
    default:
        name = get_name_from_key(cmd->key);
        fprintf(stderr,"Error from [%s]-- invalid client command -> %s\n", name, str);
        free(name);
        return -1;
    }

    return 0;
}


/* processes the command and eventually generates an answer */
static int process_command(command_t *cmd, answer_t **answer)
{
    int res=0;

    switch(cmd->cid){
    case LOGIN:
        res = run_login_command(cmd, answer);
        break;
    case PUBLISH:
        random_delay(random_delay_activated);
        res = run_publish_command(cmd, answer);
        break;
    case FOLLOW:
        random_delay(random_delay_activated);
        res = run_follow_command(cmd, answer);
        break;
    case TIMELINE:
        random_delay(random_delay_activated);
        res = run_timeline_command(cmd, answer);
        break;
    case FOLLOW_COUNT:
        res = run_fcount_command(cmd, answer);
        break;
    case RDV:
        res = run_rdv_command(cmd, answer);
        break;
    case UNREGISTER:
        res = unregisted_client(cmd);
        *answer = NULL;
        break;
    default:
        fprintf(stderr,"Error -- Unknown command id\n");
        return -1;
    }

    if(res){
        fprintf(stderr,"Error -- Failed to run command ");
        display_command(cmd, stderr);
    }

    return res;
}

//.............................................................................................................................

static void *communication_thread(void *arg)
{
    if(arg == NULL){
        fprintf(stderr, "Error -- Communication thread argument is NULL.\n");
        return NULL;
    }
    int sockfd = *((int*)arg); //socket descriptor for the client connection
    free(arg); // free the memory allocated for the socket descriptor

    char *recv_buff=NULL;
    int recv_size = 0;
    command_t *cmd = NULL;
    answer_t *answer = NULL;
    unsigned long client_key =0;
    char client_name[BABBLE_ID_MSIZE+1] = {0};

    //Recv the login message from client
    recv_size = network_recv(sockfd, (void**)&recv_buff);
    if(recv_size <= 0){
        fprintf(stderr, "Error -- Could not receive LOGIN message from client.\n");
        free(recv_buff);
        close(sockfd);
        return NULL;
    }

    //Parse the login message to get the command
    cmd = new_command(0);
    if(parse_command(recv_buff, cmd) == -1 || cmd->cid != LOGIN){
        fprintf(stderr, "Error -- Invalid LOGIN message from client.\n");
        free(recv_buff); 
        free(cmd);
        close(sockfd);
        return NULL;
    }
    free(recv_buff);
    recv_buff = NULL;

    //Process the login command to register the client and generate an answer
    cmd->sockfd = sockfd;
    if(process_command(cmd, &answer) == -1){
        fprintf(stderr, "Error -- Could not process LOGIN command from client.\n");
        free_answer(answer);
        free(cmd);
        close(sockfd);
        return NULL;
    }

    //Send the answer to the client
    client_key = cmd->key;
    strncpy(client_name, cmd->msg, BABBLE_ID_MSIZE);
    free(cmd);
    cmd = NULL;

    if(answer != NULL && send_answer_to_client(answer) == -1){
        fprintf(stderr, "Error -- Could not send LOGIN answer to client %s.\n", client_name);
        free_answer(answer);

        cmd = new_command(client_key);
        cmd->cid = UNREGISTER;
        if(command_buffer_put(&command_buffer, cmd) != 0){
            fprintf(stderr, "Error -- Could not enqueue UNREGISTER command for client %s.\n", client_name);
            free(cmd);  
            close(sockfd);
        }
      
        return NULL;
    }

    free_answer(answer);
    answer = NULL;

    //while there are messages from the client, parse and process them
    while((recv_size = network_recv(sockfd, (void**)&recv_buff)) > 0){
        cmd = new_command(client_key);
        if(parse_command(recv_buff, cmd) == -1){
            fprintf(stderr, "Warning -- Could not parse message from client %s.\n",
                    client_name);

            if(notify_parse_error(cmd, recv_buff, &answer) == -1){
                fprintf(stderr, "Error -- Could not create parse-error answer.\n");
            }

            if(answer != NULL && send_answer_to_client(answer) == -1){
                fprintf(stderr, "Error -- Could not send parse-error answer.\n");
            }

            free_answer(answer);
            answer = NULL;
            free(cmd);
        }
        else{
            if(command_buffer_put(&command_buffer, cmd) != 0){
                fprintf(stderr, "Error -- Could not enqueue command from client %s.\n", client_name);
                free(cmd); 
                free(recv_buff);
                recv_buff = NULL;
                break;   
            }
        }
        free(recv_buff);
        recv_buff = NULL;
    }

    //unregister the client when the connection is closed or an error occurs
    cmd = new_command(client_key);
    cmd->cid = UNREGISTER;
    if(command_buffer_put(&command_buffer, cmd) != 0){
        fprintf(stderr, "Error -- Could not enqueue UNREGISTER command for client %s.\n", client_name);
        free(cmd);
        close(sockfd);
    }
    return NULL;
}
//....................................................................................................................


//.............................................................................................................................
static void *executor_thread(void *arg)
{
    //Initialize the thread random generator for test delays.
    fastRandomSetSeed(time(NULL) + pthread_self() * 100);

    while(1){
        command_t *command = NULL; //command_buffer_get() will store the command to be executed in this variable.
        answer_t *answer = NULL; //process_command() will store the answer to be sent to the client in this variable.

        //Take a command from the command_buffer
        if(command_buffer_get(&command_buffer, &command) !=0){
            fprintf(stderr, "Error -- Executor could not get command from buffer. \n");
            return NULL; 
        }

        //Process the command and generate an answer to be sent to the client
        if(process_command(command, &answer) !=0){
            fprintf(stderr, "Error -- Could not process command from client %lu. \n", command->key);
        }

        //Send the answer to the client
        if(answer != NULL && send_answer_to_client(answer)!=0){
            fprintf(stderr, "Error -- Could not send answer to client %lu. \n", answer->key);
        }

        free_answer(answer);
        free(command);
    }
    return NULL;
}
//.................................................................................................................

int main(int argc, char *argv[])
{
    int sockfd, newsockfd;
    int portno=BABBLE_PORT;
    
    int opt;
    int nb_args=1;

    //char* recv_buff=NULL;
    //int recv_size=0;
    
    //unsigned long client_key=0;
    //char client_name[BABBLE_ID_MSIZE+1];

    //command_t *cmd;
    //answer_t *answer=NULL;

    //................................
    pthread_t executor_thread_id;
    int thread_error;

    pthread_t communication_thread_id;
    int *client_sockfd;
    //................................
    
    while ((opt = getopt (argc, argv, "+hp:r")) != -1){
        switch (opt){
        case 'p':
            portno = atoi(optarg);
            nb_args+=2;
            break;
        case 'r':
            random_delay_activated=1;
            nb_args+=1;
            break;
        case 'h':
        case '?':
        default:
            display_help(argv[0]);
            return -1;
        }
    }
    
    if(nb_args != argc){
        display_help(argv[0]);
        return -1;
    }

    server_data_init();

    //...............................................................................
    //Start the executor thread 

    //Initialize the command buffer
    if(command_buffer_init(&command_buffer)!=0){
        fprintf(stderr, "Error -- Could not initialize command buffer. \n");
        return -1;
    }

    //Create the executor thread
    thread_error = pthread_create(&executor_thread_id, NULL, executor_thread, NULL);
    if(thread_error !=0)
    {
        fprintf(stderr, "Error -- Could not create executor thread. \n");
        return -1;
    }
    //.................................................................................
    
    if((sockfd = server_connection_init(portno)) == -1){
        return -1;
    }

    printf("Babble server bound to port %d\n", portno);    

    /* seed for the per-thread random number generator */
    //fastRandomSetSeed(time(NULL)+pthread_self()*100);

    /* main server loop */
    while(1){
        
        if((newsockfd= server_connection_accept(sockfd))==-1){
            return -1;
        }

        client_sockfd = malloc(sizeof(*client_sockfd));
        if(client_sockfd== NULL){
            fprintf(stderr, "Error -- Could not allocate memory for client socket descriptor. \n");
            close(newsockfd);
            continue;
        }
        *client_sockfd = newsockfd;

        thread_error = pthread_create(&communication_thread_id, NULL, communication_thread, client_sockfd);

        if(thread_error!=0){
            fprintf(stderr, "Error -- Could not create communication thread. \n");
            free(client_sockfd);
            close(newsockfd);
            continue;
        }

        //when the communication thread is done, it will clean up its resources and terminate itself.
        //main() will not wait for the communication thread to finish, it will continue to accept new client connections.
        pthread_detach(communication_thread_id);
    }
    close(sockfd);
    return 0;
}
    
    
