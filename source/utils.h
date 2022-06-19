#pragma once

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

// Client -> Server
const int32_t OPERATION_ADD = 1;
const int32_t OPERATION_SUB = 2;
const int32_t OPERATION_TERMINATION = 3;

// Server -> Client
const int32_t OPERATION_COUNTER = 4;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Open a SOCK_STREAM socket listening on the specified port.
 * @return sockfd if successful, -1 on failure
 */
int listening_socket(int port);

int connect_socket(const char *hostname, const int port);
/**
 * @brief Accept a connection on the specified sockfd.
 * @return sockfd of new connection when successful, -1 on failure
 */
int accept_connection(int sockfd);
/**
 * It takes a hostname and a port number, and returns a socket file descriptor that is connected to
 * the hostname on the given port
 * 
 * @param hostname the name of the host you want to connect to
 * @param port the port number to connect to
 * 
 * @return The socket file descriptor.
 */

/**
 * @brief Read one message from sockfd and fill the parameters. 
 *        Make sure that the whole message is read.
 * @return 0 on success, -1 on failure
 */
int recv_msg(int sockfd, int32_t *operation_type, int64_t *argument);

/**
 * @brief Send one message to sockfd given the parameters. 
 *        Make sure that the whole message is sent.
 * @return 0 on success, -1 on failure
 */
int send_msg(int sockfd, int32_t operation_type, int64_t argument);

#ifdef __cplusplus
}
#endif