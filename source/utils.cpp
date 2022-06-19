#include "utils.h"
#include "message.pb.h"
#include "string"
#define MAXBUFFERSIZE 4098 /* Defining the maximum size of the buffer that will be used to read the socket. */
extern "C"
{

    /**
     * It creates a socket, binds it to a port, and then listens for incoming connections
     *
     * @param port The port number on which the server will listen for incoming connections.
     *
     * @return The socket file descriptor
     */
    int listening_socket(int port)
    {
        /* Creating a socket. */
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0)
        {
            perror("Socket failed\n");
            return -1;
        }

        /* sockaddr_in gives the internet address */
        struct sockaddr_in serv_addr; 

        /* Setting the socket address. */
        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(port);
        
        /* Setting the socket options. */
        int enable = 1;
        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
            perror("setsockopt(SO_REUSEADDR) failed");
        if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        {
            perror("Binding failed\n");
            return -1;
        }

        /* Listening for incoming connections. */
        if (listen(sockfd, 1) < 0)
        {
            return -1;
        }
        return sockfd;
    }

    /**
     * It takes a hostname and a port number, and returns a socket file descriptor that is connected to
     * the hostname on the given port
     *
     * @param hostname the name of the host you want to connect to
     * @param port the port number to connect to
     *
     * @return The socket file descriptor.
     */
    int connect_socket(const char *hostname, const int port)
    {
        struct sockaddr_in serv_addr;
        struct hostent *server = gethostbyname(hostname);
        if (server == NULL)
        {
            perror("No such host");
            return -1;
        }

       /* Creating a socket and then checking if it was created successfully. */
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0)
        {
            perror("ERROR opening socket in connect_socket\n");
            return -1;
        }
        /* Setting the socket address. */
        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);
        /* Connecting the socket to the server. */
        if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        {
            perror("ERROR connecting in connect_socket\n");
            return -1;
        }

        return sockfd;
    }

    /**
     * It accepts a connection on the socket sockfd and returns the new socket descriptor
     *
     * @param sockfd The socket file descriptor that was returned by the socket() function.
     *
     * @return The file descriptor of the new socket.
     */
    int accept_connection(int sockfd)
    {
        struct sockaddr_in cli_addr;
        socklen_t clilen = sizeof(cli_addr);
        int newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
        if (newsockfd < 0)
        {
            return -1;
        }
        return newsockfd;
    }

    /**
     * This function receives a message from the socket and parses it into the operation type and
     * argument
     *
     * @param sockfd The socket file descriptor.
     * @param operation_type The type of operation that the client wants to perform.
     * @param argument The argument to the operation.
     *
     * @return the operation type and argument.
     */
    int recv_msg(int sockfd, int32_t *operation_type, int64_t *argument)
    {
        /* Receiving the message from the socket */
        std::string receivedString;
        char buffer[MAXBUFFERSIZE];
        size_t bytes_read;
        int message_size=0;
        bzero(buffer, MAXBUFFERSIZE - 1);
        while((bytes_read= recv(sockfd, buffer+message_size, sizeof(buffer) - message_size- 1, 0)) > 0)
        {
            message_size+= bytes_read;
            if(message_size >= MAXBUFFERSIZE - 1)
            {
                break;
            }
        }
        
        if (bytes_read < 0)
        {
            perror("Error reading socket in recv_msg");
            return -1;
        }
        
        buffer[message_size-1] = 0;
        receivedString = buffer;

        /* Parsing the message from the buffer and then setting the operation type and argument. */
        sockets::message message;
        message.ParseFromString(buffer);
        *operation_type = message.type();
        *argument = message.argument();
        return 0;
    }

    /**
     * It takes in a socket file descriptor, an operation type, and an argument, and sends a message to
     * the server
     *
     * @param sockfd The socket file descriptor.
     * @param operation_type 1 for addition, 2 for subtraction, 3 for termination, 4 for counter
     * @param argument The argument to be sent to the server.
     *
     * @return The number of bytes sent.
     */
    int send_msg(int sockfd, int32_t operation_type, int64_t argument)
    {
        /* Setting the operation type and argument. */
        sockets::message message = sockets::message();
        if (operation_type == 1)
        {
            message.set_type(sockets::OperationType::ADD);
        }
        else if (operation_type == 2)
        {
            message.set_type(sockets::OperationType::SUB);
        }
        else if (operation_type == 3)
        {
            message.set_type(sockets::OperationType::TERMINATION);
        }
        else if (operation_type == 4)
        {
            message.set_type(sockets::OperationType::COUNTER);
        }
        message.set_argument(argument);

        /* Serializing the message into a string. */
        std::string msgString;
        message.SerializeToString(&msgString);

        /* Converting the string into a char array and then sending it. */
        char buffer[MAXBUFFERSIZE];
        size_t bytes_send;
        int message_size = 0;
        bzero(buffer, MAXBUFFERSIZE - 1);
        sprintf(buffer, "%s", msgString.c_str());

        while((bytes_send = send(sockfd, buffer + message_size, sizeof(buffer) - message_size - 1, MSG_NOSIGNAL)) > 0)
        {
            message_size += bytes_send;
            if(message_size >= MAXBUFFERSIZE - 1)
            {
                break;
            }
        }
        return 0;
    }
}