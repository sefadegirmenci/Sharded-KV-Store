#include <iostream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <cxxopts.hpp>
#include <fcntl.h>
#include <fstream>
#include <fmt/core.h>
#include "message.h"
#include "shared.h"
#include "workload_traces/generate_traces.h"
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include "rocksdb/db.h"
#include <list>

const char *hostname = "localhost";
std::list<int> cluster;

std::atomic<int64_t> number{0};

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
 * It prints an error message and exits the program
 *
 * @param msg This is the message that will be displayed when the error occurs.
 */
void error(const char *msg)
{
    perror(msg);
    exit(0);
}

int shard(int key)
{
    int num_servers = cluster.size();
    int shard_id = (key % num_servers) + 1;

    return shard_id;
}

int main(int argc, char *argv[])
{
    /* This is parsing the command line arguments. */
    cxxopts::Options options(argv[0], "Sever for the sockets benchmark");
    options.allow_unrecognised_options().add_options()(
        "p,port", "Port at which the master listens to.",
        cxxopts::value<size_t>())("h,help", "Print help");

    auto args = options.parse(argc, argv);
    if (args.count("help"))
    {
        fmt::print("{}\n", options.help());
        return 0;
    }

    if (!args.count("port"))
    {
        fmt::print(stderr, "The master port is required\n{}\n",
                   options.help());
        return 1;
    }

    /* This is converting the arguments into integers. */
    int port = args["port"].as<size_t>();

    /* This is creating a socket and checking if it is valid. */
    int sockfd = listening_socket(port);
    if (sockfd < 0)
    {
        error("Error creating socket");
    }

    /* This is accepting the connections */
    int newsockfd = accept_connection(sockfd);
    if (newsockfd < 0)
    {
        error("Error accepting connection.");
    }
    std::cout << "Accepted a connection" << std::endl;
    /* This is the main loop of the server. */
    while (newsockfd != -1)
    {
        /* This is reading the message. */
        auto [bytecount, buffer] = secure_recv(newsockfd);
        if (bytecount <= 0)
        {
            std::cout << "Error receiving message" << std::endl;
            break;
        }
        // std::cout<<"Received a message with size "<<bytecount << std::endl;
        if (buffer == nullptr || bytecount == 0)
        {
            return 1;
        }
        /* Parsing the message from the buffer */
        sockets::master_msg request;
        auto size = bytecount;
        std::string master_message(buffer.get(), size);
        request.ParseFromString(master_message);
        // std::cout<<"Message is "<<master_message.DebugString()<<std::endl;

        /* This is handling the message. */
        if (request.operation() == sockets::master_msg::SERVER_JOIN)
        {
            cluster.push_back(request.server_port());
        }
        else if (request.operation() == sockets::master_msg::CLIENT_LOCATE)
        {
            if (cluster.size() == 0)
            {
                std::cout << "No servers are running" << std::endl;
                return 1;
            }
            int shard_id = shard(request.key());

            auto cluster_front = cluster.begin();
            std::advance(cluster_front, shard_id - 1);
            int server_port = *cluster_front;
            sockets::master_msg response;
            response.set_operation(sockets::master_msg::RESPONSE_LOCATE);
            response.set_port(server_port);
            std::string response_message;
            response.SerializeToString(&response_message);
            auto response_size = response_message.size();
            auto response_buffer = std::make_unique<char[]>(response_size + length_size_field);
            construct_message(response_buffer.get(), response_message.c_str(), response_size);
            secure_send(newsockfd, response_buffer.get(), response_size + length_size_field);
        }
        /* Accepting a connection. */
        newsockfd = accept_connection(sockfd);
    }

    /* Closing the sockets. */
    close(newsockfd);
    close(sockfd);

    return 0;
}