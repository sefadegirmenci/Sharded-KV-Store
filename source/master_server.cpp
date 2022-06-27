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
#include <sys/select.h>
#include "shared.h"
#include "workload_traces/generate_traces.h"
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include "rocksdb/db.h"
#include <list>

const char *hostname = "localhost";
std::list<int> cluster; /* This stores the port number of the shards */
std::list<int> keys;    /* This stores the keys */

std::atomic<int64_t> number{0};


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

int find_shard(int key)
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

    fd_set current_sockets, ready_sockets;
    FD_ZERO(&current_sockets);
    FD_SET(sockfd, &current_sockets);

    struct timeval tv;
    tv.tv_sec = 4;
    tv.tv_usec = 0;

    while (true)
    {
        ready_sockets = current_sockets;
        if (select(FD_SETSIZE, &ready_sockets, NULL, NULL, NULL) < 0)
        {
            error("Error in select");
        }
        for (int i = 0; i < FD_SETSIZE; i++)
        {
            if (FD_ISSET(i, &ready_sockets))
            {
                if (i == sockfd)
                {
                    // this is a new connection we can accept
                    int newsockfd = accept_connection(sockfd);
                    if (newsockfd < 0)
                    {
                        error("Error accepting connection.");
                    }
                    FD_SET(newsockfd, &current_sockets);
                }
                else
                {
                    /* This is reading the message. */
                    auto [bytecount, buffer] = secure_recv(i);
                    if (bytecount <= 0)
                    {
                        break;
                    }
                    if (buffer == nullptr || bytecount == 0)
                    {
                        return 1;
                    }
                    /* Parsing the message from the buffer */
                    sockets::master_msg request;
                    auto size = bytecount;
                    std::string master_message(buffer.get(), size);
                    request.ParseFromString(master_message);
                    /* This is handling the message. */
                    if (request.operation() == sockets::master_msg::SERVER_JOIN)
                    {
                        /* There is a need for redistribution */
                        if (cluster.size() > 0)
                        {
                            for (auto key : keys)
                            {
                                int shard_id = find_shard(key);
                                auto cluster_front = cluster.begin();
                                std::advance(cluster_front, shard_id - 1);
                                int server_port = *cluster_front;
                                int serverfd = connect_socket(hostname, server_port);
                                if (serverfd < 0)
                                {
                                    error("Error connecting to server");
                                }
                                /* Get the value of key from server */
                                server::server_msg server_msg;
                                server_msg.set_operation(server::server_msg::GET);
                                server_msg.set_key(key);
                                /* Send the proto message */
                                std::string server_str;
                                server_msg.SerializeToString(&server_str);
                                auto msg_size = server_str.size();
                                auto buf = std::make_unique<char[]>(msg_size + length_size_field);
                                construct_message(buf.get(), server_str.c_str(), msg_size);
                                secure_send(serverfd, buf.get(), msg_size + length_size_field);
                                /* Receive the response from the server */
                                auto [bytecount, buffer] = secure_recv(serverfd);
                                if (bytecount <= 0)
                                {
                                    return 1;
                                }

                                if (buffer == nullptr || bytecount == 0)
                                {
                                    return 1;
                                }
                                /* Parsing the message from the buffer */
                                server::server_msg response;
                                auto size = bytecount;
                                std::string response_message(buffer.get(), size);
                                response.ParseFromString(response_message);
                                std::string value = response.value();
                                /* Send delete request to the server */
                                serverfd = connect_socket(hostname, server_port);
                                server_msg.set_operation(server::server_msg::DELETE);
                                server_msg.set_key(key);
                                /* Send the proto message */
                                server_str.clear();
                                server_msg.SerializeToString(&server_str);
                                msg_size = server_str.size();
                                buf = std::make_unique<char[]>(msg_size + length_size_field);
                                construct_message(buf.get(), server_str.c_str(), msg_size);
                                secure_send(serverfd, buf.get(), msg_size + length_size_field);

                                /* Send put request to the correct server */
                                cluster.push_back(request.server_port());
                                shard_id = find_shard(key);
                                cluster_front = cluster.begin();
                                std::advance(cluster_front, shard_id - 1);
                                server_port = *cluster_front;
                                serverfd = connect_socket(hostname, server_port);
                                cluster.pop_back();
                                server_msg.set_operation(server::server_msg::PUT);
                                server_msg.set_key(key);
                                server_msg.set_value(value);
                                /* Send the proto message */
                                server_str.clear();
                                server_msg.SerializeToString(&server_str);
                                msg_size = server_str.size();
                                buf = std::make_unique<char[]>(msg_size + length_size_field);
                                construct_message(buf.get(), server_str.c_str(), msg_size);
                                secure_send(serverfd, buf.get(), msg_size + length_size_field);
                            }
                        }
                        cluster.push_back(request.server_port());
                        
                    }
                    else if (request.operation() == sockets::master_msg::CLIENT_LOCATE)
                    {
                        if (cluster.size() == 0)
                        {
                            return 1;
                        }
                        int shard_id = find_shard(request.key());
                        keys.push_back(request.key());

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
                        secure_send(i, response_buffer.get(), response_size + length_size_field);
                    }
                    FD_CLR(i, &current_sockets);
                }
            }
        }
    }

    /* Closing the sockets. */
    close(sockfd);

    return 0;
}
