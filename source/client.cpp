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

const char *hostname = "localhost";

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

void error(const char *msg)
{
    perror(msg);
    exit(0);
}

int main(int argc, char *argv[])
{
    /* This is parsing the command line arguments. */
    cxxopts::Options options(argv[0], "Sever for the sockets benchmark");
    options.allow_unrecognised_options().add_options()(
        "p,port", "Port at which the target server listens to. This parameter should only be valid when DIRECT is set to `1`.",
        cxxopts::value<size_t>())("o,operation", "Either a GET or PUT request",
                                  cxxopts::value<std::string>())(
        "k,key", "Key for the operation",
        cxxopts::value<size_t>())(
        "v,value", "Value for the operation corresponding to the key. Only valid if the OPERATION is PUT.",
        cxxopts::value<std::string>())(
        "m,masterport", "Port at which the master listens to for the client.",
        cxxopts::value<std::size_t>())(
        "d,direct", "Specifies whether the client can talk to the server at port PORT.",
        cxxopts::value<std::size_t>())("h,help", "Print help");

    auto args = options.parse(argc, argv);
    if (args.count("help"))
    {
        fmt::print("{}\n", options.help());
        return 0;
    }

    if (!args.count("port"))
    {
        fmt::print(stderr, "The server port is required\n{}\n",
                   options.help());
        return 1;
    }

    if (!args.count("operation"))
    {
        fmt::print(stderr, "The operation type is required\n{}\n", options.help());
        return 1;
    }

    if (!args.count("key"))
    {
        fmt::print(stderr, "The key is required\n{}\n", options.help());
        return 1;
    }

    if (!args.count("value"))
    {
        fmt::print(stderr, "The value of key is required\n{}\n",
                   options.help());
        return 1;
    }

    if (!args.count("masterport"))
    {
        fmt::print(stderr, "The value of master port is required\n{}\n",
                   options.help());
        return 1;
    }

    if (!args.count("direct"))
    {

        fmt::print(stderr, "The direct variable should be specified\n{}\n",
                   options.help());
        return 1;
    }

    /* This is converting the arguments into integers. */
    int port = args["port"].as<size_t>();
    std::string operation = args["operation"].as<std::string>();
    int key = args["key"].as<size_t>();
    std::string value = args["value"].as<std::string>();
    int master_port = args["masterport"].as<size_t>();
    int direct = args["direct"].as<size_t>();

    /* Client cannot talk to the server directly */
    if (direct == 0)
    {
        /* Client socket */
        int sockfd;
        /* Connecting the socket to the master. */
        sockfd = connect_socket(hostname, master_port);
        if (sockfd < 0)
        {
            error("Error connecting master socket");
        }
        /* Create proto message */
        sockets::master_msg msg;
        msg.set_operation(sockets::master_msg::CLIENT_LOCATE);
        msg.set_key(key);

        /* Send the proto message */
        std::string str;
        msg.SerializeToString(&str);
        auto msg_size = str.size();
        auto buf = std::make_unique<char[]>(msg_size + length_size_field);
        construct_message(buf.get(), str.c_str(), msg_size);
        secure_send(sockfd, buf.get(), msg_size + length_size_field);

        /* Receive the port of the responsible server as a proto message */
        auto [bytecount, buffer] = secure_recv(sockfd);
        if (bytecount <= 0)
        {
            std::cout << "Error receiving message" << std::endl;
            return 1;
        }
        // std::cout<<"Received a message with size "<<bytecount << std::endl;
        if (buffer == nullptr || bytecount == 0)
        {
            return 1;
        }
        /* Parsing the message from the buffer */
        sockets::master_msg response;
        auto size = bytecount;
        std::string master_message(buffer.get(), size);
        response.ParseFromString(master_message);
        // std::cout<<"Message is "<<master_message.DebugString()<<std::endl;
        /* Get the port from proto message */
        port = response.port();
        // std::cout<<"Server port is "<<server_port<<std::endl;
        close(sockfd);
    }

    /* Sending the operation PUT/GET to the server*/
    int serverfd = connect_socket(hostname, port);
    if (serverfd < 0)
    {
        error("Error connecting socket");
    }

    /* Create proto message */
    server::server_msg server_msg;
    if (operation == "GET")
    {
        server_msg.set_operation(server::server_msg::GET);
        server_msg.set_key(key);
    }
    else if (operation == "PUT")
    {
        server_msg.set_operation(server::server_msg::PUT);
        server_msg.set_key(key);
        server_msg.set_value(value);
    }
    else
    {
        std::cout << "Invalid operation" << std::endl;
        return 1;
    }

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
        std::cout << "Error receiving message" << std::endl;
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

    if(response.key_exists() == false) {
        std::cout << "Key does not exist" << std::endl;
        return 2;
    }

    if(response.success() == false)
    {
        std::cout << "Error in the server" << std::endl;
        return 1;
    }

    /* Closing the socket. */
    close(serverfd);
    return 0;
}