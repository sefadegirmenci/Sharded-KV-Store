#include <iostream>
#include <string>
#include "utils.h"
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

const char* hostname = "localhost";

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

int main(int args, char *argv[])
{
    /* Creating a local KV store. */
    rocksdb::DB *db;
    rocksdb::Options opts;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    opts.IncreaseParallelism();
    opts.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    opts.create_if_missing = true;
    opts.compression_per_level.resize(opts.num_levels);
    #if 1
        for(int i=0;i<opts.num_levels;i++)
        {
            opts.compression_per_level[i] = rocksdb::kNoCompression;
        }
    #endif
    opts.compression = kNoCompression;

    rocksdb::Status status = rocksdb::DB::Open(opts, "./testdb", &db);
    
    assert(status.ok());

    /* This is parsing the command line arguments. */
    cxxopts::Options options(argv[0], "Sever for the sockets benchmark");
    options.allow_unrecognised_options().add_options()(
        "p,port", "Port at which the server listens to.",
            cxxopts::value<size_t>())
        ("m,masterport", "Port of the master server",
            cxxopts::value<std::size_t>())
        ("h,help", "Print help");

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

    if (!args.count("masterport"))
    {
        fmt::print(stderr, "The value of master port is required\n{}\n",
                   options.help());
        return 1;
    }

    /* This is converting the arguments into integers. */
    int port = args["port"].as<size_t>();
    int master_port = args["masterport"].as<size_t>();

    std::cout<< "Connecting master server at port " << master_port << std::endl;
    int masterfd = connect_socket(hostname,master_port);
    std::cout<< "Connected master server at port " << master_port << std::endl;

    /* Create join message to the master */
    sockets::master_msg master_msg;
    master_msg.set_operation(sockets::master_msg::SERVER_JOIN);
    master_msg.set_server_port(port);
    /* Send the join message */
    std::string join_str;
    master_msg->SerializeToString(&join_str);
    auto msg_size = join_str.size();
    auto buf = std::make_unique<char[]>(msg_size + length_size_field);
    construct_message(buf.get(), join_str.c_str(), msg_size);
    secure_send(masterfd, buf.get(), msg_size + length_size_field);
    std::cout<< "Sent join message to master server" << std::endl;

    std::cout<< "Waiting for a client to connect" << std::endl;
    /* This is creating a socket and checking if it is valid. */
    int sockfd = listening_socket(port);
    if (sockfd < 0)
    {
        error("Error creating socket");
    }

    /* This is accepting the connection from the client. */
    int newsockfd = accept_connection(sockfd);
    if (newsockfd < 0)
    {
        error("Error accepting connection.");
    }
    std::cout<< "Accepted connection from a client" << std::endl;
    /* This is the main loop of the server. */
    while (newsockfd != -1)
    {
        /* Receive the port of the responsible server as a proto message */
        auto [bytecount, buffer] = secure_recv(newsockfd);
        if(bytecount <=0){
            std::cout<<"Error receiving message"<<std::endl;
            break;
        }
        //std::cout<<"Received a message with size "<<bytecount << std::endl;
        if (buffer == nullptr || bytecount == 0) {
            return 1;
        }
        /* Parsing the message from the buffer */
        server::server_msg request;    
        auto size = bytecount;
        std::string server_message(buffer.get(), size);
        request.ParseFromString(server_message);
        //std::cout<<"Message is "<<server_message.DebugString()<<std::endl;
        /* Get the port from proto message */

        server::server_msg response;
        response.set_operation(request.operation());
        response.set_key(request.key());
        

        if(request.operation() == server::server_msg::GET){
            std::cout << "Received GET request for key " << request.key() << std::endl;
            std::string value;
            rocksdb::Status status = db->Get(rocksdb::ReadOptions(), std::to_string(request.key()), &value);
            assert(status.ok());
            std::cout << "Value for key " << request.key() << " is " << value << std::endl;
            response.set_value(value);
        }else if(request.operation() == server::server_msg::PUT){
            std::cout << "Received PUT request for key " << request.key() << std::endl;
            rocksdb::Status status = db->Put(rocksdb::WriteOptions(), std::to_string(request.key()), request.value());
            assert(status.ok());
            response.set_value("");
        }
        response.set_success(true);

        /* Sending the response to the client. */
        std::string response_str;
        response.SerializeToString(&response_str);
        auto msg_size = response_str.size();
        auto buf = std::make_unique<char[]>(msg_size + length_size_field);
        construct_message(buf.get(), response_str.c_str(), msg_size);
        secure_send(newsockfd, buf.get(), msg_size + length_size_field);       
        std::cout<< "Sent response to client" << std::endl;
        /* Accepting the connection from the client. */
        newsockfd = accept_connection(sockfd);
    }

    /* Closing the sockets. */
    close(newsockfd);
    close(sockfd);

    return 0;
}