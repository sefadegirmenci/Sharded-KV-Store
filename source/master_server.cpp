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
list<int> cluster;

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

int shard(int key){
    int num_servers = cluster.size();
    int shard_id = (key % num_servers)+1;

    return shard_id;
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
        "p,port", "Port at which the master listens to.",
            cxxopts::value<size_t>())
        ("h,help", "Print help");

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
    std::cout<< "Accepted a connection" << std::endl;
    /* This is the main loop of the server. */
    while (newsockfd != -1)
    {
        /* This is reading the message. */
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
        sockets::master_msg request;    
        auto size = bytecount;
        std::string master_message(buffer.get(), size);
        request.ParseFromString(master_message);
        //std::cout<<"Message is "<<master_message.DebugString()<<std::endl;

        /* This is handling the message. */
        if(request.operation()==sockets::master_msg::SERVER_JOIN){
            cluster.push_back(request.server_port);
        }
        else if(request.operation()==sockets::master_msg::CLIENT_LOCATE){
            if(cluster.size()==0){
                std::cout<<"No servers are running"<<std::endl;
                return 1;
            }
            int shard_id = shard(request.key());
            int server_port = cluster[shard_id-1];
            sockets::master_msg response;
            response.set_operation(sockets::master_msg::RESPONSE_LOCATE);
            response.set_port(server_port);
            std::string response_message;
            response.SerializeToString(&response_message);
            auto response_size = response_message.size();
            auto response_buffer = std::make_unique<char[]>(response_size);
            memcpy(response_buffer.get(), response_message.c_str(), response_size);
            secure_send(newsockfd, response_size, response_buffer.get());
        }
        /* Accepting a connection. */
        newsockfd = accept_connection(sockfd);
    }

    /* Closing the sockets. */
    close(newsockfd);
    close(sockfd);

    return 0;
}