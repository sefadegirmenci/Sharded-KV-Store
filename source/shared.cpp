#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "shared.h"

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

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

inline auto destruct_message(char *msg, size_t bytes)
    -> std::optional<uint32_t> {
  if (bytes < 4) {
    return std::nullopt;
  }

  auto actual_msg_size = convert_byte_array_to_int(msg);
  return actual_msg_size;
}

static auto read_n(int fd, char *buffer, size_t n) -> size_t {
  size_t bytes_read = 0;
  size_t retries = 0;
  constexpr size_t max_retries = 10000;
  while (bytes_read < n) {
    auto bytes_left = n - bytes_read;
    auto bytes_read_now = recv(fd, buffer + bytes_read, bytes_left, 0);
    // negative return_val means that there are no more data (fine for non
    // blocking socket)
    if (bytes_read_now == 0) {
      if (retries >= max_retries) {
        return bytes_read;
      }
      ++retries;
      continue;
    }
    if (bytes_read_now > 0) {
      bytes_read += bytes_read_now;
      retries = 0;
    }
  }
  return bytes_read;
}

/**
 * It reads the length of the message, then reads the message itself
 * 
 * @param fd The file descriptor to read from
 * 
 * @return A pair of size_t and unique_ptr<char[]>
 */
auto secure_recv(int fd) -> std::pair<size_t, std::unique_ptr<char[]>> {
  char dlen[4];

  if (auto byte_read = read_n(fd, dlen, length_size_field);
      byte_read != length_size_field) {
    debug_print("[{}] Length of size field does not match got {} expected {}\n",
                __func__, byte_read, length_size_field);
    return {0, nullptr};
  }

  auto actual_msg_size_opt = destruct_message(dlen, length_size_field);
  if (!actual_msg_size_opt) {
    debug_print("[{}] Could not get a size from message\n", __func__);
    return {0, nullptr};
  }

  auto actual_msg_size = *actual_msg_size_opt;
  auto buf = std::make_unique<char[]>(static_cast<size_t>(actual_msg_size) + 1);
  buf[actual_msg_size] = '\0';
  if (auto byte_read = read_n(fd, buf.get(), actual_msg_size);
      byte_read != actual_msg_size) {
    debug_print("[{}] Length of message is incorrect got {} expected {}\n",
                __func__, byte_read, actual_msg_size);
    return {0, nullptr};
  }

  if (actual_msg_size == 0) {
    debug_print("[{}] wrong .. {} bytes\n", __func__, actual_msg_size);
  }
  return {actual_msg_size, std::move(buf)};
}

/**
 * It sends all the data
 * through the socket
 * 
 * @param fd the file descriptor of the socket
 * @param data the data to be sent
 * @param len the length of the data to be sent
 * 
 * @return The number of bytes sent.
 */
auto secure_send(int fd, char *data, size_t len) -> std::optional<size_t> {
  auto bytes = 0LL;
  auto remaining_bytes = len;

  char *tmp = data;

  while (remaining_bytes > 0) {
    bytes = send(fd, tmp, remaining_bytes, 0);
    if (bytes < 0) {
      // @dimitra: the socket is in non-blocking mode; select() should be also
      // applied
      //             return -1;
      //
      return std::nullopt;
    }
    remaining_bytes -= bytes;
    tmp += bytes;
  }

  return len;
}
