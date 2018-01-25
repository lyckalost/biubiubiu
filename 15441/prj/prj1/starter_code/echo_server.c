/******************************************************************************
* echo_server.c                                                               *
*                                                                             *
* Description: This file contains the C source code for an echo server.  The  *
*              server runs on a hard-coded port and simply write back anything*
*              sent to it by connected clients.  It does not support          *
*              concurrent clients.                                            *
*                                                                             *
* Authors: Athula Balachandran <abalacha@cs.cmu.edu>,                         *
*          Wolf Richter <wolf@cs.cmu.edu>                                     *
*                                                                             *
*******************************************************************************/

#include <netinet/in.h>
#include <netinet/ip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/select.h>

#define ECHO_PORT 9999
#define BUF_SIZE 4096

int close_socket(int sock)
{
    if (close(sock))
    {
        fprintf(stderr, "Failed closing socket.\n");
        return 1;
    }
    return 0;
}

int main(int argc, char* argv[])
{
    int sock, client_sock, fdmax;
    ssize_t readret;
    socklen_t cli_size;
    struct sockaddr_in addr, cli_addr;
    char buf[BUF_SIZE];
    fd_set master, read_fds;
    struct timeval tv;
    
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    FD_ZERO(&master);
    FD_ZERO(&read_fds);
    fprintf(stdout, "----- Echo Server -----\n");
    
    /* all networked programs must create a socket */
    if ((sock = socket(PF_INET, SOCK_STREAM, 0)) == -1)
    {
        fprintf(stderr, "Failed creating socket.\n");
        return EXIT_FAILURE;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(ECHO_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    /* servers bind sockets to ports---notify the OS they accept connections */
    if (bind(sock, (struct sockaddr *) &addr, sizeof(addr)))
    {
        close_socket(sock);
        fprintf(stderr, "Failed binding socket.\n");
        return EXIT_FAILURE;
    }


    if (listen(sock, 5))
    {
        close_socket(sock);
        fprintf(stderr, "Error listening on socket.\n");
        return EXIT_FAILURE;
    }
    fdmax = sock;
    FD_SET(sock, &master);

    while (1) {
        read_fds = master;
        if (select(fdmax + 1, &read_fds, NULL, NULL, &tv) == -1) {
            perror("select");
            exit(4);
        }
        
        for (int i = 0; i <= fdmax; i++) {
            if (FD_ISSET(i, &read_fds) == 0)
                continue;
            // 拿到sock，那么接下来是accept client放入set
            if (i == sock) {
                cli_size = sizeof(cli_addr);
                client_sock = accept(sock, (struct sockaddr *) &cli_addr, &cli_size);
                if (client_sock == -1) {
                    perror("accept error!");
                } else {
                    FD_SET(client_sock, &master);
                    if (client_sock > fdmax)
                        fdmax = client_sock;
                }
            } 
            else {
                memset(buf, 0, BUF_SIZE);
                readret = recv(i, buf, BUF_SIZE, 0);
                if (readret == -1) {
                    close_socket(i);
                    close_socket(sock);
                    perror("read from client failed!");
                }
                if (readret == 0) {
                    FD_CLR(i, &master);
                    close_socket(i);
                } else {
                    if(send(i, buf, readret, 0) != readret) {
                        close_socket(i);
                        close_socket(sock);
                        perror("send to client failed!");
                    }
                }
            }
        }
    }

    close_socket(sock);

    return EXIT_SUCCESS;
}
