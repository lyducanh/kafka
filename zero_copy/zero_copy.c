#include <sys/sendfile.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stddef.h>
#include <unistd.h>

int main() {
    int server = socket(AF_INET, SOCK_STREAM, 0);
    
    // This prevents the "Address already in use" error if you restart the server
    int opt = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(8080) };
    bind(server, (struct sockaddr*)&addr, sizeof(addr));
    listen(server, 10);

    int file = open("data.txt", O_RDONLY);
    struct stat stat_buf;
    // fstat(file, &stat_buf);

    // LOOP: Keep the server running forever
    while (1) {
        int client = accept(server, NULL, NULL);
        
        // We must tell sendfile to start at byte 0 for every new client
        off_t offset = 0; 
        sendfile(client, file, &offset, stat_buf.st_size);
        
        // Close the connection so 'nc' knows the download is done
        close(client);
    }

    return 0;
}