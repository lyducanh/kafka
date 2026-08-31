#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <stddef.h>

int main() {
    int server = socket(AF_INET, SOCK_STREAM, 0);
    
    // Prevent "Address already in use" error if you restart the server
    int opt = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(8080) };
    bind(server, (struct sockaddr*)&addr, sizeof(addr));
    listen(server, 10);

    int file = open("data.txt", O_RDONLY);

    // LOOP: Keep the server running forever
    while (1) {
        int client = accept(server, NULL, NULL);

        // Rewind the file back to the beginning for each new client
        // lseek(file, 0, SEEK_SET);

        char buffer[4096];
        int bytes_read;
        
        // Loop until the whole file is read and written
        while ((bytes_read = read(file, buffer, sizeof(buffer))) > 0) {
            write(client, buffer, bytes_read);
        }

        // Close the connection so 'nc' knows the download is done
        close(client);
    }

    return 0;
}