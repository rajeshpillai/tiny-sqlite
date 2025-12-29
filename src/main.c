#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INPUT_BUFFER_SIZE 1024

static void print_prompt(void) {
    printf("minidb> ");
}

static void read_input(char *buffer, size_t buffer_size) {
    if (!fgets(buffer, buffer_size, stdin)) {
        puts("\n");
        exit(0);
    }

    // Remove trailing newline if present
    size_t len = strlen(buffer);
    if (len > 0 && buffer[len - 1] == '\n') {
        buffer[len - 1] = '\0';
    }
}

int main(void) {
    char input_buffer[INPUT_BUFFER_SIZE];
    puts("mindb (sqlite-like toy DB)");
    puts("Enter .exit to quit.");

    while(1) {
        print_prompt();
        read_input(input_buffer, sizeof(input_buffer));

        if (strlen(input_buffer) == 0) {
            continue;
        }

        if (strcmp(input_buffer, ".exit") == 0) {
            puts("Bye!");
            break;
        }

        printf("Unrecognized command: '%s'\n", input_buffer);
    }
    return 0;
}