#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define INPUT_BUFFER_SIZE 1024

static void print_prompt(void) {
    printf("minidb> ");
}

static bool read_input(char *buffer, size_t buffer_size) {
    if (!fgets(buffer, buffer_size, stdin)) {
        puts("\n");
        return false;
    }

    // Remove trailing newline if present
    size_t len = strlen(buffer);
    if (len > 0 && buffer[len - 1] == '\n') {
        buffer[len - 1] = '\0';
    }
    return true;
}

/* --------- Meta commands -------------*/
static bool do_meta_command(const char *input) {
    if (strcmp(input, ".exit") == 0) {
        return false;
    }
    if (strcmp(input, ".help") == 0) {
        puts("Meta commands: ");
        puts("  .help   Show this help");
        puts("  .exit   Exit the program");
        return true;
    }

    printf("Unrecognized meta command: '%s'\n", input);
    return true;
}

/* ------ SQL Statements stub*/

static void prepare_statement(const char *input) {
    printf("SQL received (not executed yet): \"%s\"\n", input);
}


int main(void) {
    char input_buffer[INPUT_BUFFER_SIZE];
    puts("mindb (sqlite-like toy DB)");
    puts("Enter .help to show available commands.");

    while(1) {
        print_prompt();
        
        if (!read_input(input_buffer, sizeof(input_buffer))) {
            break;
        }

        if (strlen(input_buffer) == 0) {
            continue;
        }

        if (input_buffer[0] == '.') {
            if (!do_meta_command(input_buffer)) {
                break;
            }
            continue;
        }

        prepare_statement(input_buffer);
    }

    puts("Bye!");
    return 0;
}