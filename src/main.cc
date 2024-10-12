#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstddef>
#include <iostream>
#include <string>

struct InputBuffer {
    std::string buffer;
};

InputBuffer* new_input_buffer()
{
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    return input_buffer;
}

void print_prompt() {
    printf("db > ");
}

void read_input(InputBuffer* input_buffer)
{
    if (!std::getline(std::cin, input_buffer->buffer))
    {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
}

void close_input_buffer(InputBuffer* input_buffer)
{
    free(input_buffer);
}

int main(int argc, char** argv)
{
    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer == ".exit") {
            printf("byte...\n");
            close_input_buffer(input_buffer);
            exit(EXIT_SUCCESS);
        } else {
            printf("Unrecognized command '%s'.\n", input_buffer->buffer.c_str());
        }
    }
    return 0;
}
