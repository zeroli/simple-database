#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstddef>
#include <iostream>
#include <string>

struct InputBuffer {
  std::string buffer;
};

enum MetaCommandResult {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND,
};
enum PrepareResult {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED_STATEMENT,
};

enum StatementType {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
};

struct Statement {
  StatementType type;
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

MetaCommandResult do_meta_command(InputBuffer* input_buffer)
{
  if (input_buffer->buffer == ".exit") {
    printf("byte...\n");
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement)
{
  if (input_buffer->buffer == "insert") {
    statement->type = STATEMENT_INSERT;
    return PREPARE_SUCCESS;
  }
  if (input_buffer->buffer == "select") {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement)
{
  switch (statement->type) {
    case STATEMENT_INSERT:
    printf("This is where we would do an insert.\n");
    break;
    case STATEMENT_SELECT:
    printf("This is where we would do a select.\n");
    break;
  }
}

int main(int argc, char** argv)
{
  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);
    if (input_buffer->buffer.empty()) {
      continue;
    }

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer)) {
      case META_COMMAND_SUCCESS:
        continue;
      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command '%s'\n", input_buffer->buffer.c_str());
        continue;
      default:
        break;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case PREPARE_SUCCESS:
      break;
      case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword at start of '%s'\n", input_buffer->buffer.c_str());
      continue; // go to wait for input
    }

    execute_statement(&statement);
    printf("Executed.\n");
  }
  return 0;
}
